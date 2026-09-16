//go:build linux || darwin

package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

func runCommand(ctx context.Context, name string, args []string, dir string, env []string, logPath string) (int, error) {
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return -1, err
	}
	defer log.Close()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	for _, entry := range os.Environ() {
		// Registry credentials belong to the HTTP client, not scanned code or package managers.
		name := strings.SplitN(entry, "=", 2)[0]
		if name == "ORT_REGISTER_API_KEY" || name == "AUTH_TOKEN_URL" || name == "AUTH_CLIENT_ID" ||
			name == "AUTH_CLIENT_SECRET" || name == "AUTH_SCOPES" {
			continue
		}
		cmd.Env = append(cmd.Env, entry)
	}
	cmd.Env = append(cmd.Env, env...)
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = 5 * time.Second
	err = cmd.Run()
	if ctx.Err() != nil {
		return -1, fmt.Errorf("%s: %w", name, ctx.Err())
	}
	if err == nil {
		return 0, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), err
	}
	return -1, err
}

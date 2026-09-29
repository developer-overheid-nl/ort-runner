//go:build linux || darwin

package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/developer-overheid-nl/ort-runner/internal/register"
)

// Register credentials belong to the HTTP clients, never to scanned code or package managers.
func protectedEnvironment(name string) bool {
	return name == register.APIKeyVariable || slices.Contains(register.ResultCredentialVariables, name)
}

func runCommand(ctx context.Context, name string, args []string, dir string, env []string, logPath string) (int, error) {
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return -1, err
	}
	defer log.Close()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	for _, entry := range os.Environ() {
		if protectedEnvironment(strings.SplitN(entry, "=", 2)[0]) {
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

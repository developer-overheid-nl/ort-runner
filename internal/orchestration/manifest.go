package orchestration

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sort"

	"github.com/developer-overheid-nl/ort-runner/internal/register"
)

const manifestSchemaVersion = 1

type Manifest struct {
	SchemaVersion int                   `json:"schemaVersion"`
	BatchID       string                `json:"batchId"`
	Repositories  []register.Repository `json:"repositories"`
}

type Chunk struct {
	Index  int
	Count  int
	Digest string
	Data   []byte
}

type chunkPayload struct {
	SchemaVersion int                   `json:"schemaVersion"`
	BatchID       string                `json:"batchId"`
	Index         int                   `json:"index"`
	Count         int                   `json:"count"`
	Repositories  []register.Repository `json:"repositories"`
}

func EncodeManifest(manifest Manifest, maxChunkBytes int) ([]Chunk, error) {
	if maxChunkBytes <= 0 {
		return nil, fmt.Errorf("maximum chunk size must be positive")
	}
	if manifest.SchemaVersion != manifestSchemaVersion {
		return nil, fmt.Errorf("unsupported manifest schema version %d", manifest.SchemaVersion)
	}
	if manifest.BatchID == "" {
		return nil, fmt.Errorf("batch ID is required")
	}

	repositories := append([]register.Repository(nil), manifest.Repositories...)
	sort.Slice(repositories, func(i, j int) bool { return repositories[i].ID < repositories[j].ID })
	seen := make(map[string]struct{}, len(repositories))
	for _, repository := range repositories {
		if repository.ID == "" {
			return nil, fmt.Errorf("repository ID is required")
		}
		if _, ok := seen[repository.ID]; ok {
			return nil, fmt.Errorf("duplicate repository ID %s", repository.ID)
		}
		seen[repository.ID] = struct{}{}
	}

	groups := make([][]register.Repository, 0, 1)
	if len(repositories) == 0 {
		groups = append(groups, []register.Repository{})
	}
	for _, repository := range repositories {
		if len(groups) == 0 {
			groups = append(groups, nil)
		}
		candidate := append(append([]register.Repository(nil), groups[len(groups)-1]...), repository)
		payload := chunkPayload{SchemaVersion: manifest.SchemaVersion, BatchID: manifest.BatchID, Index: len(groups) - 1, Count: len(repositories), Repositories: candidate}
		_, compressed, _, err := encodeChunkPayload(payload)
		if err != nil {
			return nil, err
		}
		if len(compressed) <= maxChunkBytes {
			groups[len(groups)-1] = candidate
			continue
		}
		if len(groups[len(groups)-1]) == 0 {
			return nil, fmt.Errorf("repository %s cannot fit in a %d-byte chunk", repository.ID, maxChunkBytes)
		}
		groups = append(groups, []register.Repository{repository})
		payload.Index = len(groups) - 1
		payload.Repositories = groups[len(groups)-1]
		_, compressed, _, err = encodeChunkPayload(payload)
		if err != nil {
			return nil, err
		}
		if len(compressed) > maxChunkBytes {
			return nil, fmt.Errorf("repository %s cannot fit in a %d-byte chunk", repository.ID, maxChunkBytes)
		}
	}

	chunks := make([]Chunk, 0, len(groups))
	for index, group := range groups {
		payload := chunkPayload{SchemaVersion: manifest.SchemaVersion, BatchID: manifest.BatchID, Index: index, Count: len(groups), Repositories: group}
		plain, compressed, digest, err := encodeChunkPayload(payload)
		if err != nil {
			return nil, err
		}
		_ = plain
		if len(compressed) > maxChunkBytes {
			return nil, fmt.Errorf("manifest chunk %d exceeds %d bytes", index, maxChunkBytes)
		}
		chunks = append(chunks, Chunk{Index: index, Count: len(groups), Digest: digest, Data: compressed})
	}
	return chunks, nil
}

func DecodeManifest(chunks []Chunk) (Manifest, error) {
	if len(chunks) == 0 {
		return Manifest{}, fmt.Errorf("manifest chunks are required")
	}
	ordered := append([]Chunk(nil), chunks...)
	slices.SortFunc(ordered, func(a, b Chunk) int { return a.Index - b.Index })
	count := ordered[0].Count
	if count <= 0 || len(ordered) != count {
		return Manifest{}, fmt.Errorf("manifest requires %d chunks, got %d", count, len(ordered))
	}

	result := Manifest{Repositories: []register.Repository{}}
	seen := make(map[string]struct{})
	for expected, chunk := range ordered {
		if chunk.Index != expected {
			return Manifest{}, fmt.Errorf("manifest chunk index %d missing or duplicated", expected)
		}
		if chunk.Count != count {
			return Manifest{}, fmt.Errorf("manifest chunk %d has inconsistent count", chunk.Index)
		}
		payload, err := decodeChunkPayload(chunk)
		if err != nil {
			return Manifest{}, fmt.Errorf("manifest chunk %d: %w", chunk.Index, err)
		}
		if payload.Index != chunk.Index || payload.Count != chunk.Count {
			return Manifest{}, fmt.Errorf("manifest chunk %d metadata mismatch", chunk.Index)
		}
		if payload.SchemaVersion != manifestSchemaVersion {
			return Manifest{}, fmt.Errorf("unsupported manifest schema version %d", payload.SchemaVersion)
		}
		if expected == 0 {
			if payload.BatchID == "" {
				return Manifest{}, fmt.Errorf("batch ID is required")
			}
			result.SchemaVersion = payload.SchemaVersion
			result.BatchID = payload.BatchID
		} else if payload.SchemaVersion != result.SchemaVersion || payload.BatchID != result.BatchID {
			return Manifest{}, fmt.Errorf("manifest chunk %d belongs to a different manifest", chunk.Index)
		}
		for _, repository := range payload.Repositories {
			if repository.ID == "" {
				return Manifest{}, fmt.Errorf("repository ID is required")
			}
			if _, ok := seen[repository.ID]; ok {
				return Manifest{}, fmt.Errorf("duplicate repository ID %s", repository.ID)
			}
			seen[repository.ID] = struct{}{}
			result.Repositories = append(result.Repositories, repository)
		}
	}
	return result, nil
}

func (manifest Manifest) Repository(index int) (register.Repository, error) {
	if index < 0 || index >= len(manifest.Repositories) {
		return register.Repository{}, fmt.Errorf("repository index %d outside range 0..%d", index, len(manifest.Repositories)-1)
	}
	return manifest.Repositories[index], nil
}

func encodeChunkPayload(payload chunkPayload) ([]byte, []byte, string, error) {
	plain, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, "", err
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(plain); err != nil {
		return nil, nil, "", err
	}
	if err := writer.Close(); err != nil {
		return nil, nil, "", err
	}
	sum := sha256.Sum256(plain)
	return plain, compressed.Bytes(), hex.EncodeToString(sum[:]), nil
}

func decodeChunkPayload(chunk Chunk) (chunkPayload, error) {
	reader, err := gzip.NewReader(bytes.NewReader(chunk.Data))
	if err != nil {
		return chunkPayload{}, err
	}
	plain, readErr := io.ReadAll(io.LimitReader(reader, 32<<20+1))
	closeErr := reader.Close()
	if readErr != nil {
		return chunkPayload{}, readErr
	}
	if closeErr != nil {
		return chunkPayload{}, closeErr
	}
	if len(plain) > 32<<20 {
		return chunkPayload{}, fmt.Errorf("decompressed chunk exceeds 32 MiB")
	}
	sum := sha256.Sum256(plain)
	if hex.EncodeToString(sum[:]) != chunk.Digest {
		return chunkPayload{}, fmt.Errorf("digest mismatch")
	}
	var payload chunkPayload
	if err := json.Unmarshal(plain, &payload); err != nil {
		return chunkPayload{}, err
	}
	return payload, nil
}

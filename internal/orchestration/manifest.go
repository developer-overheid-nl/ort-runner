package orchestration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"

	"github.com/developer-overheid-nl/ort-runner/internal/register"
)

const manifestSchemaVersion = 1

// Manifest is the immutable repository list of one batch. The position of a
// repository is its Indexed Job completion index.
type Manifest struct {
	SchemaVersion int                   `json:"schemaVersion"`
	BatchID       string                `json:"batchId"`
	Repositories  []register.Repository `json:"repositories"`
}

// Chunk is one ConfigMap-sized part of a manifest; Digest is the SHA-256 of Data.
type Chunk struct {
	Index  int
	Count  int
	Digest string
	Data   []byte
}

type chunkPayload struct {
	SchemaVersion int               `json:"schemaVersion"`
	BatchID       string            `json:"batchId"`
	Index         int               `json:"index"`
	Count         int               `json:"count"`
	Repositories  []json.RawMessage `json:"repositories"`
}

// NewManifest sorts repositories by ID so that indexes are deterministic.
func NewManifest(batchID string, repositories []register.Repository) (Manifest, error) {
	sorted := slices.Clone(repositories)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	manifest := Manifest{SchemaVersion: manifestSchemaVersion, BatchID: batchID, Repositories: sorted}
	return manifest, manifest.validate()
}

func (manifest Manifest) validate() error {
	if manifest.SchemaVersion != manifestSchemaVersion {
		return fmt.Errorf("unsupported manifest schema version %d", manifest.SchemaVersion)
	}
	if manifest.BatchID == "" {
		return fmt.Errorf("batch ID is required")
	}
	seen := make(map[string]struct{}, len(manifest.Repositories))
	for _, repository := range manifest.Repositories {
		if repository.ID == "" {
			return fmt.Errorf("repository ID is required")
		}
		if _, ok := seen[repository.ID]; ok {
			return fmt.Errorf("duplicate repository ID %s", repository.ID)
		}
		seen[repository.ID] = struct{}{}
	}
	return nil
}

// EncodeManifest splits the manifest into JSON chunks of at most maxChunkBytes,
// keeping repository order.
func EncodeManifest(manifest Manifest, maxChunkBytes int) ([]Chunk, error) {
	if maxChunkBytes <= 0 {
		return nil, fmt.Errorf("maximum chunk size must be positive")
	}
	if err := manifest.validate(); err != nil {
		return nil, err
	}
	// The envelope is measured with the largest possible index values, so every
	// finished chunk is guaranteed to fit.
	upperBound := len(manifest.Repositories) + 1
	envelope, err := json.Marshal(chunkPayload{SchemaVersion: manifest.SchemaVersion, BatchID: manifest.BatchID, Index: upperBound, Count: upperBound, Repositories: []json.RawMessage{}})
	if err != nil {
		return nil, err
	}

	groups := [][]json.RawMessage{{}}
	size := len(envelope)
	for _, repository := range manifest.Repositories {
		record, err := json.Marshal(repository)
		if err != nil {
			return nil, err
		}
		recordSize := len(record) + 1 // separating comma
		if len(envelope)+recordSize > maxChunkBytes {
			return nil, fmt.Errorf("repository %s cannot fit in a %d-byte chunk", repository.ID, maxChunkBytes)
		}
		if size+recordSize > maxChunkBytes {
			groups = append(groups, nil)
			size = len(envelope)
		}
		groups[len(groups)-1] = append(groups[len(groups)-1], record)
		size += recordSize
	}

	chunks := make([]Chunk, 0, len(groups))
	for index, group := range groups {
		data, err := json.Marshal(chunkPayload{SchemaVersion: manifest.SchemaVersion, BatchID: manifest.BatchID, Index: index, Count: len(groups), Repositories: group})
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, Chunk{Index: index, Count: len(groups), Digest: digest(data), Data: data})
	}
	return chunks, nil
}

// DecodeManifest reassembles chunks in any order and rejects missing, duplicate
// or altered chunks.
func DecodeManifest(chunks []Chunk) (Manifest, error) {
	if len(chunks) == 0 {
		return Manifest{}, fmt.Errorf("manifest chunks are required")
	}
	ordered := slices.Clone(chunks)
	slices.SortFunc(ordered, func(a, b Chunk) int { return a.Index - b.Index })
	count := ordered[0].Count
	if count <= 0 || len(ordered) != count {
		return Manifest{}, fmt.Errorf("manifest requires %d chunks, got %d", count, len(ordered))
	}

	result := Manifest{Repositories: []register.Repository{}}
	for expected, chunk := range ordered {
		if chunk.Index != expected {
			return Manifest{}, fmt.Errorf("manifest chunk index %d missing or duplicated", expected)
		}
		if chunk.Count != count {
			return Manifest{}, fmt.Errorf("manifest chunk %d has inconsistent count", chunk.Index)
		}
		if digest(chunk.Data) != chunk.Digest {
			return Manifest{}, fmt.Errorf("manifest chunk %d: digest mismatch", chunk.Index)
		}
		var payload struct {
			SchemaVersion int                   `json:"schemaVersion"`
			BatchID       string                `json:"batchId"`
			Index         int                   `json:"index"`
			Count         int                   `json:"count"`
			Repositories  []register.Repository `json:"repositories"`
		}
		if err := json.Unmarshal(chunk.Data, &payload); err != nil {
			return Manifest{}, fmt.Errorf("manifest chunk %d: %w", chunk.Index, err)
		}
		if payload.Index != chunk.Index || payload.Count != chunk.Count {
			return Manifest{}, fmt.Errorf("manifest chunk %d metadata mismatch", chunk.Index)
		}
		if expected == 0 {
			result.SchemaVersion, result.BatchID = payload.SchemaVersion, payload.BatchID
		} else if payload.SchemaVersion != result.SchemaVersion || payload.BatchID != result.BatchID {
			return Manifest{}, fmt.Errorf("manifest chunk %d belongs to a different manifest", chunk.Index)
		}
		result.Repositories = append(result.Repositories, payload.Repositories...)
	}
	return result, result.validate()
}

func (manifest Manifest) Repository(index int) (register.Repository, error) {
	if index < 0 || index >= len(manifest.Repositories) {
		return register.Repository{}, fmt.Errorf("repository index %d outside range 0..%d", index, len(manifest.Repositories)-1)
	}
	return manifest.Repositories[index], nil
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

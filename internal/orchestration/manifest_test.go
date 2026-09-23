package orchestration

import (
	"slices"
	"strings"
	"testing"

	"github.com/developer-overheid-nl/ort-runner/internal/register"
)

func TestManifestRoundTripAcrossChunks(t *testing.T) {
	input := Manifest{SchemaVersion: 1, BatchID: "batch-1", Repositories: []register.Repository{
		{ID: "b", URL: "https://example.test/b.git"},
		{ID: "a", URL: "https://example.test/a.git"},
	}}
	limit := singleRepositoryChunkLimit(t, input)
	chunks, err := EncodeManifest(input, limit)
	if err != nil || len(chunks) < 2 {
		t.Fatalf("chunks=%d err=%v", len(chunks), err)
	}
	for _, chunk := range chunks {
		if len(chunk.Data) > limit {
			t.Fatalf("chunk exceeds limit: %d", len(chunk.Data))
		}
	}
	slices.Reverse(chunks)
	got, err := DecodeManifest(chunks)
	if err != nil || len(got.Repositories) != 2 || got.Repositories[0].ID != "a" || got.Repositories[1].ID != "b" {
		t.Fatalf("manifest=%+v err=%v", got, err)
	}
	first, err := got.Repository(0)
	if err != nil || first.ID != "a" {
		t.Fatalf("repository=%+v err=%v", first, err)
	}
}

func TestManifestRejectsInvalidInput(t *testing.T) {
	valid := Manifest{SchemaVersion: 1, BatchID: "batch-1", Repositories: []register.Repository{{ID: "a", URL: "https://example.test/a.git"}}}
	for _, tc := range []struct {
		name  string
		input Manifest
		limit int
	}{
		{name: "non-positive limit", input: valid, limit: 0},
		{name: "empty batch ID", input: Manifest{SchemaVersion: 1, Repositories: valid.Repositories}, limit: 1024},
		{name: "unsupported schema", input: Manifest{SchemaVersion: 2, BatchID: "batch-1", Repositories: valid.Repositories}, limit: 1024},
		{name: "empty repository ID", input: Manifest{SchemaVersion: 1, BatchID: "batch-1", Repositories: []register.Repository{{URL: "https://example.test/a.git"}}}, limit: 1024},
		{name: "duplicate repository ID", input: Manifest{SchemaVersion: 1, BatchID: "batch-1", Repositories: []register.Repository{{ID: "a"}, {ID: "a"}}}, limit: 1024},
		{name: "single record too large", input: valid, limit: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := EncodeManifest(tc.input, tc.limit); err == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
}

func TestManifestRejectsBrokenChunks(t *testing.T) {
	input := Manifest{SchemaVersion: 1, BatchID: "batch-1", Repositories: []register.Repository{
		{ID: "a", URL: "https://example.test/a.git"},
		{ID: "b", URL: "https://example.test/b.git"},
	}}
	chunks, err := EncodeManifest(input, singleRepositoryChunkLimit(t, input))
	if err != nil || len(chunks) < 2 {
		t.Fatalf("fixture chunks=%d err=%v", len(chunks), err)
	}
	clone := func() []Chunk {
		result := append([]Chunk(nil), chunks...)
		for i := range result {
			result[i].Data = append([]byte(nil), result[i].Data...)
		}
		return result
	}
	for _, tc := range []struct {
		name   string
		mutate func([]Chunk) []Chunk
	}{
		{name: "missing", mutate: func(cs []Chunk) []Chunk { return cs[:len(cs)-1] }},
		{name: "duplicate", mutate: func(cs []Chunk) []Chunk { return append(cs, cs[0]) }},
		{name: "digest", mutate: func(cs []Chunk) []Chunk { cs[0].Digest = strings.Repeat("0", 64); return cs }},
		{name: "data", mutate: func(cs []Chunk) []Chunk { cs[0].Data[0] ^= 0xff; return cs }},
		{name: "inconsistent count", mutate: func(cs []Chunk) []Chunk { cs[0].Count++; return cs }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeManifest(tc.mutate(clone())); err == nil {
				t.Fatal("broken chunks accepted")
			}
		})
	}
	if _, err := DecodeManifest(nil); err == nil {
		t.Fatal("empty chunk set accepted")
	}
}

func singleRepositoryChunkLimit(t *testing.T, manifest Manifest) int {
	t.Helper()
	limit := 0
	for _, repository := range manifest.Repositories {
		chunks, err := EncodeManifest(Manifest{SchemaVersion: manifest.SchemaVersion, BatchID: manifest.BatchID, Repositories: []register.Repository{repository}}, 1<<20)
		if err != nil || len(chunks) != 1 {
			t.Fatalf("single repository chunk: chunks=%d err=%v", len(chunks), err)
		}
		limit = max(limit, len(chunks[0].Data))
	}
	return limit
}

func TestManifestRepositoryRejectsInvalidIndexes(t *testing.T) {
	manifest := Manifest{SchemaVersion: 1, BatchID: "batch-1", Repositories: []register.Repository{{ID: "a"}}}
	for _, index := range []int{-1, 1} {
		if _, err := manifest.Repository(index); err == nil {
			t.Fatalf("index %d accepted", index)
		}
	}
}

//go:build integration

package redisx

import (
	"context"
	"testing"
	"time"
)

func TestCache_Protocol(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig("n")
	client := newTestClient(t, cfg)
	c := NewCache(client, cfg)
	tags := []string{"orders", "customers"}

	if _, ok, err := c.Get(ctx, "GetOrder:missing"); err != nil || ok {
		t.Fatalf("miss: %v %v", ok, err)
	}
	snap, err := c.SnapshotTags(ctx, tags)
	if err != nil || snap["orders"] != 0 || snap["customers"] != 0 || len(snap) != 2 {
		t.Fatalf("snapshot %v %v", snap, err)
	}
	if empty, err := c.SnapshotTags(ctx, nil); err != nil || len(empty) != 0 {
		t.Fatalf("empty snapshot %v %v", empty, err)
	}
	body := []byte(`{"id":"o1","total":9007199254740993,"z":{"b":1,"a":2}}`)
	stored, err := c.SetChecked(ctx, "GetOrder:o1", snap, body, time.Minute)
	if err != nil || !stored {
		t.Fatalf("set: %v %v", stored, err)
	}
	got, ok, err := c.Get(ctx, "GetOrder:o1")
	if err != nil || !ok || string(got) != string(body) {
		t.Fatalf("hit: %s %v %v", got, ok, err)
	}
	if ttl := client.PTTL(ctx, cfg.Keys().Cache("GetOrder:o1")).Val(); ttl <= 0 || ttl > time.Minute {
		t.Fatalf("ttl %s", ttl)
	}

	// A bump makes the entry stale: cache_get UNLINKs it.
	if err := c.BumpTagsPre(ctx, []string{"orders"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := c.Get(ctx, "GetOrder:o1"); err != nil || ok {
		t.Fatalf("stale hit: %v %v", ok, err)
	}
	if n := client.Exists(ctx, cfg.Keys().Cache("GetOrder:o1")).Val(); n != 0 {
		t.Fatal("stale entry not unlinked")
	}
	// cache_set refuses a snapshot whose tag moved.
	stored, err = c.SetChecked(ctx, "GetOrder:o1", snap, body, 0)
	if err != nil || stored {
		t.Fatalf("stale set accepted: %v %v", stored, err)
	}
	if err := c.Set(ctx, "GetOrder:o1", snap, body, 0); err != nil {
		t.Fatal(err)
	}
	if n := client.Exists(ctx, cfg.Keys().Cache("GetOrder:o1")).Val(); n != 0 {
		t.Fatal("stale set wrote the entry")
	}
	// A fresh snapshot stores with the default TTL.
	snap, _ = c.SnapshotTags(ctx, tags)
	if snap["orders"] != 1 {
		t.Fatalf("snapshot after bump %v", snap)
	}
	if err := c.Set(ctx, "GetOrder:o1", snap, body, 0); err != nil {
		t.Fatal(err)
	}
	if ttl := client.PTTL(ctx, cfg.Keys().Cache("GetOrder:o1")).Val(); ttl <= 50*time.Second || ttl > time.Minute {
		t.Fatalf("default ttl %s", ttl)
	}
	if _, ok, _ := c.Get(ctx, "GetOrder:o1"); !ok {
		t.Fatal("fresh entry missed")
	}
	// The post-commit bump (a different fault point) also invalidates.
	if err := c.BumpTagsPost(ctx, []string{"customers"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := c.Get(ctx, "GetOrder:o1"); ok {
		t.Fatal("entry survived a post-commit bump")
	}
	if err := c.BumpTagsPre(ctx, nil); err != nil {
		t.Fatal(err)
	}

	// Entries without tags are always valid; tags with odd names survive.
	if err := c.Set(ctx, "NoTags:x", map[string]int64{}, []byte(`[]`), time.Minute); err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := c.Get(ctx, "NoTags:x"); !ok || string(got) != "[]" {
		t.Fatalf("no tags: %s %v", got, ok)
	}
	odd := map[string]int64{`we"ird}tag\`: 0, "b": 0}
	if err := c.Set(ctx, "Odd:x", odd, []byte(`{"b":{"v":{"b":1}}}`), time.Minute); err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := c.Get(ctx, "Odd:x"); !ok || string(got) != `{"b":{"v":{"b":1}}}` {
		t.Fatalf("odd tags: %s %v", got, ok)
	}
	if err := c.BumpTagsPost(ctx, []string{`we"ird}tag\`}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := c.Get(ctx, "Odd:x"); ok {
		t.Fatal("odd tag bump ignored")
	}

	// Corrupt entries are treated as misses and removed.
	for _, garbage := range []string{"nope", `{"v":{"a":1}}`, `{"v":{"a":"x"},"b":1}`, `{"v":{"a":1},"x":1}`} {
		key := cfg.Keys().Cache("Corrupt:x")
		if err := client.Set(ctx, key, garbage, time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := c.Get(ctx, "Corrupt:x"); err != nil || ok {
			t.Fatalf("corrupt %q: %v %v", garbage, ok, err)
		}
		if client.Exists(ctx, key).Val() != 0 {
			t.Fatalf("corrupt entry %q not unlinked", garbage)
		}
	}
	if err := c.Delete(ctx, "NoTags:x", "missing"); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := c.Get(ctx, "NoTags:x"); ok {
		t.Fatal("deleted entry hit")
	}
}

func TestCache_ClosedClient(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig("n")
	c := NewCache(closedClient(t, cfg), cfg)
	if _, _, err := c.Get(ctx, "k"); err == nil {
		t.Fatal("get")
	}
	if _, err := c.SnapshotTags(ctx, []string{"a"}); err == nil {
		t.Fatal("snapshot")
	}
	if err := c.Set(ctx, "k", map[string]int64{"a": 0}, []byte("1"), time.Second); err == nil {
		t.Fatal("set")
	}
	if err := c.BumpTagsPre(ctx, []string{"a"}); err == nil {
		t.Fatal("bump pre")
	}
	if err := c.BumpTagsPost(ctx, []string{"a"}); err == nil {
		t.Fatal("bump post")
	}
	if err := c.Delete(ctx, "k"); err == nil {
		t.Fatal("delete")
	}
}

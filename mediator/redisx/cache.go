package redisx

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// Cache implements the tag-version protocol of spec 7.4 over Redis. Entries
// are JSON documents `{"v":{"tag":ver,...},"b":<body>}`; an entry is valid
// only while every tag it recorded still has the same version, and
// invalidation is an INCR per tag. The behavior package's CacheBackend
// interface is satisfied by *Cache.
type Cache struct {
	client *redis.Client
	cfg    Config
	keys   Keys
	logger *slog.Logger
}

// NewCache returns a cache over client with the prefix and default TTL of cfg.
func NewCache(client *redis.Client, cfg Config) *Cache {
	cfg = cfg.WithDefaults()
	return &Cache{client: client, cfg: cfg, keys: cfg.Keys(), logger: slog.Default().WithGroup("mediator")}
}

// cacheGetScript is Lua cache_get (spec 7.4). It extracts the version object
// textually rather than decoding the whole entry, because cjson would
// re-encode the body (losing int64 precision and key order). KEYS[1] is the
// entry, ARGV[1] the tag-version key prefix.
var cacheGetScript = redis.NewScript(`
local entry = redis.call('GET', KEYS[1])
if not entry then return false end
if string.sub(entry, 1, 6) ~= '{"v":{' then redis.call('UNLINK', KEYS[1]) return false end
local n = #entry
local i = 6
local depth = 0
local instr = false
local esc = false
local endv = nil
while i <= n do
  local ch = string.sub(entry, i, i)
  if instr then
    if esc then esc = false
    elseif ch == '\\' then esc = true
    elseif ch == '"' then instr = false end
  else
    if ch == '"' then instr = true
    elseif ch == '{' then depth = depth + 1
    elseif ch == '}' then
      depth = depth - 1
      if depth == 0 then endv = i break end
    end
  end
  i = i + 1
end
if not endv or string.sub(entry, endv + 1, endv + 5) ~= ',"b":' or string.sub(entry, n, n) ~= '}' then
  redis.call('UNLINK', KEYS[1]) return false
end
local ok, vers = pcall(cjson.decode, string.sub(entry, 6, endv))
if not ok or type(vers) ~= 'table' then redis.call('UNLINK', KEYS[1]) return false end
for tag, ver in pairs(vers) do
  local cur = redis.call('GET', ARGV[1] .. tag)
  if tonumber(cur or '0') ~= tonumber(ver) then
    redis.call('UNLINK', KEYS[1]) return false
  end
end
return string.sub(entry, endv + 6, n - 1)`)

// cacheSetScript is Lua cache_set (spec 7.4): compare every tag version with
// the snapshot; store with PX only when all are equal. KEYS[1] is the entry,
// KEYS[2..] the tag-version keys; ARGV[1] the TTL in ms, ARGV[2] the entry,
// ARGV[3..] the snapshot versions aligned with KEYS[2..].
var cacheSetScript = redis.NewScript(`
for i = 2, #KEYS do
  local cur = redis.call('GET', KEYS[i])
  if tonumber(cur or '0') ~= tonumber(ARGV[i + 1]) then return 0 end
end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[1])
return 1`)

// CacheKey returns the logical cache key of a request, "<name>:<hex sha256
// of the canonical JSON>". Cache methods prepend "<prefix>:cache:".
func CacheKey(name string, req any) (string, error) {
	h, err := mediator.CanonicalHash(req)
	if err != nil {
		return "", fmt.Errorf("redisx: cache key for %s: %w", name, err)
	}
	return name + ":" + hex.EncodeToString(h[:]), nil
}

// encodeCacheEntry builds `{"v":{...},"b":<body>}` with sorted tag names.
func encodeCacheEntry(versions map[string]int64, body []byte) []byte {
	tags := make([]string, 0, len(versions))
	for t := range versions {
		tags = append(tags, t)
	}
	sort.Strings(tags)
	var buf bytes.Buffer
	buf.WriteString(`{"v":{`)
	for i, t := range tags {
		if i > 0 {
			buf.WriteByte(',')
		}
		name, _ := json.Marshal(t)
		buf.Write(name)
		buf.WriteByte(':')
		buf.WriteString(strconv.FormatInt(versions[t], 10))
	}
	buf.WriteString(`},"b":`)
	buf.Write(body)
	buf.WriteByte('}')
	return buf.Bytes()
}

// decodeCacheEntry is the Go mirror of the script's parsing, used by tests
// and ops tooling.
func decodeCacheEntry(entry []byte) (map[string]int64, []byte, error) {
	const head = `{"v":{`
	if !bytes.HasPrefix(entry, []byte(head)) || len(entry) < len(head)+1 || entry[len(entry)-1] != '}' {
		return nil, nil, errors.New("redisx: malformed cache entry")
	}
	depth, instr, esc := 0, false, false
	endv := -1
	for i := len(head) - 1; i < len(entry); i++ {
		ch := entry[i]
		if instr {
			switch {
			case esc:
				esc = false
			case ch == '\\':
				esc = true
			case ch == '"':
				instr = false
			}
			continue
		}
		switch ch {
		case '"':
			instr = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				endv = i
			}
		}
		if endv >= 0 {
			break
		}
	}
	if endv < 0 || !bytes.HasPrefix(entry[endv+1:], []byte(`,"b":`)) {
		return nil, nil, errors.New("redisx: malformed cache entry")
	}
	var versions map[string]int64
	if err := json.Unmarshal(entry[len(head)-1:endv+1], &versions); err != nil {
		return nil, nil, fmt.Errorf("redisx: malformed cache entry versions: %w", err)
	}
	body := entry[endv+6 : len(entry)-1]
	return versions, body, nil
}

// Get runs cache_get: it returns the body when every recorded tag version is
// current, and UNLINKs the entry otherwise. Fault point redis.cache.get.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := testkit.Fault(ctx, "redis.cache.get"); err != nil {
		return nil, false, err
	}
	res, err := cacheGetScript.Run(ctx, c.client, []string{c.keys.Cache(key)}, c.keys.TagVerPrefix()).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("redisx: cache get: %w", err)
	}
	body, ok := res.(string)
	if !ok {
		return nil, false, fmt.Errorf("redisx: cache get: unexpected reply %T", res)
	}
	return []byte(body), true, nil
}

// SnapshotTags returns the current version of each tag with one MGET;
// missing tags are 0.
func (c *Cache) SnapshotTags(ctx context.Context, tags []string) (map[string]int64, error) {
	out := make(map[string]int64, len(tags))
	if len(tags) == 0 {
		return out, nil
	}
	keys := make([]string, len(tags))
	for i, t := range tags {
		keys[i] = c.keys.TagVer(t)
	}
	vals, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redisx: snapshot tags: %w", err)
	}
	for i, t := range tags {
		var n int64
		if i < len(vals) && vals[i] != nil {
			s, _ := vals[i].(string)
			n, _ = strconv.ParseInt(s, 10, 64)
		}
		out[t] = n
	}
	return out, nil
}

// Set stores body under key with the versions of snapshot, unless any tag
// moved since the snapshot, in which case the entry is not written and nil
// is returned. A ttl of zero uses CacheDefaultTTL. Fault point redis.cache.set.
func (c *Cache) Set(ctx context.Context, key string, snapshot map[string]int64, body []byte, ttl time.Duration) error {
	_, err := c.SetChecked(ctx, key, snapshot, body, ttl)
	return err
}

// SetChecked is Set that also reports whether the entry was stored.
func (c *Cache) SetChecked(ctx context.Context, key string, snapshot map[string]int64, body []byte, ttl time.Duration) (stored bool, err error) {
	if err := testkit.Fault(ctx, "redis.cache.set"); err != nil {
		return false, err
	}
	if ttl <= 0 {
		ttl = c.cfg.CacheDefaultTTL
	}
	tags := make([]string, 0, len(snapshot))
	for t := range snapshot {
		tags = append(tags, t)
	}
	sort.Strings(tags)
	keys := make([]string, 0, len(tags)+1)
	keys = append(keys, c.keys.Cache(key))
	args := make([]any, 0, len(tags)+2)
	args = append(args, ttl.Milliseconds(), encodeCacheEntry(snapshot, body))
	for _, t := range tags {
		keys = append(keys, c.keys.TagVer(t))
		args = append(args, snapshot[t])
	}
	n, err := cacheSetScript.Run(ctx, c.client, keys, args...).Int64()
	if err != nil {
		return false, fmt.Errorf("redisx: cache set: %w", err)
	}
	return n == 1, nil
}

// BumpTagsPre increments every tag version before the command commits.
// Fault point redis.tag.bump.pre.
func (c *Cache) BumpTagsPre(ctx context.Context, tags []string) error {
	if err := testkit.Fault(ctx, "redis.tag.bump.pre"); err != nil {
		return err
	}
	return c.bump(ctx, tags)
}

// BumpTagsPost increments every tag version after the command committed.
// Fault point redis.tag.bump.post.
func (c *Cache) BumpTagsPost(ctx context.Context, tags []string) error {
	if err := testkit.Fault(ctx, "redis.tag.bump.post"); err != nil {
		return err
	}
	return c.bump(ctx, tags)
}

func (c *Cache) bump(ctx context.Context, tags []string) error {
	if len(tags) == 0 {
		return nil
	}
	pipe := c.client.Pipeline()
	for _, t := range tags {
		pipe.Incr(ctx, c.keys.TagVer(t))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redisx: bump tags: %w", err)
	}
	return nil
}

// Delete removes cache entries by logical key (UNLINK).
func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = c.keys.Cache(k)
	}
	if err := c.client.Unlink(ctx, full...).Err(); err != nil {
		return fmt.Errorf("redisx: cache delete: %w", err)
	}
	return nil
}

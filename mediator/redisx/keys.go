package redisx

import (
	"fmt"
	"strconv"
	"strings"
)

// Keys builds every key of the layout in spec 7.1 under one prefix. Hash tags
// keep everything for one partition in one slot so the layout survives a
// future move to Redis Cluster.
type Keys struct {
	// Prefix replaces the literal "mediator" of the spec.
	Prefix string
}

// Stream is the partition stream `<prefix>:{<topic>:p<n>}`.
func (k Keys) Stream(topic string, partition int) string {
	return fmt.Sprintf("%s:{%s:p%d}", k.Prefix, topic, partition)
}

// DLQ is the dead-letter stream of a consumer group, `<prefix>:dlq:{<group>}`.
func (k Keys) DLQ(group string) string { return k.Prefix + ":dlq:{" + group + "}" }

// Lease is the lease string `<prefix>:lease:{<group>:<topic>:p<n>}`.
func (k Keys) Lease(group, topic string, partition int) string {
	return fmt.Sprintf("%s:lease:{%s:%s:p%d}", k.Prefix, group, topic, partition)
}

// LeasePattern is the SCAN pattern matching every lease of a group.
func (k Keys) LeasePattern(group string) string {
	return k.Prefix + ":lease:{" + group + ":*}"
}

// Members is the membership zset of a consumer group, `<prefix>:members:<group>`.
func (k Keys) Members(group string) string { return k.Prefix + ":members:" + group }

// Cache is the cache entry key `<prefix>:cache:<query>:<hash>`; key is the
// logical part produced by CacheKey.
func (k Keys) Cache(key string) string { return k.Prefix + ":cache:" + key }

// TagVer is the version key of a cache tag, `<prefix>:tagver:<tag>`.
func (k Keys) TagVer(tag string) string { return k.Prefix + ":tagver:" + tag }

// TagVerPrefix is the prefix the cache_get script prepends to tag names.
func (k Keys) TagVerPrefix() string { return k.Prefix + ":tagver:" }

// RateLimit is the GCRA state key `<prefix>:rl:<name>:<key>`.
func (k Keys) RateLimit(name, key string) string { return k.Prefix + ":rl:" + name + ":" + key }

// RPC is the remote dispatch request stream of one command type,
// `<prefix>:rpc:<request>`.
func (k Keys) RPC(request string) string { return k.Prefix + ":rpc:" + request }

// Reply is the reply stream of one node, `<prefix>:reply:<nodeID>`.
func (k Keys) Reply(nodeID string) string { return k.Prefix + ":reply:" + nodeID }

// Handlers is the zset of nodes serving a request type, `<prefix>:handlers:<request>`.
func (k Keys) Handlers(request string) string { return k.Prefix + ":handlers:" + request }

// ParseLease splits a lease key produced by Lease back into its parts.
func (k Keys) ParseLease(key string) (group, topic string, partition int, ok bool) {
	prefix := k.Prefix + ":lease:{"
	if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, "}") {
		return "", "", 0, false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(key, prefix), "}")
	i := strings.LastIndexByte(body, ':')
	if i < 0 || !strings.HasPrefix(body[i+1:], "p") {
		return "", "", 0, false
	}
	n, err := strconv.Atoi(body[i+2:])
	if err != nil || n < 0 {
		return "", "", 0, false
	}
	rest := body[:i]
	j := strings.IndexByte(rest, ':')
	if j <= 0 || j == len(rest)-1 {
		return "", "", 0, false
	}
	return rest[:j], rest[j+1:], n, true
}

// ParseRPC returns the request name of an RPC stream key.
func (k Keys) ParseRPC(key string) (string, bool) {
	prefix := k.Prefix + ":rpc:"
	if !strings.HasPrefix(key, prefix) || len(key) == len(prefix) {
		return "", false
	}
	return key[len(prefix):], true
}

// LeaseValue encodes the lease string "<nodeID>:<epoch>".
func LeaseValue(nodeID string, epoch int64) string {
	return nodeID + ":" + strconv.FormatInt(epoch, 10)
}

// ParseLeaseValue splits "<nodeID>:<epoch>". Node IDs may contain colons;
// the epoch is everything after the last one.
func ParseLeaseValue(v string) (nodeID string, epoch int64, ok bool) {
	i := strings.LastIndexByte(v, ':')
	if i <= 0 {
		return "", 0, false
	}
	n, err := strconv.ParseInt(v[i+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return v[:i], n, true
}

// streamIDMillis returns the millisecond part of a stream ID such as
// "1700000000000-3".
func streamIDMillis(id string) (int64, bool) {
	if i := strings.IndexByte(id, '-'); i >= 0 {
		id = id[:i]
	}
	ms, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return 0, false
	}
	return ms, true
}

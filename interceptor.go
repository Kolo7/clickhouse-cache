package sqlcache

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ngrok/sqlmw"
	"github.com/prashanthpai/sqlcache/cache"
)

// Config is the configuration passed to NewInterceptor for creating new
// Interceptor instances.
type Config struct {
	// Cache must be set to a type that implements the cache.Cacher interface
	// which abstracts the backend cache implementation. This is a required
	// field and cannot be nil.
	Cache cache.Cacher
	// OnError is called whenever methods of cache.Cacher interface or HashFunc
	// returns error. Since sqlcache package does not log any failures, you can
	// use this hook to log errors or even choose to disable/bypass sqlcache.
	OnError func(error)
	// HashFunc can be optionally set to provide a custom hashing function. By
	// default sqlcache uses mitchellh/hashstructure which internally uses FNV.
	// If hash collision is a concern to you, consider using NoopHash.
	HashFunc func(query string, args []driver.NamedValue) (string, error)
}

// Interceptor is a ngrok/sqlmw interceptor that caches SQL queries and
// their responses.
type Interceptor struct {
	c        cache.Cacher
	hashFunc func(query string, args []driver.NamedValue) (string, error)
	onErr    func(error)
	stats    Stats
	disabled bool
	sqlmw.NullInterceptor
	KeyRWLock
}

// NewInterceptor returns a new instance of sqlcache interceptor initialised
// with the provided config.
func NewInterceptor(config *Config) (*Interceptor, error) {
	if config == nil {
		return nil, fmt.Errorf("config can't be nil")
	}

	if config.Cache == nil {
		return nil, fmt.Errorf("cache must be set in Config")
	}

	if config.HashFunc == nil {
		config.HashFunc = defaultHashFunc
	}

	return &Interceptor{
		config.Cache,
		config.HashFunc,
		config.OnError,
		Stats{},
		false,
		sqlmw.NullInterceptor{},
		KeyRWLock{},
	}, nil
}

// Driver returns the supplied driver.Driver with a new object that has
// all of its calls intercepted by the sqlcache.Interceptor. Any DB call
// without a context passed will not be intercepted.
func (i *Interceptor) Driver(d driver.Driver) driver.Driver {
	return sqlmw.Driver(d, i)
}

// Enable enables the interceptor. Interceptor instance is enabled by default
// on creation.
func (i *Interceptor) Enable() {
	i.disabled = false
}

// Disable disables the interceptor resulting in cache bypass. All queries
// would go directly to the SQL backend.
func (i *Interceptor) Disable() {
	i.disabled = true
}

// StmtQueryContext intecepts database/sql's stmt.QueryContext calls from a prepared statement.
func (i *Interceptor) StmtQueryContext(ctx context.Context, conn driver.StmtQueryContext, query string, args []driver.NamedValue) (driver.Rows, error) {

	if i.disabled {
		return conn.QueryContext(ctx, args)
	}

	attrs := getAttrs(query)
	if attrs == nil {
		return conn.QueryContext(ctx, args)
	}

	hash, err := i.hashFunc(query, args)
	if err != nil {
		atomic.AddUint64(&i.stats.Errors, 1)
		if i.onErr != nil {
			i.onErr(fmt.Errorf("HashFunc failed: %w", err))
		}
		return conn.QueryContext(ctx, args)
	}
	// 按hash值加锁，相同的hash值db查询同时只能有一个，拿到写锁之后再查一次cache，有值就返回，没有就查sql
	// 进入加读锁
	i.RLock(hash)
	// log.Printf("clickhouse-cache, 拿到读锁\n")
	if cached := i.checkCache(hash); cached != nil {
		// log.Printf("clickhouse-cache, 命中缓存\n")
		i.RUnlock(hash)
		// log.Printf("clickhouse-cache, 释放读锁\n")
		return cached, nil
	}
	// log.Printf("clickhouse-cache, 缓存miss\n")
	// 释放读锁，加写锁
	i.RUnlock(hash)
	// log.Printf("clickhouse-cache, 释放读锁\n")
	i.Lock(hash)
	// log.Printf("clickhouse-cache, 拿到写锁\n")
	// 出错要解锁，或者是写入缓存后要解锁，正常流程结束方法还没有写入缓存不应该解锁
	wUnlockFunc := func() {
		i.Unlock(hash)
		// log.Printf("clickhouse-cache, 释放写锁\n")
	}

	// 读锁应该要有超时机制，如果超过一段时间没有解锁，要主动的去解锁，这里可能用锁无法实现了，要用协程同步工具

	// 加写锁后，再读一次cache
	if cached := i.checkCache(hash); cached != nil {
		wUnlockFunc()
		// log.Printf("clickhouse-cache, 命中缓存\n")
		return cached, nil
	}
	// log.Printf("clickhouse-cache, 缓存再次miss\n")
	// cache没有数据的话，再查db
	rows, err := conn.QueryContext(ctx, args)
	if err != nil {
		wUnlockFunc()
		return rows, err
	}
	// log.Printf("clickhouse-cache, 完成查询db\n")
	// 写cache后释放写锁
	cacheSetter := func(item *cache.Item) {
		defer wUnlockFunc()
		err := i.c.Set(hash, item, time.Duration(attrs.ttl)*time.Second)
		if err != nil {
			atomic.AddUint64(&i.stats.Errors, 1)
			if i.onErr != nil {
				i.onErr(fmt.Errorf("Cache.Set failed: %w", err))
			}
		}
		// log.Printf("clickhouse-cache, 写入缓存\n")
	}

	return newRowsRecorder(cacheSetter, rows, attrs.maxRows), err
}

// ConnQueryContext intecepts database/sql's DB.QueryContext Conn.QueryContext calls.
func (i *Interceptor) ConnQueryContext(ctx context.Context, conn driver.QueryerContext, query string, args []driver.NamedValue) (driver.Rows, error) {

	if i.disabled {
		return conn.QueryContext(ctx, query, args)
	}

	attrs := getAttrs(query)
	if attrs == nil {
		return conn.QueryContext(ctx, query, args)
	}

	hash, err := i.hashFunc(query, args)
	if err != nil {
		atomic.AddUint64(&i.stats.Errors, 1)
		if i.onErr != nil {
			i.onErr(fmt.Errorf("HashFunc failed: %w", err))
		}
		return conn.QueryContext(ctx, query, args)
	}

	if cached := i.checkCache(hash); cached != nil {
		return cached, nil
	}

	rows, err := conn.QueryContext(ctx, query, args)
	if err != nil {
		return rows, err
	}

	cacheSetter := func(item *cache.Item) {
		err := i.c.Set(hash, item, time.Duration(attrs.ttl)*time.Second)
		if err != nil {
			atomic.AddUint64(&i.stats.Errors, 1)
			if i.onErr != nil {
				i.onErr(fmt.Errorf("Cache.Set failed: %w", err))
			}
		}
	}

	return newRowsRecorder(cacheSetter, rows, attrs.maxRows), err
}

func (i *Interceptor) checkCache(hash string) driver.Rows {
	item, ok, err := i.c.Get(hash)
	if err != nil {
		atomic.AddUint64(&i.stats.Errors, 1)
		if i.onErr != nil {
			i.onErr(fmt.Errorf("Cache.Get failed: %w", err))
		}
		return nil
	}

	if !ok {
		atomic.AddUint64(&i.stats.Misses, 1)
		return nil
	}
	atomic.AddUint64(&i.stats.Hits, 1)

	return &rowsCached{
		item,
		0,
	}
}

// Stats contains sqlcache statistics.
type Stats struct {
	Hits   uint64
	Misses uint64
	Errors uint64
}

// Stats returns sqlcache stats.
func (i *Interceptor) Stats() *Stats {
	return &Stats{
		Hits:   atomic.LoadUint64(&i.stats.Hits),
		Misses: atomic.LoadUint64(&i.stats.Misses),
		Errors: atomic.LoadUint64(&i.stats.Errors),
	}
}

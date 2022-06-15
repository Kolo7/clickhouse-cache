package sqlcache

import (
	"sync"
	"sync/atomic"
)

/*
 带key的锁，不同key之间不影响,最后释放的锁将会检查是否有协程持有锁，没有话删掉锁
*/
type timerRwMutex struct {
	count int64
	sync.RWMutex
}

type KeyRWLock struct {
	keyLock sync.Map
}

func (k *KeyRWLock) RLock(key interface{}) *sync.RWMutex {
	tRw, _ := k.getKeyLock(key)
	// 指针对应的值加一，要确定加一之前是看到的值，count=-1代表锁已经从map移除了，用递归方式再次申请锁，如果是count已经被其他的协程修改过了再次重试。
	// 成功加一后，就锁定了lock不会从map移除
	for {
		count := tRw.count
		if count == -1 {
			return k.RLock(key)
		}
		if count != -1 && atomic.CompareAndSwapInt64(&tRw.count, count, count+1) {
			tRw.RLock()
			break
		}
	}
	return &tRw.RWMutex
}

func (k *KeyRWLock) Lock(key interface{}) *sync.RWMutex {
	tRw, _ := k.getKeyLock(key)
	for {
		count := tRw.count
		if atomic.CompareAndSwapInt64(&tRw.count, count, count+1) {
			tRw.Lock()
			break
		}
	}
	return &tRw.RWMutex
}

func (k *KeyRWLock) RUnlock(key interface{}) {
	tRw, _ := k.getKeyLock(key)

	// tRw是不会减成负数的，最后一个会减为0
	atomic.AddInt64(&tRw.count, -1)

	// 检查tRw.count是不是0，是0则置为-1，不再允许持有lock，然后从map移除这把锁
	if atomic.CompareAndSwapInt64(&tRw.count, 0, -1) {
		// log.Printf("clickhouse-cache, 把锁从map清除:%s", key)
		k.keyLock.Delete(key)
	}
	tRw.RUnlock()
}

func (k *KeyRWLock) Unlock(key interface{}) {
	tRw, _ := k.getKeyLock(key)
	atomic.AddInt64(&tRw.count, -1)
	if atomic.CompareAndSwapInt64(&tRw.count, 0, -1) {
		// log.Printf("clickhouse-cache, 把锁从map清除:%s", key)
		k.keyLock.Delete(key)
	}
	tRw.Unlock()
}

func (k *KeyRWLock) getKeyLock(key interface{}) (*timerRwMutex, bool) {
	tm := timerRwMutex{
		count: 0,
	}
	mwMutexI, loaded := k.keyLock.LoadOrStore(key, &tm)
	mwMutex := mwMutexI.(*timerRwMutex)
	return mwMutex, loaded
}

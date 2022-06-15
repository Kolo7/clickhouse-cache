package sqlcache

import (
	"log"
	"testing"
)

func TestKeyRWLock_Lock(t *testing.T) {
	lock := KeyRWLock{}
	lock.RLock(1)
	log.Printf("RLock(1)")
	lock.RLock(1)
	log.Printf("RLock(1)")
	lock.RLock(2)
	log.Printf("RLock(2)")
	lock.RLock(2)
	log.Printf("RLock(2)")

	lock.RUnlock(1)
	lock.RUnlock(1)
	log.Printf("RUnlock(1)")
	lock.RUnlock(2)
	lock.RUnlock(2)
	log.Printf("RUnlock(2)")

	lock.Lock(1)
	log.Printf("Lock(1)")
	lock.Lock(2)
	log.Printf("Lock(2)")
	lock.Unlock(1)
	log.Printf("Unlock(1)")
	lock.Unlock(2)
	log.Printf("Unlock(2)")
}

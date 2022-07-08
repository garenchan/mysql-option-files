package setaria

import "sync"

type HashMutex struct {
	slots []sync.RWMutex
	size  uint32
}

func NewHashMutex(size uint32) *HashMutex {
	return &HashMutex{
		slots: make([]sync.RWMutex, size),
		size:  size,
	}
}

func (hm *HashMutex) GetMutex(key string) *sync.RWMutex {
	hashed := HashToUint32([]byte(key))
	index := hashed % hm.size

	return &hm.slots[index]
}

package fg_mutex

import (
	"crypto/md5"
	"fmt"
	"reflect"
	"strconv"
	"unsafe"
	// "fmt"
	"sync"
	"sync/atomic"
)

type FGMutex struct {
	buckets               unsafe.Pointer
	keyshifts             uintptr
	autoDeleteAfterUnlock bool // whether auto delete mutex after unlock it

	index      []*Mutex
	linkedList *MutexList
}

type MutexList struct {
	count uintptr
	head  *Mutex
}

type Mutex struct {
	keyHash uintptr
	key     string

	*sync.RWMutex
	count uintptr

	fgm      *FGMutex
	previous unsafe.Pointer
	next     unsafe.Pointer
}

const (
	intSizeBytes = strconv.IntSize >> 3
)

// Next returns the mutex on the right.
func (m *Mutex) Next() *Mutex {
	return (*Mutex)(atomic.LoadPointer(&m.next))
}

// Previous returns the mutex on the left.
func (m *Mutex) Previous() *Mutex {
	return (*Mutex)(atomic.LoadPointer(&m.previous))
}

func (m *Mutex) isDeleted() (bool, uintptr) {
	count := atomic.LoadUintptr(&m.count)
	return count == uintptr(0), count
}

func (m *Mutex) Delete() {
	count := atomic.AddUintptr(&m.count, ^uintptr(0)) // decrease counter
	if count == uintptr(0) {
		// not in use, delete it
		m.fgm.deleteItem(m)
	}
}

func (m *Mutex) Unlock() {
	m.RWMutex.Unlock()
	fmt.Println("unlocked")

	if m.fgm.autoDeleteAfterUnlock {
		m.Delete()
	}
}

func NewMutexList() *MutexList {
	return &MutexList{head: &Mutex{}}
}

func (l *MutexList) Add(item *Mutex, searchStart *Mutex) (existing bool, inserted bool) {
	left, found, right := l.search(searchStart, item)
	if found != nil {
		return true, false
	}

	return false, l.insertAt(item, left, right)
}

func (l *MutexList) search(searchStart *Mutex, item *Mutex) (left *Mutex, found *Mutex, right *Mutex) {
	if searchStart != nil && item.keyHash < searchStart.keyHash {
		searchStart = nil
	}

	if searchStart == nil {
		left = l.head // no need to check whether head is nil
		found = left.Next()
		if found == nil {
			return nil, nil, nil
		}
	} else {
		found = searchStart
	}

	for {
		if item.keyHash == found.keyHash && item.key == found.key {
			return nil, found, nil
		}

		if item.keyHash < found.keyHash {
			if l.head == left {
				return nil, nil, found
			}
			return left, nil, found
		}

		// go to next element in sorted linked list
		left = found
		found = left.Next()
		if found == nil { // no more items on the right
			return left, nil, nil
		}
	}
}

func (l *MutexList) insertAt(item *Mutex, left *Mutex, right *Mutex) bool {
	if left == nil {
		left = l.head
	}

	// item->previous = left
	item.previous = unsafe.Pointer(left)
	// item->next = right
	item.next = unsafe.Pointer(right)

	// left->next = item
	if !atomic.CompareAndSwapPointer(&left.next, unsafe.Pointer(right), unsafe.Pointer(item)) {
		return false // item was modified concurrently
	}

	// right->previous = item
	if right != nil {
		if !atomic.CompareAndSwapPointer(&right.previous, unsafe.Pointer(l.head), unsafe.Pointer(item)) {
			return false // seems never happen, but in case
		}
	}

	atomic.AddUintptr(&l.count, 1)
	return true
}

// Delete deletes an element from the list.
func (l *MutexList) delete(item *Mutex) {
	for {
		left := item.Previous()
		right := item.Next()

		if left == nil {
			// seems never happen, just in case
			left = l.head
		}

		if !atomic.CompareAndSwapPointer(&left.next, unsafe.Pointer(item), unsafe.Pointer(right)) {
			continue // item was modified concurrently
		}

		if right != nil {
			atomic.CompareAndSwapPointer(&right.previous, unsafe.Pointer(item), unsafe.Pointer(left))
		}
		break
	}

	atomic.AddUintptr(&l.count, ^uintptr(0)) // decrease counter
}

func NewFGMutex(size uintptr, autoDeleteAfterUnlock bool) *FGMutex {
	fgm := &FGMutex{
		autoDeleteAfterUnlock: autoDeleteAfterUnlock,
	}
	fgm.allocate(size)
	return fgm
}

func (fgm *FGMutex) allocate(size uintptr) {
	size = roundUpPower2(size)

	fgm.keyshifts = strconv.IntSize - log2(size)
	fgm.index = make([]*Mutex, size)
	header := (*reflect.SliceHeader)(unsafe.Pointer(&fgm.index))
	fgm.buckets = unsafe.Pointer(header.Data)

	fgm.linkedList = NewMutexList()
}

func (fgm *FGMutex) Get(key string) *Mutex {
	h := getKeyHash(key)
	var newMutex *Mutex

	for {
		mutex := fgm.indexMutex(h)

		for mutex != nil {
			if mutex.keyHash == h && mutex.key == key {
				isDeleted, count := mutex.isDeleted()
				if isDeleted {
					break // is deleting
				}

				if atomic.CompareAndSwapUintptr(&mutex.count, count, count+1) {
					return mutex
				} else {
					// concurrency
					break
				}
			}

			if mutex.keyHash > h {
				break
			}

			mutex = mutex.Next()
		}

		// allocate only once
		if newMutex == nil {
			newMutex = &Mutex{
				key:     key,
				keyHash: h,
				RWMutex: &sync.RWMutex{},
				count:   1,
				fgm:     fgm,
			}
		}

		if fgm.insertMutexList(newMutex) {
			return newMutex
		}
	}
}

func (fgm *FGMutex) indexMutex(hashedKey uintptr) *Mutex {
	index := hashedKey >> fgm.keyshifts
	ptr := (*unsafe.Pointer)(unsafe.Pointer(uintptr(fgm.buckets) + index*intSizeBytes))
	item := (*Mutex)(atomic.LoadPointer(ptr))
	return item
}

func (fgm *FGMutex) insertMutexList(item *Mutex) bool {
	for {
		existing := fgm.indexMutex(item.keyHash)

		existed, inserted := fgm.linkedList.Add(item, existing)
		if existed {
			return false
		}
		if !inserted {
			continue
		}

		fgm.addItemToIndex(item)
		return true
	}
}

func (fgm *FGMutex) addItemToIndex(item *Mutex) {
	index := item.keyHash >> fgm.keyshifts
	ptr := (*unsafe.Pointer)(unsafe.Pointer(uintptr(fgm.buckets) + index*intSizeBytes))

	for { // loop until the smallest key hash is in the index
		existing := (*Mutex)(atomic.LoadPointer(ptr)) // get the current item in the index

		if existing == nil {
			if atomic.CompareAndSwapPointer(ptr, nil, unsafe.Pointer(item)) {
				return
			}
			continue // a new item was inserted concurrently, retry
		}

		if item.keyHash < existing.keyHash {
			// the new item is the smallest for this index?
			if !atomic.CompareAndSwapPointer(ptr, unsafe.Pointer(existing), unsafe.Pointer(item)) {
				continue // a new item was inserted concurrently, retry
			}
		}

		break
	}
}

func (fgm *FGMutex) deleteItem(item *Mutex) {
	index := item.keyHash >> fgm.keyshifts
	ptr := (*unsafe.Pointer)(unsafe.Pointer(uintptr(fgm.buckets) + index*intSizeBytes))

	next := item.Next()
	atomic.CompareAndSwapPointer(ptr, unsafe.Pointer(item), unsafe.Pointer(next))

	fgm.linkedList.delete(item)
}

func getKeyHash(key string) (v uintptr) {
	h := md5.New()
	h.Write([]byte(key))

	b := h.Sum(nil)

	v += uintptr(b[0])
	v <<= 8
	v += uintptr(b[1])
	v <<= 8
	v += uintptr(b[2])
	v <<= 8
	v += uintptr(b[3])

	return
}

// roundUpPower2 rounds a number to the next power of 2.
func roundUpPower2(i uintptr) uintptr {
	i--
	i |= i >> 1
	i |= i >> 2
	i |= i >> 4
	i |= i >> 8
	i |= i >> 16
	i |= i >> 32
	i++
	return i
}

// log2 computes the binary logarithm of x, rounded up to the next integer.
func log2(i uintptr) uintptr {
	var n, p uintptr
	for p = 1; p < i; p += p {
		n++
	}
	return n
}

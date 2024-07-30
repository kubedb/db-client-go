package zk

import "fmt"

// ringBuffer is a circular buffer that overwrites the oldest item when full.
// It is not thread-safe.
type ringBuffer[T any] struct {
	// The buffer.
	buf []T
	// The index of the first item in the buffer.
	head uint32
	// The index of the last item in the buffer.
	tail uint32
	// The number of items in the buffer.
	count uint32
	// The capacity of the buffer.
	capacity uint32
}

// newRingBuffer creates a new ringBuffer with the given capacity.
func newRingBuffer[T any](capacity uint32) *ringBuffer[T] {
	return &ringBuffer[T]{
		buf:      make([]T, capacity),
		capacity: capacity,
	}
}

func (rb *ringBuffer[T]) isEmpty() bool {
	return rb.count == 0
}

// len returns the number of items in the buffer.
func (rb *ringBuffer[T]) len() uint32 {
	return rb.count
}

// cap returns the capacity of the buffer.
func (rb *ringBuffer[T]) cap() uint32 {
	return rb.capacity
}

// offer adds an item to the buffer, if there is space.
// Returns true if the item was added, false otherwise (buffer full).
func (rb *ringBuffer[T]) offer(t T) bool {
	if rb.count == rb.capacity {
		return false
	}
	rb.buf[rb.tail] = t
	rb.tail = (rb.tail + 1) % rb.capacity
	rb.count++
	return true
}

// push adds an item to the buffer.
// If the buffer is full, the oldest item is overwritten.
func (rb *ringBuffer[T]) push(t T) {
	rb.buf[rb.tail] = t
	rb.tail = (rb.tail + 1) % rb.capacity
	if rb.count == rb.capacity {
		rb.head = rb.tail
	} else {
		rb.count++
	}
}

// peek returns the oldest item in the buffer, without removing it.
// If the buffer is empty, returns the zero value and false.
func (rb *ringBuffer[T]) peek() (T, bool) {
	if rb.count == 0 {
		var zero T
		return zero, false
	}
	return rb.buf[rb.head], true
}

// pop returns the oldest item in the buffer, removing it.
// If the buffer is empty, returns the zero value and false.
func (rb *ringBuffer[T]) pop() (T, bool) {
	if rb.count == 0 {
		var zero T
		return zero, false
	}
	t := rb.buf[rb.head]
	rb.head = (rb.head + 1) % rb.capacity
	rb.count--
	return t, true
}

// clear removes all items from the buffer.
func (rb *ringBuffer[T]) clear() {
	rb.head = 0
	rb.tail = 0
	rb.count = 0
}

// ensureCapacity increases the capacity of the buffer to at least the given capacity.
// If the buffer is already at least that large, this is a no-op.
func (rb *ringBuffer[T]) ensureCapacity(minCapacity uint32) {
	if minCapacity <= rb.capacity {
		return
	}
	newBuf := make([]T, minCapacity)
	if rb.count > 0 {
		if rb.head < rb.tail {
			copy(newBuf, rb.buf[rb.head:rb.tail])
		} else {
			n := copy(newBuf, rb.buf[rb.head:])
			copy(newBuf[n:], rb.buf[:rb.tail])
		}
	}
	rb.buf = newBuf
	rb.head = 0
	rb.tail = rb.count
	rb.capacity = minCapacity
}

// toSlice returns a slice of the items in the buffer (not aliased).
func (rb *ringBuffer[T]) toSlice() []T {
	if rb.count == 0 {
		return []T{}
	}
	out := make([]T, rb.count)
	if rb.tail > rb.head {
		copy(out, rb.buf[rb.head:rb.tail])
	} else {
		copy(out, rb.buf[rb.head:])
		copy(out[rb.capacity-rb.head:], rb.buf[:rb.tail])
	}
	return out
}

func (rb *ringBuffer[T]) String() string {
	return fmt.Sprintf("ringBuffer[%d/%d]", rb.count, rb.capacity)
}

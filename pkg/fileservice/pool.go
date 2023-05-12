// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"sync/atomic"
	_ "unsafe"
)

type Pool[T any] struct {
	initFunc    func(*T)
	finallyFunc func(*T)
	resetFunc   func(*T)
	pool        []_PoolElem[T]
	capacity    uint32
}

type _PoolElem[T any] struct {
	Taken atomic.Uint32
	Value T
}

// NewPool creates a new pool with the given capacity.
func NewPool[T any](
	capacity uint32,
	initFunc func(*T),
	resetFunc func(*T),
	finallyFunc func(*T),
) *Pool[T] {
	pool := &Pool[T]{
		capacity:    capacity,
		initFunc:    initFunc,
		finallyFunc: finallyFunc,
		resetFunc:   resetFunc,
		pool:        make([]_PoolElem[T], capacity),
	}

	for i := uint32(0); i < capacity; i++ {
		if initFunc != nil {
			initFunc(&pool.pool[i].Value)
		}
	}
	return pool
}

// Get returns a index and its value from the pool.
// If the pool is busy, return index -1 and a new value is created.
func (p *Pool[T]) Get() (int, *T) {
	// magic number 4.
	for i := 0; i < 4; i++ {
		idx := fastrand() % p.capacity
		if p.pool[idx].Taken.CompareAndSwap(0, 1) {
			return int(idx), &p.pool[idx].Value
		}
	}
	// If we can't get a value from the pool, we just create a new one.
	var value T
	if p.initFunc != nil {
		p.initFunc(&value)
	}
	return -1, &value
}

func (p *Pool[T]) Put(idx int, value *T) {
	if idx >= 0 {
		if p.resetFunc != nil {
			p.resetFunc(value)
		}

		if !p.pool[idx].Taken.CompareAndSwap(1, 0) {
			panic("bad put")
		}
	} else {
		if p.finallyFunc != nil {
			p.finallyFunc(value)
		}
	}
}

var bytesPoolDefaultBlockSize = NewPool(
	1024,
	func(t *[]byte) {
		*t = make([]byte, _DefaultBlockSize)
	},
	// XXX do we need to zero the slice?   Most likely not because this is buffer for io.  But note that
	// this is not golang semantics.
	nil,
	nil,
)

//go:linkname fastrand runtime.fastrand
func fastrand() uint32

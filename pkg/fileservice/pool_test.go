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
	"encoding/binary"
	"sync"
	"testing"
)

func TestBytesPool(t *testing.T) {
	pool := NewPool(8,
		func(v *[]byte) {
			*v = make([]byte, 8)
		},
		nil,
		nil)

	wg := new(sync.WaitGroup)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				poolidx, bs := pool.Get()
				defer pool.Put(poolidx, bs)
				binary.PutUvarint(*bs, uint64(i))
			}
		}()
	}
	wg.Wait()
}

func BenchmarkBytesPool(b *testing.B) {
	pool := NewPool(1024,
		func(v *[]byte) {
			*v = make([]byte, 8)
		},
		nil, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx, v := pool.Get()
		pool.Put(idx, v)
	}
}

func BenchmarkParallelBytesPool(b *testing.B) {
	pool := NewPool(1024,
		func(v *[]byte) {
			*v = make([]byte, 8)
		},
		nil, nil)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx, v := pool.Get()
			pool.Put(idx, v)
		}
	})
}

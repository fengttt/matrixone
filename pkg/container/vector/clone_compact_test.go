// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// A clone keeps only the payload its live rows reference: bytes behind a NULL
// row, or no longer referenced by any descriptor, are dropped, and a payload
// shared by several rows is copied once per row.
func TestCloneToFlatCompactDropsUnreferencedPayload(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	long := func(c string) []byte { return []byte(strings.Repeat(c, 40)) }

	src := NewVec(types.T_varchar.ToType(), mp)
	defer src.Free(mp)
	for _, c := range []string{"a", "b", "c", "d"} {
		require.NoError(t, AppendBytes(src, long(c), false, mp))
	}
	require.NoError(t, AppendBytes(src, []byte("inline"), false, mp))
	require.Len(t, src.GetArea(), 4*40)
	// Row 1 becomes NULL over its stale payload; row 3 shares row 0's payload,
	// so "d" is no longer referenced.
	src.SetNull(1)
	values := MustFixedColNoTypeCheck[types.Varlena](src)
	values[3] = values[0]

	dst, err := src.CloneToFlatCompact(mp)
	require.NoError(t, err)
	defer dst.Free(mp)
	require.False(t, dst.IsConst())
	require.Equal(t, src.Length(), dst.Length())
	require.Len(t, dst.GetArea(), 3*40, "live non-inline rows: a, c and a again")
	require.True(t, dst.VarlenaAreaIsDisjoint())
	for row := range src.Length() {
		require.Equal(t, src.IsNull(uint64(row)), dst.IsNull(uint64(row)))
		if !src.IsNull(uint64(row)) {
			require.Equal(t, src.GetBytesAt(row), dst.GetBytesAt(row))
		}
	}
}

// When the payload allocation is refused, the clone frees what it already
// allocated (descriptors and null bitmap) and charges nothing.
func TestCloneToFlatCompactFreesPartialAllocation(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	src := NewVec(types.T_varchar.ToType(), mp)
	defer src.Free(mp)
	for range 8 {
		require.NoError(t, AppendBytes(src, make([]byte, 64<<10), false, mp))
	}
	require.NoError(t, AppendBytes(src, nil, true, mp))

	// The budget admits the descriptors and bitmap but not the payload.
	state := newTestVectorAllocationAccount(t, 64<<10, 16)
	t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, state) })
	got, err := src.CloneToFlatCompact(state.view(t, mp))
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Nil(t, got)
	require.Zero(t, state.account.Snapshot().Used)
}

func TestCloneToFlatCompactExpandsConst(t *testing.T) {
	mp := mpool.MustNewZero()
	tests := []struct {
		name       string
		newSource  func(t *testing.T) *Vector
		wantNull   bool
		checkValue func(t *testing.T, vec *Vector)
	}{
		{
			name: "fixed-value",
			newSource: func(t *testing.T) *Vector {
				vec, err := NewConstFixed(types.T_int8.ToType(), int8(7), 3, mp)
				require.NoError(t, err)
				return vec
			},
			checkValue: func(t *testing.T, vec *Vector) {
				for row := range 3 {
					require.Equal(t, int8(7), GetFixedAtNoTypeCheck[int8](vec, row))
				}
			},
		},
		{
			name: "varlen-value",
			newSource: func(t *testing.T) *Vector {
				vec, err := NewConstBytes(types.T_varchar.ToType(), []byte("rollup"), 3, mp)
				require.NoError(t, err)
				return vec
			},
			checkValue: func(t *testing.T, vec *Vector) {
				for row := range 3 {
					require.Equal(t, "rollup", string(vec.GetBytesAt(row)))
				}
			},
		},
		{
			name: "fixed-const-null",
			newSource: func(t *testing.T) *Vector {
				return NewConstNull(types.T_int8.ToType(), 3, mp)
			},
			wantNull: true,
		},
		{
			name: "varlen-const-null",
			newSource: func(t *testing.T) *Vector {
				return NewConstNull(types.T_varchar.ToType(), 3, mp)
			},
			wantNull: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			src := test.newSource(t)
			dst, err := src.CloneToFlatCompact(mp)
			require.NoError(t, err)
			require.False(t, dst.IsConst())
			require.Equal(t, 3, dst.Length())
			for row := range 3 {
				require.Equal(t, test.wantNull, dst.GetNulls().Contains(uint64(row)))
			}
			if test.checkValue != nil {
				test.checkValue(t, dst)
			}

			src.Free(mp)
			dst.Free(mp)
			require.Equal(t, int64(0), mp.CurrNB())
		})
	}
}

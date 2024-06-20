// Copyright 2021 Matrix Origin
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

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func checkPackUnpack64(t *testing.T, s, exps string) {
	var unp UnpackDec64
	err := unp.Parse(s)
	if exps == "" {
		require.Error(t, err, s)
	} else {
		require.NoError(t, err, s)
	}

	d := unp.Pack()
	err = unp.Unpack(d)
	require.NoError(t, err)

	ss := unp.ToString()
	require.Equal(t, ss, exps)
}

func checkPackUnpack128(t *testing.T, s, exps string) {
	var unp UnpackDec128
	err := unp.Parse(s)
	if exps == "" {
		require.Error(t, err, s)
	} else {
		require.NoError(t, err, s)
	}

	d := unp.Pack()
	err = unp.Unpack(d)
	require.NoError(t, err)

	ss := unp.ToString()
	require.Equal(t, ss, exps)
}

func TestPackUnpack(t *testing.T) {
	checkPackUnpack64(t, "0", "0")
	checkPackUnpack128(t, "0", "0")
	checkPackUnpack64(t, "0.00", "0")
	checkPackUnpack128(t, "0.00", "0")

	checkPackUnpack64(t, "1", "1")
	checkPackUnpack128(t, "1", "1")
	checkPackUnpack64(t, "1.00", "1")
	checkPackUnpack128(t, "1.00", "1")

	checkPackUnpack64(t, "-0", "0")
	checkPackUnpack128(t, "-0", "0")
	checkPackUnpack64(t, "-0.00", "0")
	checkPackUnpack128(t, "-0.00", "0")

	checkPackUnpack64(t, "-1", "-1")
	checkPackUnpack128(t, "-1", "-1")
	checkPackUnpack64(t, "-1.00", "-1")
	checkPackUnpack128(t, "-1.00", "-1")

	checkPackUnpack64(t, "10.02", "10.02")
	checkPackUnpack128(t, "10.02", "10.02")
	checkPackUnpack64(t, "10.0200", "10.02")
	checkPackUnpack128(t, "10.0200", "10.02")

	checkPackUnpack64(t, "-10.02", "-10.02")
	checkPackUnpack128(t, "-10.02", "-10.02")
	checkPackUnpack64(t, "-10.0200", "-10.02")
	checkPackUnpack128(t, "-10.0200", "-10.02")

	checkPackUnpack64(t, "123.456789012345", "123.456789012345")
	checkPackUnpack128(t, "123.456789012345", "123.456789012345")
	checkPackUnpack64(t, "123.456789012345", "123.456789012345")
	checkPackUnpack128(t, "123.456789012345", "123.456789012345")
}

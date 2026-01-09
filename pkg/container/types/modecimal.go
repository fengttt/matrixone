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
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

//
// Mo's own dec64/dec128.  Only support +, -, *, /, %, and round.
// MoDec64 has precision of 17 decimal digits.
// MoDec128 has precision of 36 decimal digits.
//
// Binary format for dec64, from most significant bit to least:
// 	1 bit	: sign bit, 0 for neg and 1 for pos.
// 	5 bits	: number of integral dec digits.
// 	intp    : bits to store integral part, see unpack below.
//  fracp   : fraction part, see unpack below.
// if the number if neg (bit 1 is 0), the rest of the bits are reversed.
//
// Example:  12345.6789012345
// 	1 bit 	: 1           -- pos
//  5 bits 	: 5           -- 7 decimal digits
//  17 bits : 12345       -- Need 17 bits to store 5 decimal digits
//  30 bits : 678,901,234 -- 30 bits to store first 9 decimal digits after dec pt.
//  10 bits : 500         -- 10 bits to store 3 decimal digits, note that 5 is stored as 500.
//  1 bit   : 0           -- unused.
//
// Another example: -1.02
//  1 bit   : 0           -- neg
//  5 bits  : 1           -- 1 int part
//  4 bits  : 1           -- 4 bits to store 1 dec digits
//  30 bits : 020,000,000 -- 30 bits to store 9 dec digits, note the diff between 1.02 and 1.2
//  24 bits : 0           -- the rest of fracs.
// then all bits other than the leading sign bit is reversed.
//
// Binary format for dec128 is the same as dec64 except we use 6 bits for number of integral
// decimal digits.
//
// The format is designed with the following in mind,
// 0. decimal to binary is an injection (no cohort), it is not a bijection means some bit
//    patterns are not valid modec 64/128.
// 1. binary format is precision/scale independent.
// 2. you can compare two decimals with memcmp.  If we are on big endian machines, we
//    actually can do uint64 comparison.  Unfortuately x64 and arm64 default to little endian
//    and it is not clear if a ntohl will pay off.
// 3. the above which also means, you can hash/partition etc, treating dec64 just as bytes.
//

const (
	DEC64_NDIGITS  = 17
	DEC64_STR_LEN  = 20 // 17 digits, plus +/-, and . so 20 bytes can hold any dec64.
	DEC128_NDIGITS = 36
	DEC128_STR_LEN = 40 // 40 bytes can hold any dec128

	// Base 1 billion
	GIGA_BASE         = 1000000000
	GIGA_BASE_NDIGITS = 9
	GIGA_BASE_NBITS   = 30
)

// bits needed to encode 0-9 decimal digits.  Good enough for GIGA_BASE.
var bits_needed = [...]uint8{0, 4, 7, 10, 14, 17, 20, 24, 27, 30}

// bit mask of tailing 1s of a byte, in uint32 type.
var u32Masks = [...]uint32{0xFF, 0x7F, 0x3F, 0x1F, 0x0F, 0x07, 0x03, 0x01}

// scale u32 by pow of 10.
func scaleBy(u32 uint32, n uint8) uint32 {
	switch n {
	case 9:
		return u32 * 1000000000
	case 8:
		return u32 * 100000000
	case 7:
		return u32 * 10000000
	case 6:
		return u32 * 1000000
	case 5:
		return u32 * 100000
	case 4:
		return u32 * 10000
	case 3:
		return u32 * 1000
	case 2:
		return u32 * 100
	case 1:
		return u32 * 10
	case 0:
		return u32
	}
	panic("bad u32 scale")
}

func scaleDownBy(u32 uint32, n uint8) uint32 {
	switch n {
	case 9:
		return u32 / 1000000000
	case 8:
		return u32 / 100000000
	case 7:
		return u32 / 10000000
	case 6:
		return u32 / 1000000
	case 5:
		return u32 / 100000
	case 4:
		return u32 / 10000
	case 3:
		return u32 / 1000
	case 2:
		return u32 / 100
	case 1:
		return u32 / 10
	case 0:
		return u32
	}
	panic("bad u32 scale")
}

// number of dec digits of u32
func u32Ndigits(u32 uint32) uint8 {
	switch {
	case u32 == 0:
		return 0
	case u32 < 10:
		return 1
	case u32 < 100:
		return 2
	case u32 < 1000:
		return 3
	case u32 < 10000:
		return 4
	case u32 < 100000:
		return 5
	case u32 < 1000000:
		return 6
	case u32 < 10000000:
		return 7
	case u32 < 100000000:
		return 8
	// case u32 < 1000000000:
	default:
		return 9
	}
}

// Unpack dec64 to a struct for easy arithmatics.
type UnpackDec64 struct {
	sign   int8      // sign bit
	digits [4]uint32 // 4 uint32, first 2 are for intpart, last 2 are for frac.
}

type UnpackDec128 struct {
	sign   int8      // sign bit
	digits [8]uint32 // 4 uint32, first 4 are for intpart, last 4 are for frac.
}

func flipBits64(d64 *Decimal64) {
	up := (*uint64)(unsafe.Pointer(d64))
	*up = ^*up
}

func flipBits128(d128 *Decimal128) {
	ptr := unsafe.Pointer(d128)
	up := (*uint64)(ptr)
	*up = ^*up
	ptr2 := unsafe.Add(ptr, 8)
	up2 := (*uint64)(ptr2)
	*up2 = ^*up2
}

func dec64ToBytes(d64 *Decimal64) []byte {
	ptr := (*byte)(unsafe.Pointer(d64))
	return unsafe.Slice(ptr, 8)
}

func dec128ToBytes(d128 *Decimal128) []byte {
	ptr := (*byte)(unsafe.Pointer(d128))
	return unsafe.Slice(ptr, 16)
}

func decodeGigaBase(p []byte, bitPos, nBits uint8) uint32 {
	currByte := bitPos >> 3
	currBit := bitPos & 7

	var ret uint32

	for nBits > 0 {
		// current byte has this many bits
		hasBits := 8 - currBit
		if nBits <= hasBits {
			u32 := (uint32(p[currByte]) & u32Masks[currBit]) >> (hasBits - nBits)
			return ret | u32
		} else {
			u32 := (uint32(p[currByte]) & u32Masks[currBit])
			ret |= u32
			ret <<= hasBits
			currByte += 1
			currBit = 0
			nBits -= hasBits
		}
	}
	return ret
}

func encodeGigaBase(bs []byte, bitPos uint8, nBits uint8, u32 uint32) {
	if u32 == 0 {
		return
	}

	currByte := bitPos >> 3
	currBit := bitPos & 7
	for nBits > 0 {
		hasBits := 8 - currBit
		if nBits <= hasBits {
			bs[currByte] |= uint8((u32 & u32Masks[nBits]) << (hasBits - nBits))
			return
		} else {
			bs[currByte] |= uint8(u32 >> (nBits - hasBits))
			currByte += 1
			currBit = 0
			nBits -= hasBits
			u32 &= (1 << nBits) - 1
		}
	}
}

func (d Decimal64) IsNegative() bool {
	return (d[0] & 0x80) == 0
}
func (d Decimal128) IsNegative() bool {
	return (d[0] & 0x80) == 0
}

func (unp *UnpackDec64) Unpack(d64 Decimal64) error {
	if d64.IsNegative() {
		unp.sign = -1
		// d64 call by value
		flipBits64(&d64)
	} else {
		unp.sign = 1
	}
	// sign bit extracted, and neg has been flipped

	// start decoding. first, number of integral dec digits. bit 1-5
	nintp := (d64[0] & 0x7F) >> 2
	if nintp > DEC64_NDIGITS {
		return moerr.NewInvalidArg("dec64 precision", nintp)
	}
	nfrac := DEC64_NDIGITS - nintp

	// Decoding position, currently we are at the bit 6.
	bitPos := uint8(6)

	// for easier access
	bs := dec64ToBytes(&d64)

	// decode intp
	if nintp > GIGA_BASE_NDIGITS {
		// decode first
		nb := bits_needed[nintp-GIGA_BASE_NDIGITS]
		unp.digits[0] = decodeGigaBase(bs, bitPos, nb)
		bitPos += nb

		unp.digits[1] = decodeGigaBase(bs, bitPos, GIGA_BASE_NBITS)
		bitPos += GIGA_BASE_NBITS
	} else {
		unp.digits[0] = 0
		nb := bits_needed[nintp]
		unp.digits[1] = decodeGigaBase(bs, bitPos, nb)
		bitPos += nb
	}

	// decode fracp
	if nfrac > GIGA_BASE_NDIGITS {
		unp.digits[2] = decodeGigaBase(bs, bitPos, GIGA_BASE_NBITS)
		bitPos += GIGA_BASE_NBITS
		nfrac -= GIGA_BASE_NDIGITS
		nb := bits_needed[nfrac]
		unp.digits[3] = decodeGigaBase(bs, bitPos, nb)
		if unp.digits[3] != 0 {
			// scale
			unp.digits[3] = scaleBy(unp.digits[3], GIGA_BASE_NDIGITS-nfrac)
		}
	} else {
		nb := bits_needed[nfrac]
		unp.digits[2] = decodeGigaBase(bs, bitPos, nb)
		if unp.digits[2] != 0 {
			// scale
			unp.digits[2] = scaleBy(unp.digits[2], GIGA_BASE_NDIGITS-nfrac)
		}
		unp.digits[3] = 0
	}
	return nil
}

func (unp *UnpackDec64) Pack() Decimal64 {
	var d64 Decimal64
	bs := dec64ToBytes(&d64)

	bitpos := uint8(6)
	var nintp, nfrac uint8

	// encode intp
	if unp.digits[0] > 0 {
		nd := u32Ndigits(unp.digits[0])
		nb := bits_needed[nd]
		encodeGigaBase(bs, bitpos, nb, unp.digits[0])
		bitpos += nb
		encodeGigaBase(bs, bitpos, GIGA_BASE_NBITS, unp.digits[1])
		bitpos += GIGA_BASE_NBITS
		nintp = nd + GIGA_BASE_NDIGITS
		nfrac = DEC64_NDIGITS - nintp
	} else {
		nd := u32Ndigits(unp.digits[1])
		nb := bits_needed[nd]
		encodeGigaBase(bs, bitpos, nb, unp.digits[1])
		bitpos += nb
		nintp = nd
		nfrac = DEC64_NDIGITS - nintp
	}
	// nintp computed, put it inplace.
	bs[0] |= (nintp << 2)

	// next encode fraction part.
	if nfrac > GIGA_BASE_NDIGITS {
		encodeGigaBase(bs, bitpos, GIGA_BASE_NBITS, unp.digits[2])
		bitpos += GIGA_BASE_NBITS
		if unp.digits[3] != 0 {
			frac2 := nfrac - GIGA_BASE_NDIGITS
			u32 := scaleDownBy(unp.digits[3], GIGA_BASE_NDIGITS-frac2)
			encodeGigaBase(bs, bitpos, bits_needed[frac2], u32)
		}
	} else {
		if unp.digits[2] != 0 {
			u32 := scaleDownBy(unp.digits[2], GIGA_BASE_NDIGITS-nfrac)
			encodeGigaBase(bs, bitpos, bits_needed[nfrac], u32)
		}
	}

	// sign bit
	bs[0] |= 0x80
	if unp.sign < 0 {
		// sign bit is fliped too
		flipBits64(&d64)
	}
	return d64
}

func (unp *UnpackDec128) Unpack(d128 Decimal128) error {
	if d128.IsNegative() {
		unp.sign = -1
		flipBits128(&d128)
	} else {
		unp.sign = 1
	}
	// sign bit extracted, and neg has been flipped

	// start decoding. first, number of integral dec digits. bit 1-6
	nintp := (d128[0] & 0x7F) >> 1
	if nintp > DEC128_NDIGITS {
		return moerr.NewInvalidArg("dec128 precision", nintp)
	}
	nfrac := DEC128_NDIGITS - nintp

	// Decoding position, currently we are at the bit 7.
	bitPos := uint8(7)

	// for easier access
	bs := dec128ToBytes(&d128)

	// decode intp, digits[0]
	if nintp > GIGA_BASE_NDIGITS*3 {
		nb := bits_needed[nintp-GIGA_BASE_NDIGITS*3]
		unp.digits[0] = decodeGigaBase(bs, bitPos, nb)
		bitPos += nb
		nintp = GIGA_BASE_NDIGITS * 3
	}

	// decode intp, digits[1]
	if nintp > GIGA_BASE_NDIGITS*2 {
		nb := bits_needed[nintp-GIGA_BASE_NDIGITS*2]
		unp.digits[1] = decodeGigaBase(bs, bitPos, nb)
		bitPos += nb
		nintp = GIGA_BASE_NDIGITS * 2
	}

	// decode intp, digits[2]
	if nintp > GIGA_BASE_NDIGITS {
		nb := bits_needed[nintp-GIGA_BASE_NDIGITS]
		unp.digits[2] = decodeGigaBase(bs, bitPos, nb)
		bitPos += nb
		nintp = GIGA_BASE_NDIGITS
	}

	// decode digits[3]
	nb := bits_needed[nintp]
	unp.digits[3] = decodeGigaBase(bs, bitPos, nb)
	bitPos += nb

	// decode fracp
	digIdx := 4
	for nfrac > GIGA_BASE_NDIGITS {
		unp.digits[digIdx] = decodeGigaBase(bs, bitPos, GIGA_BASE_NBITS)
		bitPos += GIGA_BASE_NBITS
		nfrac -= GIGA_BASE_NDIGITS
		digIdx++
	}
	// last frac digit
	nb = bits_needed[nfrac]
	unp.digits[digIdx] = decodeGigaBase(bs, bitPos, nb)
	if unp.digits[digIdx] != 0 {
		// scale.
		unp.digits[digIdx] = scaleBy(unp.digits[digIdx], GIGA_BASE_NDIGITS-nfrac)

	}
	return nil
}

func (unp *UnpackDec128) Pack() Decimal128 {
	var d128 Decimal128
	bs := dec128ToBytes(&d128)

	bitpos := uint8(7)
	var nintp, nfrac uint8

	// encode intp
	for digIdx := 0; digIdx < 4; digIdx++ {
		if nintp > 0 || unp.digits[digIdx] > 0 {
			nd := uint8(GIGA_BASE_NDIGITS)
			if nintp == 0 {
				nd = u32Ndigits(unp.digits[1])
			}
			nb := bits_needed[nd]
			encodeGigaBase(bs, bitpos, nb, unp.digits[digIdx])
			bitpos += nb
			nintp += nd
		}
	}

	// nintp computed, put it inplace.
	bs[0] |= (nintp << 1)

	// next encode fraction part.
	for digIdx := 4; digIdx < 8; digIdx++ {
		if nfrac > GIGA_BASE_NDIGITS {
			encodeGigaBase(bs, bitpos, GIGA_BASE_NBITS, unp.digits[digIdx])
			bitpos += GIGA_BASE_NBITS
			nfrac -= GIGA_BASE_NDIGITS
		} else {
			if unp.digits[digIdx] != 0 {
				u32 := scaleDownBy(unp.digits[3], GIGA_BASE_NDIGITS-nfrac)
				encodeGigaBase(bs, bitpos, bits_needed[nfrac], u32)
			}
			// break out of the loop
			break
		}
	}

	// sign bit
	bs[0] |= 0x80
	if unp.sign < 0 {
		// sign bit is fliped too
		flipBits128(&d128)
	}
	return d128
}

func parseDecString(s string) (iBegin, iEnd, fBegin, fEnd int, sign int8, err error) {
	if s == "" {
		err = moerr.NewInvalidArg("dec64", s)
		return
	}

	// Assume no sign
	sign = 1
	if s[0] == '+' {
		iBegin = 1
	} else if s[0] == '-' {
		sign = -1
		iBegin = 1
	}

	// next intp
	for iEnd = iBegin; iEnd < len(s); iEnd++ {
		if s[iEnd] >= '0' && s[iEnd] <= '9' {
			continue
		} else if s[iEnd] == '.' {
			break
		} else {
			err = moerr.NewInvalidArg("dec64", s)
			return
		}
	}

	fBegin = iEnd + 1
	for fEnd = fBegin; fEnd < len(s); fEnd++ {
		if s[fEnd] >= '0' && s[fEnd] <= '9' {
			continue
		} else {
			err = moerr.NewInvalidArg("dec64", s)
			return
		}
	}
	return
}

func parseIntPart(p *uint32, s string, iBegin, iEnd int) error {
	for iEnd > iBegin {
		nChar := iEnd - iBegin
		if nChar > 9 {
			nChar = 9
		}
		i, err := strconv.Atoi(s[iEnd-nChar : iEnd])
		if err != nil {
			return moerr.NewInvalidArg("parse decimal number", s)
		}
		*p = uint32(i)
		p = (*uint32)(unsafe.Add(unsafe.Pointer(p), -4))
		iEnd -= nChar
	}
	return nil
}

func parseFracPart(p *uint32, s string, fBegin, fEnd int) error {
	for fBegin < fEnd {
		nChar := fEnd - fBegin
		if nChar > 9 {
			nChar = 9
		}
		i, err := strconv.Atoi(s[fBegin : fBegin+nChar])
		if err != nil {
			return moerr.NewInvalidArg("parse decimal number", s)
		}
		*p = uint32(i)
		p = (*uint32)(unsafe.Add(unsafe.Pointer(p), 4))
		fBegin += nChar
	}
	return nil
}

func (unp *UnpackDec64) Parse(s string) error {
	iBegin, iEnd, fBegin, fEnd, sign, err := parseDecString(s)
	if err != nil {
		return err
	}

	unp.sign = sign
	if (iEnd-iBegin)+(fEnd-fBegin) > DEC64_NDIGITS {
		return moerr.NewOutOfRange("decimal64", "%s", s)
	}

	if err = parseIntPart(&unp.digits[1], s, iBegin, iEnd); err != nil {
		return err
	}

	if err = parseFracPart(&unp.digits[2], s, fBegin, fEnd); err != nil {
		return err
	}
	return nil
}

func (unp *UnpackDec128) Parse(s string) error {
	iBegin, iEnd, fBegin, fEnd, sign, err := parseDecString(s)
	if err != nil {
		return err
	}

	unp.sign = sign
	if (iEnd-iBegin)+(fEnd-fBegin) > DEC128_NDIGITS {
		return moerr.NewOutOfRange("decimal128", "%s", s)
	}

	if err = parseIntPart(&unp.digits[3], s, iBegin, iEnd); err != nil {
		return err
	}

	if err = parseFracPart(&unp.digits[4], s, fBegin, fEnd); err != nil {
		return err
	}
	return nil
}

func (unp *UnpackDec64) ToString() string {
	is := fmt.Sprintf("%09d%09d", unp.digits[0], unp.digits[1])
	is = strings.TrimLeft(is, "0")
	if is == "" {
		is = "0"
	}
	fs := fmt.Sprintf("%09d%09d", unp.digits[2], unp.digits[3])
	fs = strings.TrimRight(fs, "0")
	if fs != "" {
		fs = "." + fs
	}

	ss := ""
	if unp.sign == -1 {
		ss = "-"
	}
	return ss + is + fs
}

func (unp *UnpackDec128) ToString() string {
	is := fmt.Sprintf("%09d%09d%09d%09d", unp.digits[0], unp.digits[1], unp.digits[2], unp.digits[3])
	is = strings.TrimLeft(is, "0")
	if is == "" {
		is = "0"
	}
	fs := fmt.Sprintf("%09d%09d%09d%09d", unp.digits[4], unp.digits[5], unp.digits[6], unp.digits[7])
	fs = strings.TrimRight(fs, "0")
	if fs != "" {
		fs = "." + fs
	}

	ss := ""
	if unp.sign == -1 {
		ss = "-"
	}
	return ss + is + fs
}

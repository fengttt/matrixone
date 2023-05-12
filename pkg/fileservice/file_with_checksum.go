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
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
)

// FileWithChecksum maps file contents to blocks with checksum
type FileWithChecksum[T FileLike] struct {
	ctx              context.Context
	underlying       T
	blockSize        int
	blockContentSize int
	contentOffset    int64
	perfCounterSets  []*perfcounter.CounterSet
}

const (
	_ChecksumSize     = crc32.Size
	_DefaultBlockSize = 2048
	_BlockContentSize = _DefaultBlockSize - _ChecksumSize
	_BlockSize        = _BlockContentSize + _ChecksumSize
)

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)

	ErrChecksumNotMatch = moerr.NewInternalErrorNoCtx("checksum not match")
)

func NewFileWithChecksum[T FileLike](
	ctx context.Context,
	underlying T,
	blockContentSize int,
	perfCounterSets []*perfcounter.CounterSet,
) *FileWithChecksum[T] {
	return &FileWithChecksum[T]{
		ctx:              ctx,
		underlying:       underlying,
		blockSize:        blockContentSize + _ChecksumSize,
		blockContentSize: blockContentSize,
		perfCounterSets:  perfCounterSets,
	}
}

func NewFileWithChecksumOSFile(
	ctx context.Context,
	underlying *os.File,
	blockContentSize int,
	perfCounterSets []*perfcounter.CounterSet,
) (int, *FileWithChecksum[*os.File]) {
	idx, f := fileWithChecksumPoolOSFile.Get()
	(*f).ctx = ctx
	(*f).underlying = underlying
	(*f).blockSize = blockContentSize + _ChecksumSize
	(*f).blockContentSize = blockContentSize
	(*f).perfCounterSets = perfCounterSets
	return idx, f
}

func ReleaseFileWithChecksumOSFile(idx int, f *FileWithChecksum[*os.File]) {
	fileWithChecksumPoolOSFile.Put(idx, f)
}

var fileWithChecksumPoolOSFile = NewPool(
	1024,
	nil, // init
	func(f *FileWithChecksum[*os.File]) {
		*f = emptyFileWithChecksumOSFile
	}, // reset
	nil,
)

var emptyFileWithChecksumOSFile FileWithChecksum[*os.File]

var _ FileLike = new(FileWithChecksum[*os.File])

func (f *FileWithChecksum[T]) ReadAt(buf []byte, offset int64) (n int, err error) {
	defer func() {
		perfcounter.Update(f.ctx, func(c *perfcounter.CounterSet) {
			c.FileService.FileWithChecksum.Read.Add(int64(n))
		}, f.perfCounterSets...)
	}()

	for len(buf) > 0 {
		var poolIdx int
		var pdata *[]byte
		var retdata []byte
		blockOffset, offsetInBlock := f.contentOffsetToBlockOffset(offset)
		poolIdx, pdata, retdata, err = f.readBlock(blockOffset)
		defer f.releaseBlock(poolIdx, pdata)

		if err != nil && err != io.EOF {
			// read error
			return
		}
		retdata = retdata[offsetInBlock:]
		nBytes := copy(buf, *pdata)
		buf = buf[nBytes:]
		if err == io.EOF && nBytes != len(*pdata) {
			// not fully read
			err = nil
		}
		offset += int64(nBytes)
		n += nBytes
		if err == io.EOF && nBytes == 0 {
			// no more data
			break
		}
	}
	return
}

func (f *FileWithChecksum[T]) Read(buf []byte) (n int, err error) {
	n, err = f.ReadAt(buf, f.contentOffset)
	f.contentOffset += int64(n)
	return
}

func (f *FileWithChecksum[T]) WriteAt(buf []byte, offset int64) (n int, err error) {
	defer func() {
		perfcounter.Update(f.ctx, func(c *perfcounter.CounterSet) {
			c.FileService.FileWithChecksum.Write.Add(int64(n))
		}, f.perfCounterSets...)
	}()

	for len(buf) > 0 {
		blockOffset, offsetInBlock := f.contentOffsetToBlockOffset(offset)
		poolIdx, pdata, retdata, err := f.readBlock(blockOffset)
		defer f.releaseBlock(poolIdx, pdata)

		if err != nil && err != io.EOF {
			return 0, err
		}

		// extend data
		if len((*pdata)[offsetInBlock:]) == 0 {
			nAppend := len(buf)
			if nAppend+len(*pdata) > f.blockContentSize {
				nAppend = f.blockContentSize - len(*pdata)
			}
			retdata = append(retdata, make([]byte, nAppend)...)
		}

		// copy to data
		nBytes := copy((*pdata)[offsetInBlock:], buf)
		buf = buf[nBytes:]

		checksum := crc32.Checksum(*pdata, crcTable)
		checksumBytes := make([]byte, _ChecksumSize)
		binary.LittleEndian.PutUint32(checksumBytes, checksum)
		if n, err := f.underlying.WriteAt(checksumBytes, blockOffset); err != nil {
			return n, err
		} else {
			perfcounter.Update(f.ctx, func(c *perfcounter.CounterSet) {
				c.FileService.FileWithChecksum.UnderlyingWrite.Add(int64(n))
			}, f.perfCounterSets...)
		}

		if n, err := f.underlying.WriteAt(*pdata, blockOffset+_ChecksumSize); err != nil {
			return n, err
		} else {
			perfcounter.Update(f.ctx, func(c *perfcounter.CounterSet) {
				c.FileService.FileWithChecksum.UnderlyingWrite.Add(int64(n))
			}, f.perfCounterSets...)
		}

		n += nBytes
		offset += int64(nBytes)
	}

	return
}

func (f *FileWithChecksum[T]) Write(buf []byte) (n int, err error) {
	n, err = f.WriteAt(buf, f.contentOffset)
	f.contentOffset += int64(n)
	return
}

func (f *FileWithChecksum[T]) Seek(offset int64, whence int) (int64, error) {

	fileSize, err := f.underlying.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	nBlock := ceilingDiv(fileSize, int64(f.blockSize))
	contentSize := fileSize - _ChecksumSize*nBlock

	switch whence {
	case io.SeekStart:
		f.contentOffset = offset
	case io.SeekCurrent:
		f.contentOffset += offset
	case io.SeekEnd:
		f.contentOffset = contentSize + offset
	}

	if f.contentOffset < 0 {
		f.contentOffset = 0
	}
	if f.contentOffset > contentSize {
		f.contentOffset = contentSize
	}

	return f.contentOffset, nil
}

func (f *FileWithChecksum[T]) contentOffsetToBlockOffset(
	contentOffset int64,
) (
	blockOffset int64,
	offsetInBlock int64,
) {

	nBlock := contentOffset / int64(f.blockContentSize)
	blockOffset += nBlock * int64(f.blockSize)

	offsetInBlock = contentOffset % int64(f.blockContentSize)

	return
}

func (f *FileWithChecksum[T]) readBlock(offset int64) (int, *[]byte, []byte, error) {
	var poolIdx int
	var pdata *[]byte
	// _DefaultBlockSize is the most common block size, so we use it as the default pool
	// non default block size will just be allocated.   pool Put can handle diff size.
	if f.blockSize == _DefaultBlockSize {
		poolIdx, pdata = bytesPoolDefaultBlockSize.Get()
	} else {
		poolIdx = -1
		data := make([]byte, f.blockSize)
		pdata = &data
	}

	retdata := *pdata

	n, err := f.underlying.ReadAt(retdata, offset)
	retdata = retdata[:n]
	if err != nil && err != io.EOF {
		bytesPoolDefaultBlockSize.Put(poolIdx, pdata)
		return -1, nil, nil, err
	}
	perfcounter.Update(f.ctx, func(c *perfcounter.CounterSet) {
		c.FileService.FileWithChecksum.UnderlyingRead.Add(int64(n))
	}, f.perfCounterSets...)

	if n < _ChecksumSize {
		// empty
		return poolIdx, pdata, retdata, nil
	}

	checksum := binary.LittleEndian.Uint32(retdata[:_ChecksumSize])
	retdata = retdata[_ChecksumSize:]

	expectedChecksum := crc32.Checksum(retdata, crcTable)
	if checksum != expectedChecksum {
		bytesPoolDefaultBlockSize.Put(poolIdx, pdata)
		return -1, nil, nil, ErrChecksumNotMatch
	}

	return poolIdx, pdata, retdata, nil
}

func (f *FileWithChecksum[T]) releaseBlock(poolIdx int, p *[]byte) {
	if p != nil {
		bytesPoolDefaultBlockSize.Put(poolIdx, p)
	}
}

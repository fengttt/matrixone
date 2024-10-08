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

package table

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

const DefaultWriterBufferSize = 10 * mpool.MB

type MergeLogType string

func (t MergeLogType) String() string { return string(t) }

const MergeLogTypeMerged MergeLogType = "merged"
const MergeLogTypeLogs MergeLogType = "logs"
const MergeLogTypeALL MergeLogType = "*"

const FilenameSeparator = "_"
const CsvExtension = ".csv"
const TaeExtension = ".tae"
const FSExtension = ".fs"

const ETLParamTypeAll = MergeLogTypeALL
const ETLParamAccountAll = "*"

const AccountAll = ETLParamAccountAll
const AccountSys = "sys"

var ETLParamTSAll = time.Time{}

// PathBuilder hold strategy to build filepath
type PathBuilder interface {
	// Build directory path
	Build(account string, typ MergeLogType, ts time.Time, db string, name string) string
	// BuildETLPath return path for EXTERNAL table 'infile' options
	//
	// like: {account}/merged/*/*/*/{name}/*.csv
	BuildETLPath(db, name, account string) string
	// ParsePath
	//
	// switch path {
	// case "{timestamp_writedown}_{node_uuid}_{ndoe_type}.csv":
	// case "{timestamp_start}_{timestamp_end}_merged.csv"
	// }
	ParsePath(ctx context.Context, path string) (Path, error)
	NewMergeFilename(timestampStart, timestampEnd, extension string) string
	NewLogFilename(name, nodeUUID, nodeType string, ts time.Time, extension string) string
	// SupportMergeSplit const. if false, not support SCV merge|split task
	SupportMergeSplit() bool
	// SupportAccountStrategy const
	SupportAccountStrategy() bool
	// GetName const
	GetName() string
}

type Path interface {
	Table() string
	Timestamp() []string
}

var _ Path = (*ETLPath)(nil)

type ETLPath struct {
	// path raw data
	path string
	// table parsed from path
	table string
	// filename
	filename string
	// timestamps parsed from filename in path
	timestamps []string
	// fileType, val in [log, merged]
	fileType MergeLogType
}

const PathElems = 7
const PathIdxFilename = 6
const PathIdxTable = 5
const PathIdxAccount = 0
const FilenameElems = 3
const FilenameElemsV2 = 4
const FilenameIdxType = 2

// NewETLPath
//
// path like: sys/[log|merged]/yyyy/mm/dd/table/***.csv
// ##    idx: 0   1            2    3  4  5     6
// filename like: {timestamp}_{node_uuid}_{node_type}.csv
// ##         or: {timestamp_start}_{timestamp_end}_merged.csv
func NewETLPath(path string) *ETLPath {
	return &ETLPath{path: path}
}

func (p *ETLPath) Parse(ctx context.Context) error {
	// parse path => filename, table
	elems := strings.Split(p.path, "/")
	if len(elems) != PathElems {
		return moerr.NewInternalErrorf(ctx, "invalid etl path: %s", p.path)
	}
	p.filename = elems[PathIdxFilename]
	p.table = elems[PathIdxTable]

	// parse filename => fileType, timestamps
	filename := strings.Trim(p.filename, CsvExtension)
	fnElems := strings.Split(filename, FilenameSeparator)
	if len(fnElems) != FilenameElems && len(fnElems) != FilenameElemsV2 {
		return moerr.NewInternalErrorf(ctx, "invalid etl filename: %s", p.filename)
	}
	if fnElems[FilenameIdxType] == string(MergeLogTypeMerged) {
		p.fileType = MergeLogTypeMerged
		p.timestamps = fnElems[:2]
	} else {
		p.fileType = MergeLogTypeLogs
		p.timestamps = fnElems[:1]
	}

	return nil
}

func (p *ETLPath) Table() string {
	return p.table
}

func (p *ETLPath) Timestamp() []string {
	return p.timestamps
}

type PathBuilderConfig struct {
	withDatabase bool
}

type PathBuilderOption func(*PathBuilderConfig)

func (opt PathBuilderOption) Apply(cfg *PathBuilderConfig) {
	opt(cfg)
}

func WithDatabase(with bool) PathBuilderOption {
	return PathBuilderOption(func(cfg *PathBuilderConfig) {
		cfg.withDatabase = with
	})
}

var _ PathBuilder = (*AccountDatePathBuilder)(nil)

type AccountDatePathBuilder struct {
	PathBuilderConfig
}

func NewAccountDatePathBuilder(opts ...PathBuilderOption) *AccountDatePathBuilder {
	builder := &AccountDatePathBuilder{}
	for _, opt := range opts {
		opt.Apply(&builder.PathBuilderConfig)
	}
	return builder
}

func (b *AccountDatePathBuilder) Build(account string, typ MergeLogType, ts time.Time, db string, tblName string) string {
	identify := tblName
	if b.withDatabase {
		identify = fmt.Sprintf("%s.%s", db, tblName)
	}
	if ts != ETLParamTSAll {
		return path.Join(account,
			typ.String(),
			fmt.Sprintf("%d", ts.Year()),
			fmt.Sprintf("%02d", ts.Month()),
			fmt.Sprintf("%02d", ts.Day()),
			identify,
		)
	} else {
		return path.Join(account, typ.String(), "*/*/*" /*All datetime*/, identify)
	}
}

// BuildETLPath implement PathBuilder
//
// #     account | typ | ts   | table | filename
// like: *       /*    /*/*/* /metric /*.csv
func (b *AccountDatePathBuilder) BuildETLPath(db, name, account string) string {
	etlDirectory := b.Build(account, ETLParamTypeAll, ETLParamTSAll, db, name)
	etlFilename := "*"
	return path.Join("/", etlDirectory, etlFilename)
}

func (b *AccountDatePathBuilder) ParsePath(ctx context.Context, path string) (Path, error) {
	p := NewETLPath(path)
	return p, p.Parse(ctx)
}

var timeMu sync.Mutex

var NSecString = func() string {
	timeMu.Lock()
	nsec := time.Now().Nanosecond()
	timeMu.Unlock()
	return fmt.Sprintf("%09d", nsec)
}

func (b *AccountDatePathBuilder) NewMergeFilename(timestampStart, timestampEnd, extension string) string {
	seq := NSecString()
	return strings.Join([]string{timestampStart, timestampEnd, string(MergeLogTypeMerged), seq}, FilenameSeparator) + extension
}

func (b *AccountDatePathBuilder) NewLogFilename(name, nodeUUID, nodeType string, ts time.Time, extension string) string {
	seq := NSecString()
	return strings.Join([]string{fmt.Sprintf("%d", ts.Unix()), nodeUUID, nodeType, seq}, FilenameSeparator) + extension
}

func (b *AccountDatePathBuilder) SupportMergeSplit() bool      { return true }
func (b *AccountDatePathBuilder) SupportAccountStrategy() bool { return true }
func (b *AccountDatePathBuilder) GetName() string              { return "AccountDate" }

var _ PathBuilder = (*DBTablePathBuilder)(nil)

type DBTablePathBuilder struct{}

// BuildETLPath implement PathBuilder
//
// like: system/metric_*.csv
func (m *DBTablePathBuilder) BuildETLPath(db, name, account string) string {
	return fmt.Sprintf("%s/%s_*", db, name) + CsvExtension
}

func NewDBTablePathBuilder() *DBTablePathBuilder {
	return &DBTablePathBuilder{}
}

func (m *DBTablePathBuilder) Build(account string, typ MergeLogType, ts time.Time, db string, name string) string {
	return db
}

func (m *DBTablePathBuilder) ParsePath(ctx context.Context, path string) (Path, error) {
	panic("not implement")
}

func (m *DBTablePathBuilder) NewMergeFilename(timestampStart, timestampEnd, extension string) string {
	panic("not implement")
}

func (m *DBTablePathBuilder) NewLogFilename(name, nodeUUID, nodeType string, ts time.Time, extension string) string {
	return fmt.Sprintf(`%s_%s_%s_%s`, name, nodeUUID, nodeType, ts.Format("20060102.150405.000000")) + extension
}

func (m *DBTablePathBuilder) SupportMergeSplit() bool      { return false }
func (m *DBTablePathBuilder) SupportAccountStrategy() bool { return false }
func (m *DBTablePathBuilder) GetName() string              { return "DBTable" }

func PathBuilderFactory(pathBuilder string) PathBuilder {
	switch pathBuilder {
	case (*DBTablePathBuilder)(nil).GetName():
		return NewDBTablePathBuilder()
	case (*AccountDatePathBuilder)(nil).GetName():
		return NewAccountDatePathBuilder()
	default:
		return nil
	}
}

// GetExtension
// deprecated.
func GetExtension(ext string) string {
	switch ext {
	case CsvExtension, TaeExtension:
		return ext
	case "csv":
		return CsvExtension
	case "tae":
		return TaeExtension
	default:
		panic("unknown type of ext")
	}
}

// BufferSettable for util/export/etl/ContentWriter, co-operate with RowWriter
// mode 1: set empty buffer, do the WriteRow and FlushAndClose
//
//	if writer.NeedBuffer(); need {
//	  writer.SetBuffer(buffer, releaseBufferCallback)
//	}
//	for _, row := range rows {
//	  writer.WriteRow(row) // implement RowWriter interface
//	}
//	writer.FlushAndClose()
//
// mode 2: set the fulfill buffer, and do the FlushAndClose
//
//	buf := bytes.NewBuffer(...)
//	buf.Write(....)
//
//	if writer.NeedBuffer(); need {
//	  writer.SetBuffer(buffer, releaseBufferCallback)
//	}
//	writer.FlushAndClose()
type BufferSettable interface {
	// SetBuffer set the buffer into Writer, and the callback for Close or Release.
	// @callback can be nil.
	SetBuffer(buf *bytes.Buffer, callback func(buffer *bytes.Buffer))
	// NeedBuffer return true, means the writer need outside buffer.
	NeedBuffer() bool
}

// Flusher work for util/export/etl/ContentWriter
type Flusher interface {
	// FlushBuffer flush the buffer and close.
	FlushBuffer(*bytes.Buffer) (int, error)
}

// RowWriter for etl export
// base usage: WriteRow -> [ WriteRow -> [ WriteRow -> ... ]] -> FlushAndClose
type RowWriter interface {
	WriteRow(row *Row) error
	// GetContent get buffer content
	GetContent() string
	GetContentLength() int
	// FlushAndClose flush its buffer and close.
	FlushAndClose() (int, error)
}

type BackOff interface {
	// Count do the event count
	// return true, means not in backoff cycle. You can run your code.
	// return false, means you should skip this time.
	Count() bool
}

// BackOffSettable work with reactWriter and ContentWriter
type BackOffSettable interface {
	SetupBackOff(BackOff)
}

// RowField work with Row
// base usage:
//
// tbl := RowField.GetTable()
// row := tbl.GetRow() // Table.GetRow
// RowField.FillRow(row)
// RowWriter.WriteRow(row)
type RowField interface {
	GetTable() *Table
	FillRow(context.Context, *Row)
}

type AckHook func(context.Context)

// AfterWrite for writer which implements RowWriter
// basic work for reactWriter in util/export pkg.
type AfterWrite interface {
	AddAfter(AckHook)
	RowWriter
}

// NeedAck co-operate with AfterWrite and RowField
type NeedAck interface {
	NeedCheckAck() bool
	GetAckHook() AckHook
	RowField
}

// NeedSyncWrite for item with need to do sync-write
// It should trigger export ASAP.
// Co-operate with NeedAck to receiver the ack. And the NeedAck.NeedCheckAck() should return true.
type NeedSyncWrite interface {
	NeedSyncWrite() bool
	NeedAck
}

// WriteRequest work in Collector, is the req passing through the Collector's workflow.
// work on flow: from Generate to Export.
type WriteRequest interface {
	Handle() (int, error)
}

type ExportRequests []WriteRequest

type RowRequest struct {
	writer RowWriter
	// backoff adapt BackOffSettable
	backoff BackOff
}

func NewRowRequest(writer RowWriter, backoff BackOff) *RowRequest {
	return &RowRequest{
		writer:  writer,
		backoff: backoff,
	}
}

func (r *RowRequest) Handle() (n int, err error) {
	if r.writer == nil {
		return 0, nil
	}
	if setter, ok := r.writer.(BackOffSettable); ok {
		setter.SetupBackOff(r.backoff)
	}
	n, err = r.writer.FlushAndClose()
	r.writer = nil
	return
}

// GetContent for test
func (r *RowRequest) GetContent() string {
	return r.writer.GetContent()
}

type WriterFactory interface {
	GetRowWriter(ctx context.Context, account string, tbl *Table, ts time.Time) RowWriter
	GetWriter(ctx context.Context, filepath string) io.WriteCloser
}

var _ WriterFactory = (*defaultWriterGetter)(nil)

type defaultWriterGetter struct {
	rowWriterGetter func(ctx context.Context, account string, tbl *Table, ts time.Time) RowWriter
	writerGetter    func(ctx context.Context, filepath string) io.WriteCloser
}

func NewWriterFactoryGetter(
	rowWriterGetter func(ctx context.Context, account string, tbl *Table, ts time.Time) RowWriter,
	writerGetter func(ctx context.Context, filepath string) io.WriteCloser,
) WriterFactory {
	return &defaultWriterGetter{rowWriterGetter: rowWriterGetter, writerGetter: writerGetter}
}

func (f *defaultWriterGetter) GetRowWriter(ctx context.Context, account string, tbl *Table, ts time.Time) RowWriter {
	return f.rowWriterGetter(ctx, account, tbl, ts)
}

func (f *defaultWriterGetter) GetWriter(ctx context.Context, filepath string) io.WriteCloser {
	return f.writerGetter(ctx, filepath)
}

type FilePathCfg struct {
	NodeUUID  string
	NodeType  string
	Extension string
}

func (c *FilePathCfg) LogsFilePathFactory(account string, tbl *Table, ts time.Time) string {
	filename := tbl.PathBuilder.NewLogFilename(tbl.Table, c.NodeUUID, c.NodeType, ts, c.Extension)
	dir := tbl.PathBuilder.Build(account, MergeLogTypeLogs, ts, tbl.Database, tbl.Table)
	return path.Join(dir, filename)
}

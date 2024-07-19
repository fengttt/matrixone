// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "table_function"

func (tableFunction *TableFunction) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	tblArg := tableFunction
	var (
		f bool
		e error
	)
	idx := tableFunction.GetIdx()

	// Pass the argResult to the table function as input args.
	// get result back in the tblResult.
	argResult, err := tableFunction.GetChildren(0).Call(proc)
	tblResult := &tblArg.GetOperatorBase().Result

	if err != nil {
		return *tblResult, err
	}

	anal := proc.GetAnalyze(tableFunction.GetIdx(), tableFunction.GetParallelIdx(), tableFunction.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	switch tblArg.FuncName {
	case plan2.TableFunctionUnnest:
		f, e = unnestCall(idx, proc, tblArg, argResult, tblResult)
	case plan2.TableFunctionGenerateSeries:
		f, e = generateSeriesCall(idx, proc, tblArg, argResult, tblResult)
	case plan2.TableFunctionMetaScan:
		f, e = metaScanCall(idx, proc, tblArg, argResult, tblResult)
	case plan2.TableFunctionCurrentAccount:
		f, e = currentAccountCall(idx, proc, tblArg, argResult, tblResult)
	case plan2.TableFunctionMetadataScan:
		f, e = metadataScan(idx, proc, tblArg, argResult, tblResult)
	case plan2.TableFunctionProcesslist:
		f, e = processlist(idx, proc, tblArg, argResult, tblResult)
	case plan2.TableFunctionMoLocks:
		f, e = moLocksCall(idx, proc, tblArg, argResult, tblResult)
	case plan2.TableFunctionMoConfigurations:
		f, e = moConfigurationsCall(idx, proc, tblArg, argResult, tblResult)
	case plan2.TableFunctionMoTransactions:
		f, e = moTransactionsCall(idx, proc, tblArg, argResult, tblResult)
	case plan2.TableFunctionMoCache:
		f, e = moCacheCall(idx, proc, tblArg, argResult, tblResult)
	case plan2.TableFunctionWasmTable:
		f, e = wasmTableCall(idx, proc, tblArg, argResult, tblResult)
	default:
		tblResult.Status = vm.ExecStop
		return *tblResult, moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("table function %s is not supported", tblArg.FuncName))
	}
	if e != nil || f {
		if f {
			tblResult.Status = vm.ExecStop
			return *tblResult, e
		}
		return *tblResult, e
	}

	if tableFunction.ctr.buf != nil {
		proc.PutBatch(tableFunction.ctr.buf)
		tableFunction.ctr.buf = nil
	}
	tableFunction.ctr.buf = tblResult.Batch
	// XXX
	// Ownership transferred?  NO ...
	// So we cannot do the following, because the batch in result is used by
	// later operators.
	// tblResult.Batch = nil
	if tableFunction.ctr.buf == nil {
		tblResult.Status = vm.ExecStop
		return *tblResult, e
	}
	if tableFunction.ctr.buf.IsEmpty() {
		return *tblResult, e
	}

	if tableFunction.ctr.buf.VectorCount() != len(tblArg.ctr.retSchema) {
		tblResult.Status = vm.ExecStop
		return *tblResult, moerr.NewInternalError(proc.Ctx, "table function %s return length mismatch", tblArg.FuncName)
	}
	for i := range tblArg.ctr.retSchema {
		if tableFunction.ctr.buf.GetVector(int32(i)).GetType().Oid != tblArg.ctr.retSchema[i].Oid {
			tblResult.Status = vm.ExecStop
			return *tblResult, moerr.NewInternalError(proc.Ctx, "table function %s return type mismatch", tblArg.FuncName)
		}
	}

	if f {
		tblResult.Status = vm.ExecStop
		return *tblResult, e
	}
	return *tblResult, e
}

func (tableFunction *TableFunction) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(tableFunction.FuncName)
}

func (tableFunction *TableFunction) OpType() vm.OpType {
	return vm.TableFunction
}

func (tableFunction *TableFunction) Prepare(proc *process.Process) error {
	tblArg := tableFunction
	tblArg.ctr = new(container)

	retSchema := make([]types.Type, len(tblArg.Rets))
	for i := range tblArg.Rets {
		retSchema[i] = dupType(&tblArg.Rets[i].Typ)
	}
	tblArg.ctr.retSchema = retSchema

	// Set status to ExecNext.   This really should have been the default setting.
	// but we set Stop as default.
	tableFunction.GetOperatorBase().Result.Status = vm.ExecNext

	switch tblArg.FuncName {
	case plan2.TableFunctionUnnest:
		return unnestPrepare(proc, tblArg)
	case plan2.TableFunctionGenerateSeries:
		return generateSeriesPrepare(proc, tblArg)
	case plan2.TableFunctionMetaScan:
		return metaScanPrepare(proc, tblArg)
	case plan2.TableFunctionCurrentAccount:
		return currentAccountPrepare(proc, tblArg)
	case plan2.TableFunctionMetadataScan:
		return metadataScanPrepare(proc, tblArg)
	case plan2.TableFunctionProcesslist:
		return processlistPrepare(proc, tblArg)
	case plan2.TableFunctionMoLocks:
		return moLocksPrepare(proc, tblArg)
	case plan2.TableFunctionMoConfigurations:
		return moConfigurationsPrepare(proc, tblArg)
	case plan2.TableFunctionMoTransactions:
		return moTransactionsPrepare(proc, tblArg)
	case plan2.TableFunctionMoCache:
		return moCachePrepare(proc, tblArg)
	case plan2.TableFunctionWasmTable:
		return wasmTablePrepare(proc, tblArg)

	default:
		return moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("table function %s is not supported", tblArg.FuncName))
	}
}

func dupType(typ *plan.Type) types.Type {
	return types.New(types.T(typ.Id), typ.Width, typ.Scale)
}

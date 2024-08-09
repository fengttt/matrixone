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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "table_function"

func (tableFunction *TableFunction) Call(proc *process.Process) (vm.CallResult, error) {
	var res vm.CallResult
	var err error

	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(tableFunction.GetIdx(), tableFunction.GetParallelIdx(), tableFunction.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	tblArg := tableFunction
	idx := tableFunction.GetIdx()

	// Calling children, get the input batch
	input, err := vm.ChildrenCall(tableFunction.GetChildren(0), proc, anal)
	if err != nil {
		return input, err
	}
	anal.Input(input.Batch, tableFunction.IsFirst)

	switch tblArg.FuncName {
	case "unnest":
		res, err = unnestCall(idx, proc, tblArg, input)
	case "generate_series":
		res, err = generateSeriesCall(idx, proc, tblArg, input)
	case "meta_scan":
		res, err = metaScanCall(idx, proc, tblArg, input)
	case "current_account":
		res, err = currentAccountCall(idx, proc, tblArg, input)
	case "metadata_scan":
		res, err = metadataScan(idx, proc, tblArg, input)
	case "processlist":
		res, err = processlist(idx, proc, tblArg, input)
	case "mo_locks":
		res, err = moLocksCall(idx, proc, tblArg, input)
	case "mo_configurations":
		res, err = moConfigurationsCall(idx, proc, tblArg, input)
	case "mo_transactions":
		res, err = moTransactionsCall(idx, proc, tblArg, input)
	case "mo_cache":
		res, err = moCacheCall(idx, proc, tblArg, input)
	default:
		res.Status = vm.ExecStop
		return res, moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("table function %s is not supported", tblArg.FuncName))
	}

	if err != nil || res.Status == vm.ExecStop {
		res.Status = vm.ExecStop
		res.Batch = nil
		return res, err
	}

	// Some schema checks.   This check SHOULD have been done in compile time or in
	// prepare.
	if res.Batch.VectorCount() != len(tblArg.ctr.retSchema) {
		res.Status = vm.ExecStop
		return res, moerr.NewInternalError(proc.Ctx, "table function %s return length mismatch", tblArg.FuncName)
	}

	for i := range tblArg.ctr.retSchema {
		if res.Batch.GetVector(int32(i)).GetType().Oid != tblArg.ctr.retSchema[i].Oid {
			res.Status = vm.ExecStop
			return res, moerr.NewInternalError(proc.Ctx, "table function %s return type mismatch", tblArg.FuncName)
		}
	}

	return res, nil
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

	retSchema := make([]types.Type, len(tblArg.Rets))
	for i := range tblArg.Rets {
		retSchema[i] = dupType(&tblArg.Rets[i].Typ)
	}
	tblArg.ctr.retSchema = retSchema

	switch tblArg.FuncName {
	case "unnest":
		return unnestPrepare(proc, tblArg)
	case "generate_series":
		return generateSeriesPrepare(proc, tblArg)
	case "meta_scan":
		return metaScanPrepare(proc, tblArg)
	case "current_account":
		return currentAccountPrepare(proc, tblArg)
	case "metadata_scan":
		return metadataScanPrepare(proc, tblArg)
	case "processlist":
		return processlistPrepare(proc, tblArg)
	case "mo_locks":
		return moLocksPrepare(proc, tblArg)
	case "mo_configurations":
		return moConfigurationsPrepare(proc, tblArg)
	case "mo_transactions":
		return moTransactionsPrepare(proc, tblArg)
	case "mo_cache":
		return moCachePrepare(proc, tblArg)
	default:
		return moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("table function %s is not supported", tblArg.FuncName))
	}
}

func dupType(typ *plan.Type) types.Type {
	return types.New(types.T(typ.Id), typ.Width, typ.Scale)
}

// Copyright 2022 Matrix Origin
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

package table_function

import (
	"bytes"
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// wasmTable is a table function that calls a WebAssembly function.
// for example, we can implement a generate series function in WebAssembly
// and call it like this:
// select * from wasm_table('http://localhost:8080/gen.wasm',
//                          'generate_series', '[1, 10]') t;
// wasm_table takes 3 arguments:
// 		1. url: the URL of the WebAssembly module, suppose to be const,
// 		2. fname: the name of the function to call, suppose to be const,
// 		3. input: the input to the function, string.
//
// Each wasm module need to implement two functions:
// 		1. fname_init: first call, it should read the input string and
// 		       prepare the state for the next call.
// 		2. fname_next: second call, it should returns a json string.  The
// 	           json should be an arary of string (next batch of data).
//
// 		These two functions should return a postive integer if there are
// 		more batches, 0 if there is no more data.
//
// at this moment, we have not implemented the cross apply operator, so the
// only reasonable way to use wasm_table is to call it with const.
// Issue: #10060
//
// TODO: Table function execution model is very confusing.   The meaning/method
// of passing args are not clear.  Returning semantics are not cleare either.
//

func wasmTableString(buf *bytes.Buffer) {
	buf.WriteString(plan2.TableFunctionWasmTable)
}

type wasmTableArg struct {
	function.OpBuiltInWasm
	state tfStateState
	url   string
	fname string
	input string
}

func wasmTablePrepare(proc *process.Process, tableFunction *TableFunction) (err error) {
	tableFunction.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableFunction.Args)
	if err != nil {
		return err
	}

	tableFunction.ctr.tfState = new(wasmTableArg)
	if len(tableFunction.Args) != 3 {
		return moerr.NewInvalidInput(proc.Ctx, "table function wasm_table takes 3 arguments")
	}
	return nil
}

func getTfState(tableFunction *TableFunction) *wasmTableArg {
	return tableFunction.ctr.tfState.(*wasmTableArg)
}

func evalArgAt(proc *process.Process, tableFunction *TableFunction, idx uint64, bat *batch.Batch) (string, error) {
	v, err := tableFunction.ctr.executorsForArgs[idx].Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return "", err
	}
	if v.IsNull(0) {
		return "", moerr.NewInvalidInput(proc.Ctx, "table function wasm_table argument cannot be null")
	}
	return v.GetStringAt(0), nil
}

// initilize argument and wasm plugin
func wasmTableSetArgs(proc *process.Process, tableFunction *TableFunction, bat *batch.Batch) error {
	var err error
	tfState := getTfState(tableFunction)

	// Get 3 arguments
	if tfState.url, err = evalArgAt(proc, tableFunction, 0, bat); err != nil {
		return err
	}
	if tfState.fname, err = evalArgAt(proc, tableFunction, 1, bat); err != nil {
		return err
	}
	if tfState.input, err = evalArgAt(proc, tableFunction, 2, bat); err != nil {
		return err
	}

	if err = tfState.BuildWasm(proc.Ctx, tfState.url); err != nil {
		return err
	}

	// call wasm XXX_init
	_, _, err = tfState.RunWasm(tfState.fname+"_init", []byte(tfState.input))
	if err != nil {
		return err
	}

	// state transition
	tfState.state = genBatch
	return nil
}

func wasmTableCall(_ int, proc *process.Process, tableFunction *TableFunction, arg vm.CallResult, result *vm.CallResult) (bool, error) {
	var err error

	tfState := getTfState(tableFunction)
	// create result batch, or reuse the old one
	if result.Batch == nil {
		result.Batch = batch.NewWithSize(1)
		result.Batch.Vecs[0] = proc.GetVector(types.T_varchar.ToType())
	} else {
		result.Batch.CleanOnlyData()
	}

	if tfState.state == initArg {
		err = wasmTableSetArgs(proc, tableFunction, arg.Batch)
		if err != nil {
			return false, err
		}
	}

	// Generate batch
	if tfState.state == genBatch {
		_, res, err := tfState.RunWasm(tfState.fname+"_next", []byte{})
		if err != nil {
			return false, err
		}

		var resSlice []string
		if err = json.Unmarshal(res, &resSlice); err != nil {
			return false, err
		}

		if len(resSlice) == 0 {
			tfState.state = genFinish
			return true, nil
		}

		sz := 0
		for _, s := range resSlice {
			if len(s) >= types.VarlenaInlineSize {
				sz += len(s)
			}
		}
		vec := result.Batch.Vecs[0]
		if err = vec.PreExtendArea(len(resSlice), proc.GetMPool()); err != nil {
			return false, err
		}

		for _, s := range resSlice {
			vector.AppendBytes(vec, []byte(s), false, proc.GetMPool())
		}
		result.Batch.SetRowCount(len(resSlice))
		return false, nil
	} else {
		// state must be genFinish
		return true, nil
	}
}

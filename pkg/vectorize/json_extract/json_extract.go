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

package json_extract

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

func JsonExtract(json, path, result [][]byte) [][]byte {
	logutil.Debugf("jsonExtract: json=%s, path=%s, result=%s", json, path, result)
	for i := range json {
		result = append(result, jsonExtractOne(json[i], path[i]))
	}
	return result
}
func jsonExtractOne(jbytes, pbytes []byte) []byte {
	bj, err := types.ParseSliceToByteJson(jbytes)
	if err != nil {
		logutil.Debugf("jsonExtractOne: error:%v", err)
	}
	return []byte(fmt.Sprintf("%s: %s", bj.String(), string(pbytes)))
}

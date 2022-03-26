// Copyright 2022 PingCAP, Inc.
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

package core

import (
	"github.com/pingcap/tidb/parser/model"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableAsNameForVar(t *testing.T) {
	testCases := []struct {
		varName string
		tblName string
		result  string
	}{
		{"", "t", "00000_t"},
		{"a", "t", "a0001_t"},
		{"ab", "t", "ab002_t"},
		{"abc", "t", "abc03_t"},
		{"abcd", "t", "abcd400000_t"},
		{"abcde", "t", "abcd4e0001_t"},
		{"a", "b_c", "a0001_b_c"},
		{"a_b", "c", "a_b03_c"},
		{"a_B", "c", "a_B03_c"},
	}
	for _, tc := range testCases {
		assert.Equal(t, model.NewCIStr(tc.result), tableAsNameForVar(model.NewCIStr(tc.varName), model.NewCIStr(tc.tblName)))
	}
}

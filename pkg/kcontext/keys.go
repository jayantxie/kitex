/*
 * Copyright 2022 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kcontext

import "sync"

// ContextKey type
type ContextKey int

// Context key types(built-in)
const (
	ContextKeyKMethod ContextKey = iota
	ContextKeyRPCInfo
	ContextKeyCallOption
	ContextKeyCallOptionInfo
	ContextKeyProfiler
	ContextKeyH2MetadataIncoming
	ContextKeyH2MetadataOutgoing
	ContextKeyGrpcHeader
	ContextKeyGrpcTrailer
	ContextKeyGrpcStream
	ContextKeyKReqOp
	ContextKeyKRespOp
	ContextKeyEnd
)

var (
	mutex            sync.Mutex
	variablesIndexer = make(map[interface{}]ContextKey, 32)
	indexedVariables = make([]interface{}, 0, 32)
)

// BindContextKey binds key and ctxKey by variablesIndexer.
func BindContextKey(key interface{}, ctxKey ContextKey) {
	mutex.Lock()
	defer mutex.Unlock()
	variablesIndexer[key] = ctxKey
}

func RegisterContextKeys(keys ...interface{}) {
	mutex.Lock()
	defer mutex.Unlock()
	for _, key := range keys {
		if _, ok := variablesIndexer[key]; !ok {
			variablesIndexer[key] = ContextKey(len(indexedVariables))
			indexedVariables = append(indexedVariables, key)
		}
	}
}

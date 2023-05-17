/*
 * Copyright 2023 CloudWeGo Authors
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

package thriftreflection

import (
	"sync"

	"github.com/cloudwego/thriftgo/reflection"
)

// Files is a registry for looking up or iterating over files and the
// descriptors contained within them.
type Files struct {
	FilesByPath map[string]*reflection.FileDescriptor
}

var globalMutex sync.RWMutex

// GlobalFiles is a global registry of file descriptors.
var GlobalFiles = new(Files)

func RegisterIDL(bytes []byte) {
	AST := reflection.Decode(bytes)
	// TODO: check conflict
	globalMutex.Lock()
	defer globalMutex.Unlock()
	GlobalFiles.FilesByPath[AST.Filename] = AST
}

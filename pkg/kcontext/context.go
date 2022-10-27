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

import "context"

// valueCtx has a builtin slice, which stores values indexed by ContextKey.
type valueCtx struct {
	context.Context

	builtin []interface{}
}

// Value finds a value stored in the builtin slice by the input key. The steps are:
// 1. If key is of type ContextKey, find value directly from builtin slice;
// 2. If not found, try to find the ContextKey corresponding to the key through
// variablesIndexer, and find value from builtin slice by the ContextKey;
// 3. If still not found, try to find value from the parent context.
func (c *valueCtx) Value(key interface{}) interface{} {
	if contextKey, ok := key.(ContextKey); ok {
		if v := c.builtin[contextKey]; v != nil {
			return v
		}
	}
	if idx, ok := variablesIndexer[key]; ok {
		if v := c.builtin[idx]; v != nil {
			return v
		}
	}
	return c.Context.Value(key)
}

func WithValue(parent context.Context, key ContextKey, value interface{}) context.Context {
	if ctx, ok := parent.(*valueCtx); ok {
		ctx.builtin[key] = value
		return ctx
	}

	// create new valueCtx
	ctx := &valueCtx{Context: parent, builtin: make([]interface{}, int(ContextKeyEnd)+len(indexedVariables))}
	ctx.builtin[key] = value
	return ctx
}

func WithValueExt(parent context.Context, key interface{}, value interface{}) context.Context {
	idx, ok := variablesIndexer[key]
	if !ok {
		return nil
	}
	return WithValue(parent, idx, value)
}

func Clone(parent context.Context) context.Context {
	if ctx, ok := parent.(*valueCtx); ok {
		clone := &valueCtx{Context: ctx}
		// array copy assign
		clone.builtin = ctx.builtin
		return clone
	}
	return parent
}

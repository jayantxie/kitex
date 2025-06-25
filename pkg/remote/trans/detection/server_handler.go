/*
 * Copyright 2021 CloudWeGo Authors
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

// Package detection protocol detection, it is used for a scenario that switching KitexProtobuf to gRPC.
// No matter KitexProtobuf or gRPC the server side can handle with this detection handler.
package detection

import (
	"context"
	"net"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/endpoint/sep"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote"
)

// DetectableServerTransHandler implements an additional method ProtocolMatch to help
// DetectionHandler to judge which serverHandler should handle the request data.
type DetectableServerTransHandler interface {
	ProtocolMatch(ctx context.Context, conn net.Conn) (err error)
}

// NewSvrTransHandlerFactory detection factory construction. Each detectableHandlerFactory should return
// a ServerTransHandler which implements DetectableServerTransHandler after called NewTransHandler.
func NewSvrTransHandlerFactory(defaultHandlerFactory remote.ServerTransHandlerFactory,
	detectableHandlerFactory ...interface{}, // must be remote.ServerTransHandlerFactory or remote.ServerStreamTransHandlerFactory
) remote.ServerTransHandlerFactory {
	svrTransFactory := &svrTransHandlerFactory{
		defaultHandlerFactory: defaultHandlerFactory,
	}
	for _, factory := range detectableHandlerFactory {
		if f, ok := factory.(remote.ServerTransHandlerFactory); ok {
			svrTransFactory.detectableHandlerFactory = append(svrTransFactory.detectableHandlerFactory, f)
			continue
		}
		if f, ok := factory.(remote.ServerStreamTransHandlerFactory); ok {
			svrTransFactory.detectableStreamHandlerFactory = append(svrTransFactory.detectableStreamHandlerFactory, f)
			continue
		}
		panic("KITEX: invalid detectableHandlerFactory: must implement remote.ServerTransHandlerFactory or remote.ServerStreamTransHandlerFactory")
	}
	return svrTransFactory
}

type svrTransHandlerFactory struct {
	defaultHandlerFactory          remote.ServerTransHandlerFactory
	detectableHandlerFactory       []remote.ServerTransHandlerFactory
	detectableStreamHandlerFactory []remote.ServerStreamTransHandlerFactory
}

func (f *svrTransHandlerFactory) NewTransHandler(opt *remote.ServerOption) (remote.ServerTransHandler, error) {
	t := &svrTransHandler{}
	var err error
	for i := range f.detectableHandlerFactory {
		h, err := f.detectableHandlerFactory[i].NewTransHandler(opt)
		if err != nil {
			return nil, err
		}
		handler, ok := h.(DetectableServerTransHandler)
		if !ok {
			klog.Errorf("KITEX: failed to append detection server trans handler: %T", h)
			continue
		}
		t.registered = append(t.registered, handler)
	}
	for i := range f.detectableStreamHandlerFactory {
		h, err := f.detectableStreamHandlerFactory[i].NewTransHandler(opt)
		if err != nil {
			return nil, err
		}
		handler, ok := h.(DetectableServerTransHandler)
		if !ok {
			klog.Errorf("KITEX: failed to append detection server stream trans handler: %T", h)
			continue
		}
		t.registered = append(t.registered, handler)
	}
	if t.defaultHandler, err = f.defaultHandlerFactory.NewTransHandler(opt); err != nil {
		return nil, err
	}
	return t, nil
}

var (
	_ remote.ServerTransHandler       = &svrTransHandler{}
	_ remote.ServerStreamTransHandler = &svrTransHandler{}
)

type svrTransHandler struct {
	defaultHandler remote.ServerTransHandler
	registered     []DetectableServerTransHandler
}

func (t *svrTransHandler) Write(ctx context.Context, conn net.Conn, send remote.Message) (nctx context.Context, err error) {
	if r := t.which(ctx); r != nil && r.handler != nil {
		return r.handler.Write(ctx, conn, send)
	}
	return ctx, nil
}

func (t *svrTransHandler) Read(ctx context.Context, conn net.Conn, msg remote.Message) (nctx context.Context, err error) {
	if r := t.which(ctx); r != nil && r.handler != nil {
		return r.handler.Read(ctx, conn, msg)
	}
	return ctx, nil
}

func (t *svrTransHandler) OnRead(ctx context.Context, conn net.Conn) (err error) {
	// only need detect once when connection is reused
	r := t.which(ctx)
	if r.handler != nil {
		return r.handler.OnRead(ctx, conn)
	}
	if r.streamHandler != nil {
		return r.streamHandler.OnRead(ctx, conn)
	}
	// compare preface one by one
	var which DetectableServerTransHandler
	for i := range t.registered {
		if t.registered[i].ProtocolMatch(ctx, conn) == nil {
			which = t.registered[i]
			break
		}
	}
	if which != nil {
		if sh, ok := which.(remote.ServerStreamTransHandler); ok {
			r.ctx, r.streamHandler = ctx, sh
			ctx, err = r.streamHandler.OnActive(ctx, conn)
			if err != nil {
				return err
			}
			return r.streamHandler.OnRead(ctx, conn)
		} else {
			h, _ := which.(remote.ServerTransHandler)
			r.ctx, r.handler = ctx, h
			ctx, err = r.handler.OnActive(ctx, conn)
			if err != nil {
				return err
			}
			return r.handler.OnRead(ctx, conn)
		}
	} else {
		r.ctx, r.handler = ctx, t.defaultHandler
		return r.handler.OnRead(ctx, conn)
	}
}

func (t *svrTransHandler) OnInactive(ctx context.Context, conn net.Conn) {
	// Should use the ctx returned by OnActive in r.ctx
	r := t.which(ctx)
	if r != nil && r.ctx != nil {
		ctx = r.ctx
	}
	if r != nil && r.handler != nil {
		r.handler.OnInactive(ctx, conn)
		return
	}
	if r != nil && r.streamHandler != nil {
		r.streamHandler.OnInactive(ctx, conn)
		return
	}
}

func (t *svrTransHandler) OnError(ctx context.Context, err error, conn net.Conn) {
	r := t.which(ctx)
	if r != nil && r.handler != nil {
		r.handler.OnError(ctx, err, conn)
		return
	}
	if r != nil && r.streamHandler != nil {
		r.streamHandler.OnError(ctx, err, conn)
		return
	}
}

func (t *svrTransHandler) OnMessage(ctx context.Context, args, result remote.Message) (context.Context, error) {
	r := t.which(ctx)
	if r != nil && r.handler != nil {
		return r.handler.OnMessage(ctx, args, result)
	}
	return ctx, nil
}

func (t *svrTransHandler) which(ctx context.Context) *handlerWrapper {
	r, _ := ctx.Value(handlerKey{}).(*handlerWrapper)
	return r
}

func (t *svrTransHandler) SetPipeline(pipeline *remote.SvrTransPipeline) {
	for _, h := range t.registered {
		if hdl, ok := h.(remote.ServerTransHandler); ok {
			hdl.SetPipeline(pipeline)
		}
	}
	t.defaultHandler.SetPipeline(pipeline)
}

func (t *svrTransHandler) SetInvokeHandleFunc(inkHdlFunc endpoint.Endpoint) {
	for _, h := range t.registered {
		if hdl, ok := h.(remote.ServerTransHandler); ok {
			hdl.SetInvokeHandleFunc(inkHdlFunc)
		}
	}
	t.defaultHandler.SetInvokeHandleFunc(inkHdlFunc)
}

func (t *svrTransHandler) SetInvokeStreamFunc(inkStreamFunc sep.StreamEndpoint) {
	for _, h := range t.registered {
		if hdl, ok := h.(remote.ServerStreamTransHandler); ok {
			hdl.SetInvokeStreamFunc(inkStreamFunc)
		}
	}
}

func (t *svrTransHandler) OnActive(ctx context.Context, conn net.Conn) (context.Context, error) {
	ctx, err := t.defaultHandler.OnActive(ctx, conn)
	if err != nil {
		return nil, err
	}
	// svrTransHandler wraps multi kinds of ServerTransHandler.
	// We think that one connection only use one type, it doesn't need to do protocol detection for every request.
	// And ctx is initialized with a new connection, so we put a handlerWrapper into ctx, which for recording
	// the actual handler, then the later request don't need to do detection.
	return context.WithValue(ctx, handlerKey{}, &handlerWrapper{}), nil
}

func (t *svrTransHandler) GracefulShutdown(ctx context.Context) error {
	for i := range t.registered {
		if g, ok := t.registered[i].(remote.GracefulShutdown); ok {
			g.GracefulShutdown(ctx)
		}
	}
	if g, ok := t.defaultHandler.(remote.GracefulShutdown); ok {
		g.GracefulShutdown(ctx)
	}
	return nil
}

type handlerKey struct{}

type handlerWrapper struct {
	ctx           context.Context
	handler       remote.ServerTransHandler
	streamHandler remote.ServerStreamTransHandler
}

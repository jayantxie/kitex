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

package nphttp2

import (
	"context"

	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/codec/grpc"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/streaming"
)

type cliTransHandlerFactory struct {
	remoteService string // remote service name
}

// NewCliTransHandlerFactory ...
func NewCliTransHandlerFactory(remoteService string) remote.ClientStreamTransHandlerFactory {
	return &cliTransHandlerFactory{remoteService: remoteService}
}

func (f *cliTransHandlerFactory) NewTransHandler(opt *remote.ClientOption) (remote.ClientStreamTransHandler, error) {
	size := opt.GRPCConnPoolSize
	if size == 0 {
		size = poolSize()
	}
	return &cliTransHandler{
		opt:    opt,
		codec:  grpc.NewGRPCCodec(grpc.WithThriftCodec(opt.PayloadCodec)),
		cp:     NewConnPool(f.remoteService, size, *opt.GRPCConnectOpts),
		dialer: opt.Dialer,
	}, nil
}

var _ remote.ClientStreamTransHandler = &cliTransHandler{}

type cliTransHandler struct {
	opt    *remote.ClientOption
	codec  remote.Codec
	cp     *connPool
	dialer remote.Dialer
}

func (h *cliTransHandler) NewStream(ctx context.Context, ri rpcinfo.RPCInfo) (streaming.ClientStream, error) {
	var err error
	for _, shdlr := range h.opt.StreamingMetaHandlers {
		ctx, err = shdlr.OnConnectStream(ctx)
		if err != nil {
			return nil, err
		}
	}
	addr := ri.To().Address()
	if addr == nil {
		return nil, kerrors.ErrNoDestAddress
	}
	var opt remote.ConnOption
	opt.Dialer = h.dialer
	opt.ConnectTimeout = ri.Config().ConnectTimeout()
	conn, err := h.cp.Get(ctx, addr.Network(), addr.String(), opt)
	if err != nil {
		return nil, kerrors.ErrGetConnection.WithCause(err)
	}
	sx := &clientStream{
		ctx:     ctx,
		rpcInfo: rpcinfo.GetRPCInfo(ctx),
		svcInfo: h.opt.SvcInfo,
		handler: h,
	}
	sx.conn, _ = conn.(*clientConn)
	sx.grpcStream = &grpcClientStream{
		sx: sx,
	}
	return sx, nil
}

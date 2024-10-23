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

package remote

import (
	"context"
	"net"
	"time"

	"github.com/cloudwego/kitex/pkg/profiler"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/grpc"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/streaming"
)

// ServerOption contains option that is used to init the remote server.
type ServerOption struct {
	TargetSvcInfo *serviceinfo.ServiceInfo

	SvcSearcher ServiceSearcher

	TransServerFactory TransServerFactory

	SvrHandlerFactory ServerTransHandlerFactory

	Codec Codec

	PayloadCodec PayloadCodec

	// Listener is used to specify the server listener, which comes with higher priority than Address below.
	Listener net.Listener

	// Address is the listener addr
	Address net.Addr

	ReusePort bool

	// Duration that server waits for to allow any existing connection to be closed gracefully.
	ExitWaitTime time.Duration

	// Duration that server waits for after error occurs during connection accepting.
	AcceptFailedDelayTime time.Duration

	// Duration that the accepted connection waits for to read or write data.
	MaxConnectionIdleTime time.Duration

	ReadWriteTimeout time.Duration

	InitOrResetRPCInfoFunc func(rpcinfo.RPCInfo, net.Addr) rpcinfo.RPCInfo

	TracerCtl *rpcinfo.TraceController

	Profiler                 profiler.Profiler
	ProfilerTransInfoTagging TransInfoTagging
	ProfilerMessageTagging   MessageTagging

	GRPCCfg *grpc.ServerConfig

	GRPCUnknownServiceHandler func(ctx context.Context, method string, stream streaming.Stream) error

	ServerPipelineHandlers []ServerPipelineHandler

	// for thrift streaming, this is enabled by default
	// for grpc(protobuf) streaming, it's disabled by default, enable with server.WithCompatibleMiddlewareForUnary
	CompatibleMiddlewareForUnary bool
}

func (o *ServerOption) PrependPipelineHandler(h ServerPipelineHandler) {
	o.ServerPipelineHandlers = append([]ServerPipelineHandler{h}, o.ServerPipelineHandlers...)
}

func (o *ServerOption) AppendPipelineHandler(h ServerPipelineHandler) {
	o.ServerPipelineHandlers = append(o.ServerPipelineHandlers, h)
}

// ClientOption is used to init the remote client.
type ClientOption struct {
	SvcInfo *serviceinfo.ServiceInfo

	CliHandlerFactory ClientTransHandlerFactory

	Codec Codec

	PayloadCodec PayloadCodec

	ConnPool ConnPool

	Dialer Dialer

	ClientPipelineHandlers []ClientPipelineHandler

	EnableConnPoolReporter bool
}

func (o *ClientOption) PrependPipelineHandler(h ClientPipelineHandler) {
	o.ClientPipelineHandlers = append([]ClientPipelineHandler{h}, o.ClientPipelineHandlers...)
}

func (o *ClientOption) AppendPipelineHandler(h ClientPipelineHandler) {
	o.ClientPipelineHandlers = append(o.ClientPipelineHandlers, h)
}

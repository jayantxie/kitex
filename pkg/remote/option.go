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
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/cloudwego/kitex/transport"
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

	// TTHeaderStreaming
	TTHeaderStreamingOptions TTHeaderStreamingOptions

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

	Codec Codec

	PayloadCodec PayloadCodec

	Dialer Dialer

	ClientPipelineHandlers []ClientPipelineHandler

	EnableConnPoolReporter bool

	// default
	CliHandlerFactory ClientTransHandlerFactory
	ConnPool          ConnPool
	cliHandler        ClientTransHandler

	// for grpc streaming, only used for streaming call
	GRPCStreamingCliHandlerFactory ClientTransHandlerFactory
	GRPCStreamingConnPool          ConnPool
	grpcStreamingHandler           ClientTransHandler

	// for ttheader streaming, only used for streaming call
	TTHeaderStreamingCliHandlerFactory ClientTransHandlerFactory
	TTHeaderStreamingConnPool          ConnPool
	ttHeaderStreamingHandler           ClientTransHandler
}

func (o *ClientOption) PrependPipelineHandler(h ClientPipelineHandler) {
	o.ClientPipelineHandlers = append([]ClientPipelineHandler{h}, o.ClientPipelineHandlers...)
}

func (o *ClientOption) AppendPipelineHandler(h ClientPipelineHandler) {
	o.ClientPipelineHandlers = append(o.ClientPipelineHandlers, h)
}

func newCliTransHandler(factory ClientTransHandlerFactory, opt *ClientOption) (ClientTransHandler, error) {
	handler, err := factory.NewTransHandler(opt)
	if err != nil {
		return nil, err
	}
	transPl := NewClientTransPipeline(handler)
	for _, h := range opt.ClientPipelineHandlers {
		transPl.AddHandler(h)
	}
	handler.SetPipeline(transPl)
	return transPl, nil
}

func (o *ClientOption) InitializeTransHandler() (err error) {
	o.cliHandler, err = newCliTransHandler(o.CliHandlerFactory, o)
	if err != nil {
		return err
	}
	if o.GRPCStreamingCliHandlerFactory != nil {
		o.grpcStreamingHandler, err = newCliTransHandler(o.GRPCStreamingCliHandlerFactory, o)
		if err != nil {
			return err
		}
	}
	if o.TTHeaderStreamingCliHandlerFactory != nil {
		o.ttHeaderStreamingHandler, err = newCliTransHandler(o.TTHeaderStreamingCliHandlerFactory, o)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *ClientOption) GetTransHandlerAndConnPool(tp transport.Protocol) (handler ClientTransHandler, pool ConnPool) {
	if tp&transport.TTHeaderStreaming == transport.TTHeaderStreaming {
		// ttheader streaming
		pool = o.TTHeaderStreamingConnPool
		handler = o.ttHeaderStreamingHandler
		return
	}
	if tp&transport.GRPC == transport.GRPC && o.grpcStreamingHandler != nil {
		// grpc streaming
		pool = o.GRPCStreamingConnPool
		handler = o.grpcStreamingHandler
	}
	pool = o.ConnPool
	handler = o.cliHandler
	return
}

type TTHeaderStreamingOption struct {
	F func(o *TTHeaderStreamingOptions, di *utils.Slice)
}

type TTHeaderStreamingOptions struct {
	// actually is []ttstream.ServerProviderOption,
	// but we can't import ttstream here because of circular dependency,
	// so we have to use interface{} here.
	TransportOptions []interface{}
}

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

// Package genericclient ...
package genericclient

import (
	"context"
	"runtime"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/client/callopt"
	"github.com/cloudwego/kitex/client/callopt/streamcall"
	igeneric "github.com/cloudwego/kitex/internal/generic"
	"github.com/cloudwego/kitex/pkg/generic"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/streaming"
	"github.com/cloudwego/kitex/transport"
)

var _ Client = &genericServiceClient{}

// NewClient create a generic client
func NewClient(destService string, g generic.Generic, opts ...client.Option) (Client, error) {
	svcInfo := generic.ServiceInfoWithGeneric(g)
	return NewClientWithServiceInfo(destService, g, svcInfo, opts...)
}

// NewClientWithServiceInfo create a generic client with serviceInfo
func NewClientWithServiceInfo(destService string, g generic.Generic, svcInfo *serviceinfo.ServiceInfo, opts ...client.Option) (Client, error) {
	var options []client.Option
	options = append(options, client.WithGeneric(g))
	options = append(options, client.WithDestService(destService))
	options = append(options, client.WithTransportProtocol(transport.TTHeaderStreaming))
	options = append(options, opts...)

	kc, err := client.NewClient(svcInfo, options...)
	if err != nil {
		return nil, err
	}
	cli := &genericServiceClient{
		svcInfo: svcInfo,
		kClient: kc,
		sClient: kc.(client.Streaming),
		g:       g,
	}
	// binary generic need method info context
	cli.isBinaryGeneric, _ = g.GetExtra(generic.IsBinaryGeneric).(bool)
	// http generic get method name by request
	cli.getMethodFunc, _ = g.GetExtra(generic.GetMethodNameByRequestFuncKey).(generic.GetMethodNameByRequestFunc)

	runtime.SetFinalizer(cli, (*genericServiceClient).Close)
	return cli, nil
}

// Client generic client
type Client interface {
	generic.Closer

	// GenericCall generic call
	GenericCall(ctx context.Context, method string, request interface{}, callOptions ...callopt.Option) (response interface{}, err error)
	// ClientStreaming creates an implementation of ClientStreamingClient
	ClientStreaming(ctx context.Context, method string, callOptions ...streamcall.Option) (ClientStreamingClient, error)
	// ServerStreaming creates an implementation of ServerStreamingClient
	ServerStreaming(ctx context.Context, method string, req interface{}, callOptions ...streamcall.Option) (ServerStreamingClient, error)
	// BidirectionalStreaming creates an implementation of BidiStreamingClient
	BidirectionalStreaming(ctx context.Context, method string, callOptions ...streamcall.Option) (BidiStreamingClient, error)
}

type genericServiceClient struct {
	svcInfo         *serviceinfo.ServiceInfo
	kClient         client.Client
	sClient         client.Streaming
	g               generic.Generic
	isBinaryGeneric bool
	getMethodFunc   generic.GetMethodNameByRequestFunc
}

func (gc *genericServiceClient) GenericCall(ctx context.Context, method string, request interface{}, callOptions ...callopt.Option) (response interface{}, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	if gc.isBinaryGeneric {
		// To be compatible with binary generic calls, streaming mode must be passed in.
		ctx = igeneric.WithGenericStreamingMode(ctx, serviceinfo.StreamingNone)
	}
	if gc.getMethodFunc != nil {
		// get method name from http generic by request
		method, _ = gc.getMethodFunc(request)
	}
	var _args *generic.Args
	var _result *generic.Result
	mtInfo := gc.svcInfo.MethodInfo(ctx, method)
	if mtInfo != nil {
		_args = mtInfo.NewArgs().(*generic.Args)
		_args.Method = method
		_args.Request = request
		_result = mtInfo.NewResult().(*generic.Result)
	} else {
		// To correctly report the unknown method error, an empty object is returned here.
		_args = &generic.Args{}
		_result = &generic.Result{}
	}

	if err = gc.kClient.Call(ctx, method, _args, _result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (gc *genericServiceClient) Close() error {
	// no need a finalizer anymore
	runtime.SetFinalizer(gc, nil)

	// Notice: don't need to close kClient because finalizer will close it.
	return gc.g.Close()
}

func (gc *genericServiceClient) ClientStreaming(ctx context.Context, method string, callOptions ...streamcall.Option) (ClientStreamingClient, error) {
	ctx = client.NewCtxWithCallOptions(ctx, streamcall.GetCallOptions(callOptions))
	if gc.isBinaryGeneric {
		// To be compatible with binary generic calls, streaming mode must be passed in.
		ctx = igeneric.WithGenericStreamingMode(ctx, serviceinfo.StreamingClient)
	}
	st, err := gc.sClient.StreamX(ctx, method)
	if err != nil {
		return nil, err
	}
	ri := rpcinfo.GetRPCInfo(st.Context())
	return newClientStreamingClient(ri.Invocation().MethodInfo(), method, st), nil
}

func (gc *genericServiceClient) ServerStreaming(ctx context.Context, method string, req interface{}, callOptions ...streamcall.Option) (ServerStreamingClient, error) {
	ctx = client.NewCtxWithCallOptions(ctx, streamcall.GetCallOptions(callOptions))
	if gc.isBinaryGeneric {
		// To be compatible with binary generic calls, streaming mode must be passed in.
		ctx = igeneric.WithGenericStreamingMode(ctx, serviceinfo.StreamingServer)
	}
	st, err := gc.sClient.StreamX(ctx, method)
	if err != nil {
		return nil, err
	}
	ri := rpcinfo.GetRPCInfo(st.Context())
	stream := newServerStreamingClient(ri.Invocation().MethodInfo(), method, st).(*serverStreamingClient)

	args := stream.methodInfo.NewArgs().(*generic.Args)
	args.Method = stream.method
	args.Request = req
	if err := st.SendMsg(st.Context(), args); err != nil {
		return nil, err
	}
	if err := stream.CloseSend(st.Context()); err != nil {
		return nil, err
	}
	return stream, nil
}

func (gc *genericServiceClient) BidirectionalStreaming(ctx context.Context, method string, callOptions ...streamcall.Option) (BidiStreamingClient, error) {
	ctx = client.NewCtxWithCallOptions(ctx, streamcall.GetCallOptions(callOptions))
	if gc.isBinaryGeneric {
		// To be compatible with binary generic calls, streaming mode must be passed in.
		ctx = igeneric.WithGenericStreamingMode(ctx, serviceinfo.StreamingBidirectional)
	}
	st, err := gc.sClient.StreamX(ctx, method)
	if err != nil {
		return nil, err
	}
	ri := rpcinfo.GetRPCInfo(st.Context())
	return newBidiStreamingClient(ri.Invocation().MethodInfo(), method, st), nil
}

// ClientStreamingClient define client side generic client streaming APIs
type ClientStreamingClient interface {
	Send(ctx context.Context, req interface{}) error
	CloseAndRecv(ctx context.Context) (interface{}, error)
	streaming.ClientStream
}

type clientStreamingClient struct {
	methodInfo serviceinfo.MethodInfo
	method     string
	streaming.ClientStream
}

func newClientStreamingClient(methodInfo serviceinfo.MethodInfo, method string, st streaming.ClientStream) ClientStreamingClient {
	return &clientStreamingClient{
		methodInfo:   methodInfo,
		method:       method,
		ClientStream: st,
	}
}

func (c *clientStreamingClient) Send(ctx context.Context, req interface{}) error {
	args := c.methodInfo.NewArgs().(*generic.Args)
	args.Method = c.method
	args.Request = req
	return c.ClientStream.SendMsg(ctx, args)
}

func (c *clientStreamingClient) CloseAndRecv(ctx context.Context) (interface{}, error) {
	if err := c.ClientStream.CloseSend(ctx); err != nil {
		return nil, err
	}
	res := c.methodInfo.NewResult().(*generic.Result)
	if err := c.ClientStream.RecvMsg(ctx, res); err != nil {
		return nil, err
	}
	return res.GetSuccess(), nil
}

// ServerStreamingClient define client side generic server streaming APIs
type ServerStreamingClient interface {
	Recv(ctx context.Context) (interface{}, error)
	streaming.ClientStream
}

type serverStreamingClient struct {
	methodInfo serviceinfo.MethodInfo
	method     string
	streaming.ClientStream
}

func newServerStreamingClient(methodInfo serviceinfo.MethodInfo, method string, st streaming.ClientStream) ServerStreamingClient {
	return &serverStreamingClient{
		methodInfo:   methodInfo,
		method:       method,
		ClientStream: st,
	}
}

func (c *serverStreamingClient) Recv(ctx context.Context) (interface{}, error) {
	res := c.methodInfo.NewResult().(*generic.Result)
	if err := c.ClientStream.RecvMsg(ctx, res); err != nil {
		return nil, err
	}
	return res.GetSuccess(), nil
}

// BidiStreamingClient define client side generic bidirectional streaming APIs
type BidiStreamingClient interface {
	Send(ctx context.Context, req interface{}) error
	Recv(ctx context.Context) (interface{}, error)
	streaming.ClientStream
}

type bidiStreamingClient struct {
	methodInfo serviceinfo.MethodInfo
	method     string
	streaming.ClientStream
}

func newBidiStreamingClient(methodInfo serviceinfo.MethodInfo, method string, st streaming.ClientStream) BidiStreamingClient {
	return &bidiStreamingClient{
		methodInfo:   methodInfo,
		method:       method,
		ClientStream: st,
	}
}

func (c *bidiStreamingClient) Send(ctx context.Context, req interface{}) error {
	args := c.methodInfo.NewArgs().(*generic.Args)
	args.Method = c.method
	args.Request = req
	return c.ClientStream.SendMsg(ctx, args)
}

func (c *bidiStreamingClient) Recv(ctx context.Context) (interface{}, error) {
	res := c.methodInfo.NewResult().(*generic.Result)
	if err := c.ClientStream.RecvMsg(ctx, res); err != nil {
		return nil, err
	}
	return res.GetSuccess(), nil
}

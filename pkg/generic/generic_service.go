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

package generic

import (
	"context"

	"github.com/cloudwego/kitex/internal/generic"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/streaming"
)

// Service generic service interface
type Service interface {
	// GenericCall handle the generic call
	GenericCall(ctx context.Context, method string, request interface{}) (response interface{}, err error)
}

type StreamingService interface {
	ClientStreaming(ctx context.Context, method string, stream ClientStreamingServer) (err error)
	ServerStreaming(ctx context.Context, method string, request interface{}, stream ServerStreamingServer) (err error)
	BidiStreaming(ctx context.Context, method string, stream BidiStreamingServer) (err error)
}

// ServiceInfoWithGeneric create a generic ServiceInfo
func ServiceInfoWithGeneric(g Generic) *serviceinfo.ServiceInfo {
	handlerType := (*Service)(nil)

	methods, svcName := getMethodInfo(g, g.IDLServiceName())

	svcInfo := &serviceinfo.ServiceInfo{
		ServiceName:  svcName,
		HandlerType:  handlerType,
		Methods:      methods,
		PayloadCodec: g.PayloadCodecType(),
		Extra:        make(map[string]interface{}),
	}
	svcInfo.Extra["generic"] = true
	if extra, ok := g.(ExtraProvider); ok {
		if extra.GetExtra(CombineServiceKey) == "true" {
			svcInfo.Extra[CombineServiceKey] = true
		}
		if pkg := extra.GetExtra(packageNameKey); pkg != "" {
			svcInfo.Extra[packageNameKey] = pkg
		}
	}
	return svcInfo
}

func getMethodInfo(g Generic, serviceName string) (methods map[string]serviceinfo.MethodInfo, svcName string) {
	if g.PayloadCodec() != nil {
		// note: binary generic cannot be used with multi-service feature
		svcName = serviceinfo.GenericService
		methods = map[string]serviceinfo.MethodInfo{
			serviceinfo.GenericMethod: serviceinfo.NewMethodInfo(callHandler, newGenericServiceCallArgs, newGenericServiceCallResult, false),
		}
	} else {
		svcName = serviceName
		methodInfoGetter := func(handler serviceinfo.MethodHandler, streamMode serviceinfo.StreamingMode) serviceinfo.MethodInfo {
			return serviceinfo.NewMethodInfo(
				handler,
				func() interface{} {
					args := &Args{}
					args.SetCodec(g.MessageReaderWriter())
					return args
				},
				func() interface{} {
					result := &Result{}
					result.SetCodec(g.MessageReaderWriter())
					return result
				},
				false,
				serviceinfo.WithStreamingMode(streamMode),
			)
		}
		methods = map[string]serviceinfo.MethodInfo{
			serviceinfo.GenericClientStreamingMethod: methodInfoGetter(clientStreamingHandlerGetter(
				// note: construct method info twice to make the method handler obtain the args/results newer.
				methodInfoGetter(nil, serviceinfo.StreamingClient)), serviceinfo.StreamingClient),
			serviceinfo.GenericServerStreamingMethod: methodInfoGetter(serverStreamingHandlerGetter(
				// note: construct method info twice to make the method handler obtain the args/results newer.
				methodInfoGetter(nil, serviceinfo.StreamingServer)), serviceinfo.StreamingServer),
			serviceinfo.GenericBidirectionalStreamingMethod: methodInfoGetter(bidiStreamingHandlerGetter(
				// note: construct method info twice to make the method handler obtain the args/results newer.
				methodInfoGetter(nil, serviceinfo.StreamingBidirectional)), serviceinfo.StreamingBidirectional),
			serviceinfo.GenericMethod: methodInfoGetter(callHandler, serviceinfo.StreamingNone),
		}
	}
	return
}

// GetMethodInfo is only used in kitex, please DON'T USE IT.
// DEPRECATED: this method is no longer used. This method will be removed in the future
func GetMethodInfo(messageReaderWriter interface{}, serviceName string) (methods map[string]serviceinfo.MethodInfo, svcName string) {
	if messageReaderWriter == nil {
		// note: binary generic cannot be used with multi-service feature
		svcName = serviceinfo.GenericService
		methods = map[string]serviceinfo.MethodInfo{
			serviceinfo.GenericMethod: serviceinfo.NewMethodInfo(callHandler, newGenericServiceCallArgs, newGenericServiceCallResult, false),
		}
	} else {
		svcName = serviceName
		methods = map[string]serviceinfo.MethodInfo{
			serviceinfo.GenericMethod: serviceinfo.NewMethodInfo(
				callHandler,
				func() interface{} {
					args := &Args{}
					args.SetCodec(messageReaderWriter)
					return args
				},
				func() interface{} {
					result := &Result{}
					result.SetCodec(messageReaderWriter)
					return result
				},
				false,
			),
		}
	}
	return
}

func callHandler(ctx context.Context, handler, arg, result interface{}) error {
	realArg := arg.(*Args)
	realResult := result.(*Result)
	success, err := handler.(Service).GenericCall(ctx, realArg.Method, realArg.Request)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}

func clientStreamingHandlerGetter(mi serviceinfo.MethodInfo) serviceinfo.MethodHandler {
	return func(ctx context.Context, handler, arg, result interface{}) error {
		st, err := streaming.GetServerStreamFromArg(arg)
		if err != nil {
			return err
		}
		gst := &clientStreamingServer{
			methodInfo:   mi,
			ServerStream: st,
		}
		ri := rpcinfo.GetRPCInfo(ctx)
		return handler.(StreamingService).ClientStreaming(ctx, ri.Invocation().MethodName(), gst)
	}
}

func serverStreamingHandlerGetter(mi serviceinfo.MethodInfo) serviceinfo.MethodHandler {
	return func(ctx context.Context, handler, arg, result interface{}) error {
		st, err := streaming.GetServerStreamFromArg(arg)
		if err != nil {
			return err
		}
		gst := &serverStreamingServer{
			methodInfo:   mi,
			ServerStream: st,
		}
		args := mi.NewArgs().(*Args)
		if err = st.RecvMsg(ctx, args); err != nil {
			return err
		}
		ri := rpcinfo.GetRPCInfo(ctx)
		return handler.(StreamingService).ServerStreaming(ctx, ri.Invocation().MethodName(), args.Request, gst)
	}
}

func bidiStreamingHandlerGetter(mi serviceinfo.MethodInfo) serviceinfo.MethodHandler {
	return func(ctx context.Context, handler, arg, result interface{}) error {
		st, err := streaming.GetServerStreamFromArg(arg)
		if err != nil {
			return err
		}
		gst := &bidiStreamingServer{
			methodInfo:   mi,
			ServerStream: st,
		}
		ri := rpcinfo.GetRPCInfo(ctx)
		return handler.(StreamingService).BidiStreaming(ctx, ri.Invocation().MethodName(), gst)
	}
}

func newGenericServiceCallArgs() interface{} {
	return &Args{}
}

func newGenericServiceCallResult() interface{} {
	return &Result{}
}

// WithCodec set codec instance for Args or Result
type WithCodec interface {
	SetCodec(codec interface{})
}

// Args generic request
type Args = generic.Args

// Result generic response
type Result = generic.Result

var (
	_ WithCodec = (*Args)(nil)
	_ WithCodec = (*Result)(nil)
)

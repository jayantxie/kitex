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
	"errors"
	"fmt"

	igeneric "github.com/cloudwego/kitex/internal/generic"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/streaming"
	"github.com/cloudwego/kitex/transport"
)

var (
	errGenericCallNotImplemented                     = errors.New("generic.ServiceV2 GenericCall not implemented")
	errClientStreamingNotImplemented                 = errors.New("generic.ServiceV2 ClientStreaming not implemented")
	errServerStreamingNotImplemented                 = errors.New("generic.ServiceV2 ServerStreaming not implemented")
	errBidiStreamingNotImplemented                   = errors.New("generic.ServiceV2 BidiStreaming not implemented")
	errDefaultHandlerOfUnknownHandlerNotImplemented  = errors.New("generic.UnknownServiceOrMethodHandler DefaultHandler not implemented")
	errGRPCHandlerOfUnknownHandlerNotImplemented     = errors.New("generic.UnknownServiceOrMethodHandler GRPCHandler not implemented")
	errTTStreamHandlerOfUnknownHandlerNotImplemented = errors.New("generic.UnknownServiceOrMethodHandler TTStreamHandler not implemented")
)

// Service is the v1 generic service interface.
type Service interface {
	// GenericCall handle the generic call
	GenericCall(ctx context.Context, method string, request interface{}) (response interface{}, err error)
}

// ServiceV2 is the new generic service interface, provides methods to handle streaming requests
// and it also supports multi services. All methods are optional.
type ServiceV2 struct {
	// GenericCall handle the generic call
	GenericCall func(ctx context.Context, service, method string, request interface{}) (response interface{}, err error)
	// ClientStreaming handle the client streaming call
	ClientStreaming func(ctx context.Context, service, method string, stream ClientStreamingServer) (err error)
	// ServerStreaming handle the server streaming call
	ServerStreaming func(ctx context.Context, service, method string, request interface{}, stream ServerStreamingServer) (err error)
	// BidiStreaming handle the bidi streaming call
	BidiStreaming func(ctx context.Context, service, method string, stream BidiStreamingServer) (err error)
}

// ServiceV2Iface defines an interface that extends and implements all the methods required by ServiceV2,
// which can simplify the construction of the ServiceV2 class when used in conjunction with the ServiceV2Iface2ServiceV2 method.
type ServiceV2Iface interface {
	// GenericCall handle the generic call
	GenericCall(ctx context.Context, service, method string, request interface{}) (response interface{}, err error)
	// ClientStreaming handle the client streaming call
	ClientStreaming(ctx context.Context, service, method string, stream ClientStreamingServer) (err error)
	// ServerStreaming handle the server streaming call
	ServerStreaming(ctx context.Context, service, method string, request interface{}, stream ServerStreamingServer) (err error)
	// BidiStreaming handle the bidi streaming call
	BidiStreaming(ctx context.Context, service, method string, stream BidiStreamingServer) (err error)
}

// ServiceV2Iface2ServiceV2 converts ServiceV2Iface to ServiceV2.
func ServiceV2Iface2ServiceV2(iface ServiceV2Iface) *ServiceV2 {
	return &ServiceV2{
		GenericCall:     iface.GenericCall,
		ClientStreaming: iface.ClientStreaming,
		ServerStreaming: iface.ServerStreaming,
		BidiStreaming:   iface.BidiStreaming,
	}
}

// UnknownServiceOrMethodHandler handles unknown service or method traffic.
type UnknownServiceOrMethodHandler struct {
	// optional, handle ttheader/framed traffic, support both thrift binary and protobuf binary
	DefaultHandler func(ctx context.Context, service, method string, request interface{}) (response interface{}, err error)
	// optional, handle ttstream traffic, support only thrift binary
	TTStreamHandler func(ctx context.Context, service, method string, stream BidiStreamingServer) (err error)
	// optional, handle grpc traffic (include grpc unary), support both thrift binary and protobuf binary
	GRPCHandler func(ctx context.Context, service, method string, stream BidiStreamingServer) (err error)
}

// ServiceInfoWithGeneric create a generic ServiceInfo from Generic.
// Users could creates multiple ServiceInfo with different Generic,
// and resgiter them through RegisterService interface of server.Server.
// In this way, users could implement multi-service server.
// e.g.
//
//	svr := server.NewServer(opts...)
//	svr.RegisterService(ServiceInfoWithGeneric(generic.BinaryThriftGenericV2("EchoService")), &echoService{})
//  svr.RegisterService(ServiceInfoWithGeneric(generic.MapThriftGeneric(p)), &echo2Service{})
func ServiceInfoWithGeneric(g Generic) *serviceinfo.ServiceInfo {
	handlerType := (*Service)(nil)

	svcInfo := &serviceinfo.ServiceInfo{
		ServiceName:   g.IDLServiceName(),
		HandlerType:   handlerType,
		Methods:       make(map[string]serviceinfo.MethodInfo),
		PayloadCodec:  g.PayloadCodecType(),
		Extra:         make(map[string]interface{}),
		GenericMethod: g.GenericMethod(),
	}
	svcInfo.Extra["generic"] = true
	if combineService, _ := g.GetExtra(serviceinfo.CombineServiceKey).(bool); combineService {
		svcInfo.Extra[serviceinfo.CombineServiceKey] = true
	}
	if pkg, _ := g.GetExtra(serviceinfo.PackageName).(string); pkg != "" {
		svcInfo.Extra[serviceinfo.PackageName] = pkg
	}
	if isBinaryGeneric, _ := g.GetExtra(IsBinaryGeneric).(bool); isBinaryGeneric {
		svcInfo.Extra[IsBinaryGeneric] = true
	}
	return svcInfo
}

func callHandler(ctx context.Context, handler, arg, result interface{}) error {
	realArg := arg.(*Args)
	realResult := result.(*Result)
	switch svc := handler.(type) {
	case *ServiceV2:
		ri := rpcinfo.GetRPCInfo(ctx)
		methodName := ri.Invocation().MethodName()
		serviceName := ri.Invocation().ServiceName()
		if svc.GenericCall == nil {
			return errGenericCallNotImplemented
		}
		success, err := svc.GenericCall(ctx, serviceName, methodName, realArg.Request)
		if err != nil {
			return err
		}
		realResult.Success = success
		return nil
	case Service:
		success, err := handler.(Service).GenericCall(ctx, realArg.Method, realArg.Request)
		if err != nil {
			return err
		}
		realResult.Success = success
		return nil
	case *UnknownServiceOrMethodHandler:
		ri := rpcinfo.GetRPCInfo(ctx)
		methodName := ri.Invocation().MethodName()
		serviceName := ri.Invocation().ServiceName()
		if svc.DefaultHandler == nil {
			return errDefaultHandlerOfUnknownHandlerNotImplemented
		}
		success, err := svc.DefaultHandler(ctx, serviceName, methodName, realArg.Request)
		if err != nil {
			return err
		}
		realResult.Success = success
		return nil
	default:
		return fmt.Errorf("CallHandler: unknown handler type %T", handler)
	}

}

func newGenericServiceCallArgs() interface{} {
	return &Args{}
}

func newGenericServiceCallResult() interface{} {
	return &Result{}
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
		methodName := ri.Invocation().MethodName()
		serviceName := ri.Invocation().ServiceName()
		svcv2 := handler.(*ServiceV2)
		if svcv2.ClientStreaming == nil {
			return errClientStreamingNotImplemented
		}
		return svcv2.ClientStreaming(ctx, serviceName, methodName, gst)
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
		methodName := ri.Invocation().MethodName()
		serviceName := ri.Invocation().ServiceName()
		svcv2 := handler.(*ServiceV2)
		if svcv2.ServerStreaming == nil {
			return errServerStreamingNotImplemented
		}
		return svcv2.ServerStreaming(ctx, serviceName, methodName, args.Request, gst)
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
		methodName := ri.Invocation().MethodName()
		serviceName := ri.Invocation().ServiceName()
		switch svc := handler.(type) {
		case *ServiceV2:
			if svc.BidiStreaming == nil {
				return errBidiStreamingNotImplemented
			}
			return svc.BidiStreaming(ctx, serviceName, methodName, gst)
		case *UnknownServiceOrMethodHandler:
			protocol := ri.Config().TransportProtocol()
			switch {
			case protocol&transport.GRPC != 0:
				if svc.GRPCHandler == nil {
					return errGRPCHandlerOfUnknownHandlerNotImplemented
				}
				return svc.GRPCHandler(ctx, serviceName, methodName, gst)
			case protocol&transport.TTHeaderStreaming != 0:
				if svc.TTStreamHandler == nil {
					return errTTStreamHandlerOfUnknownHandlerNotImplemented
				}
				return svc.TTStreamHandler(ctx, serviceName, methodName, gst)
			default:
				return fmt.Errorf("BidiStreamingHandler: unknown transport protocol %v", protocol.String())
			}
		default:
			return fmt.Errorf("BidiStreamingHandler: unknown handler type %T", handler)
		}
	}
}

// WithCodec set codec instance for Args or Result
type WithCodec interface {
	SetCodec(codec interface{})
}

// Args generic request
type Args = igeneric.Args

// Result generic response
type Result = igeneric.Result

var (
	_ WithCodec = (*Args)(nil)
	_ WithCodec = (*Result)(nil)
)

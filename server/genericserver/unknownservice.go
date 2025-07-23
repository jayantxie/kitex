/*
 * Copyright 2025 CloudWeGo Authors
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

package genericserver

import (
	"errors"

	iserver "github.com/cloudwego/kitex/internal/server"
	"github.com/cloudwego/kitex/pkg/generic"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/server"
)

// RegisterUnknownServiceOrMethodHandler registers a service handler for unknown service or method traffic.
//
// After invoking this interface, the impact on server traffic processing is as follows:
//
// - Unknown service scenario:
//  1. Match the service info which has the same service name as the requested service name.
//  2. If no match exists, a unique service info will be created using the requested service name
//     and returned as the result of service searching.
//
// - Unknown method scenario:
//  1. Match the method info which has the same method name as the requested method name.
//  2. If no match exists,
//     - For ttheader/framed traffic: Requests are processed via the generic.UnknownServiceOrMethodHandler.DefaultHandler.
//     - For grpc traffic (include grpc unary): Requests are handled by the generic.UnknownServiceOrMethodHandler.GRPCHandler.
//     - For ttstream traffic: Requests are handled by the generic.UnknownServiceOrMethodHandler.TTStreamHandler.
//
// This implementation relies on binary generic invocation.
// User-processed request/response objects through generic.UnknownServiceOrMethodHandler
// are of type []byte, containing serialized data of the original request/response.
// And note that for Thrift Unary methods, the serialized data includes encapsulation of the Arg or Result structure.
//
// Code example:
//		svr := server.NewServer(opts...)
//		err := genericserver.RegisterUnknownServiceOrMethodHandler(svr, &generic.UnknownServiceOrMethodHandler{
//			DefaultHandler:  defaultHandler,
//			TTStreamHandler: ttstreamHandler,
//			GRPCHandler: grpcHandler,
//		})
func RegisterUnknownServiceOrMethodHandler(svr server.Server, unknownHandler *generic.UnknownServiceOrMethodHandler) error {
	if unknownHandler.DefaultHandler == nil && unknownHandler.GRPCHandler == nil && unknownHandler.TTStreamHandler == nil {
		return errors.New("neither default nor grpc nor ttstream handler registered")
	}
	return svr.RegisterService(&serviceinfo.ServiceInfo{
		ServiceName: "$UnknownService", // meaningless service info
	}, unknownHandler, iserver.WithUnknownService())
}

// NewUnknownServiceOrMethodServer creates a server which registered UnknownServiceOrMethodHandler
// through RegisterUnknownServiceOrMethodHandler. For more details, see RegisterUnknownServiceOrMethodHandler.
func NewUnknownServiceOrMethodServer(unknownHandler *generic.UnknownServiceOrMethodHandler, options ...server.Option) server.Server {
	svr := server.NewServer(options...)
	if err := RegisterUnknownServiceOrMethodHandler(svr, unknownHandler); err != nil {
		panic(err)
	}
	return svr
}

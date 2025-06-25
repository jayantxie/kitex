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

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/endpoint/sep"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/streaming"
)

// ClientTransHandlerFactory to new ClientTransHandler for client
type ClientTransHandlerFactory interface {
	NewTransHandler(opt *ClientOption) (ClientTransHandler, error)
}

// ClientStreamTransHandlerFactory to new ClientStreamTransHandler for client
type ClientStreamTransHandlerFactory interface {
	NewTransHandler(opt *ClientOption) (ClientStreamTransHandler, error)
}

// ServerTransHandlerFactory to new ServerTransHandler for server
type ServerTransHandlerFactory interface {
	NewTransHandler(opt *ServerOption) (ServerTransHandler, error)
}

// ServerStreamTransHandlerFactory to new ServerStreamTransHandler for server
type ServerStreamTransHandlerFactory interface {
	NewTransHandler(opt *ServerOption) (ServerStreamTransHandler, error)
}

// TransReadWriter defines the interface for unary client/server trans handler,
// Write writes a message to the connection, and Read reads a message from the connection.
type TransReadWriter interface {
	Write(ctx context.Context, conn net.Conn, send Message) (nctx context.Context, err error)
	Read(ctx context.Context, conn net.Conn, msg Message) (nctx context.Context, err error)
}

// ClientTransHandler provides Read/Write methods to handle ping pong messages.
// It serves protocols that only support ping pong, such as ttheader framed.
// Transport can be refactored to support pipeline, and then is able to support other extensions at conn level.
type ClientTransHandler interface {
	TransReadWriter
	OnError(ctx context.Context, err error, conn net.Conn)
	OnMessage(ctx context.Context, args, result Message) (context.Context, error)
}

// ClientStreamTransHandler provides NewStream method to create a new client stream.
type ClientStreamTransHandler interface {
	NewStream(ctx context.Context, ri rpcinfo.RPCInfo) (streaming.ClientStream, error)
}

// ServerTransHandler provides Read/Write methods to handle ping pong messages.
// It serves protocols that only support ping pong, such as ttheader framed.
// Transport can be refactored to support pipeline, and then is able to support other extensions at conn level.
type ServerTransHandler interface {
	TransReadWriter
	OnActive(ctx context.Context, conn net.Conn) (context.Context, error)
	OnInactive(ctx context.Context, conn net.Conn)
	OnError(ctx context.Context, err error, conn net.Conn)
	OnRead(ctx context.Context, conn net.Conn) error
	OnMessage(ctx context.Context, args, result Message) (context.Context, error)
	SetPipeline(pipeline *SvrTransPipeline)
	SetInvokeHandleFunc(inkHdlFunc endpoint.Endpoint)
}

// ServerStreamTransHandler removes the interface definition of ServerTransHandler for unary messages,
// and is more suitable for streaming protocols.
type ServerStreamTransHandler interface {
	OnActive(ctx context.Context, conn net.Conn) (context.Context, error)
	OnInactive(ctx context.Context, conn net.Conn)
	OnError(ctx context.Context, err error, conn net.Conn)
	OnRead(ctx context.Context, conn net.Conn) error
	SetInvokeStreamFunc(inkStFunc sep.StreamEndpoint)
}

// GracefulShutdown supports closing connections in a graceful manner.
type GracefulShutdown interface {
	GracefulShutdown(ctx context.Context) error
}

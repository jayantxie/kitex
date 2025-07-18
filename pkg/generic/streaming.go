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

package generic

import (
	"context"

	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/streaming"
)

type ClientStreamingServer interface {
	Recv(ctx context.Context) (req interface{}, err error)
	SendAndClose(ctx context.Context, res interface{}) error
	streaming.ServerStream
}

type clientStreamingServer struct {
	methodInfo serviceinfo.MethodInfo
	streaming.ServerStream
}

func (s *clientStreamingServer) Recv(ctx context.Context) (req interface{}, err error) {
	args := s.methodInfo.NewArgs().(*Args)
	if err = s.ServerStream.RecvMsg(ctx, args); err != nil {
		return
	}
	req = args.Request
	return
}

func (s *clientStreamingServer) SendAndClose(ctx context.Context, res interface{}) error {
	results := s.methodInfo.NewResult().(*Result)
	results.Success = res
	return s.ServerStream.SendMsg(ctx, results)
}

type ServerStreamingServer interface {
	Send(ctx context.Context, res interface{}) error
	streaming.ServerStream
}

type serverStreamingServer struct {
	methodInfo serviceinfo.MethodInfo
	streaming.ServerStream
}

func (s *serverStreamingServer) Send(ctx context.Context, res interface{}) error {
	results := s.methodInfo.NewResult().(*Result)
	results.Success = res
	return s.ServerStream.SendMsg(ctx, results)
}

type BidiStreamingServer interface {
	Recv(ctx context.Context) (req interface{}, err error)
	Send(ctx context.Context, res interface{}) error
	streaming.ServerStream
}

type bidiStreamingServer struct {
	methodInfo serviceinfo.MethodInfo
	streaming.ServerStream
}

func (s *bidiStreamingServer) Recv(ctx context.Context) (req interface{}, err error) {
	args := s.methodInfo.NewArgs().(*Args)
	if err = s.ServerStream.RecvMsg(ctx, args); err != nil {
		return
	}
	req = args.Request
	return
}

func (s *bidiStreamingServer) Send(ctx context.Context, res interface{}) error {
	results := s.methodInfo.NewResult().(*Result)
	results.Success = res
	return s.ServerStream.SendMsg(ctx, results)
}

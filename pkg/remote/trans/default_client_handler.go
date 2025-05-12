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

package trans

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/stats"
	"github.com/cloudwego/kitex/pkg/streaming"
)

// NewDefaultCliTransHandler to provide default impl of cliTransHandler, it can be reused in netpoll, shm-ipc, framework-sdk extensions
func NewDefaultCliTransHandler(opt *remote.ClientOption, ext Extension) (remote.ClientTransHandler, error) {
	return &cliTransHandler{
		opt:   opt,
		codec: opt.Codec,
		ext:   ext,
	}, nil
}

type cliTransHandler struct {
	opt   *remote.ClientOption
	codec remote.Codec
	rw    cliPipeTransReaderWriter
	ext   Extension
}

// Write implements the remote.ClientTransHandler interface.
func (t *cliTransHandler) Write(ctx context.Context, conn net.Conn, sendMsg remote.Message) (nctx context.Context, err error) {
	var bufWriter remote.ByteBuffer
	rpcinfo.Record(ctx, sendMsg.RPCInfo(), stats.WriteStart, nil)
	defer func() {
		t.ext.ReleaseBuffer(bufWriter, err)
		rpcinfo.Record(ctx, sendMsg.RPCInfo(), stats.WriteFinish, err)
	}()

	bufWriter = t.ext.NewWriteByteBuffer(ctx, conn, sendMsg)
	sendMsg.SetPayloadCodec(t.opt.PayloadCodec)
	err = t.codec.Encode(ctx, sendMsg, bufWriter)
	if err != nil {
		return ctx, err
	}
	return ctx, bufWriter.Flush()
}

// Read implements the remote.ClientTransHandler interface.
func (t *cliTransHandler) Read(ctx context.Context, conn net.Conn, recvMsg remote.Message) (nctx context.Context, err error) {
	var bufReader remote.ByteBuffer
	rpcinfo.Record(ctx, recvMsg.RPCInfo(), stats.ReadStart, nil)
	defer func() {
		t.ext.ReleaseBuffer(bufReader, err)
		rpcinfo.Record(ctx, recvMsg.RPCInfo(), stats.ReadFinish, err)
	}()

	t.ext.SetReadTimeout(ctx, conn, recvMsg.RPCInfo().Config(), recvMsg.RPCRole())
	bufReader = t.ext.NewReadByteBuffer(ctx, conn, recvMsg)
	recvMsg.SetPayloadCodec(t.opt.PayloadCodec)
	err = t.codec.Decode(ctx, recvMsg, bufReader)
	if err != nil {
		if t.ext.IsTimeoutErr(err) {
			err = kerrors.ErrRPCTimeout.WithCause(err)
		}
		return ctx, err
	}

	return ctx, nil
}

func (t *cliTransHandler) NewStream(ctx context.Context, conn net.Conn) (streaming.ClientStream, error) {
	ri := rpcinfo.GetRPCInfo(ctx)
	cs := cliStreamPool.Get().(*cliStream)
	cs.ctx = ctx
	cs.ri = ri
	cs.conn = conn
	cs.t = t
	cs.rw = t.rw
	return cs, nil
}

func (t *cliTransHandler) SetPipeline(pipe *remote.ClientTransPipeline) {
	if rw, ok := pipe.ClientTransHandler.(remote.TransReadWriter); ok {
		t.rw = cliPipeTransReaderWriter{TransReadWriter: rw, pipe: pipe}
	} else {
		t.rw = cliPipeTransReaderWriter{TransReadWriter: t, pipe: pipe}
	}
}

// OnInactive implements the remote.ClientTransHandler interface.
func (t *cliTransHandler) OnInactive(ctx context.Context, conn net.Conn) {
	// ineffective now and do nothing
}

// OnError implements the remote.ClientTransHandler interface.
func (t *cliTransHandler) OnError(ctx context.Context, err error, conn net.Conn) {
	if pe, ok := err.(*kerrors.DetailedError); ok {
		klog.CtxErrorf(ctx, "KITEX: send request error, remote=%s, error=%s\nstack=%s", conn.RemoteAddr(), err.Error(), pe.Stack())
	} else {
		klog.CtxErrorf(ctx, "KITEX: send request error, remote=%s, error=%s", conn.RemoteAddr(), err.Error())
	}
}

type cliPipeTransReaderWriter struct {
	remote.TransReadWriter
	pipe *remote.ClientTransPipeline
}

func (s *cliPipeTransReaderWriter) Write(ctx context.Context, conn net.Conn, sendMsg remote.Message) (nctx context.Context, err error) {
	ctx, err = s.pipe.OnStreamWrite(ctx, nil, sendMsg)
	if err != nil {
		return ctx, err
	}
	return s.TransReadWriter.Write(ctx, conn, sendMsg)
}

func (s *cliPipeTransReaderWriter) Read(ctx context.Context, conn net.Conn, recvMsg remote.Message) (nctx context.Context, err error) {
	ctx, err = s.TransReadWriter.Read(ctx, conn, recvMsg)
	if err != nil {
		return ctx, err
	}
	return s.pipe.OnStreamRead(ctx, nil, recvMsg)
}

var _ streaming.ClientStream = (*cliStream)(nil)

var (
	cliStreamPool = &sync.Pool{
		New: func() interface{} {
			return &cliStream{}
		},
	}
	emptyCliStream = cliStream{}
)

type cliStream struct {
	conn net.Conn
	t    *cliTransHandler
	rw   cliPipeTransReaderWriter
	ctx  context.Context
	ri   rpcinfo.RPCInfo
}

func (s *cliStream) RecvMsg(ctx context.Context, resp interface{}) (err error) {
	ri := s.ri
	recvMsg := remote.NewMessage(resp, s.t.opt.SvcInfo, ri, remote.Reply, remote.Client)
	defer func() {
		remote.RecycleMessage(recvMsg)
	}()
	_, err = s.rw.Read(ctx, s.conn, recvMsg)
	return err
}

func (s *cliStream) SendMsg(ctx context.Context, req interface{}) (err error) {
	ri := s.ri
	methodName := ri.Invocation().MethodName()
	svcInfo := s.t.opt.SvcInfo
	m := svcInfo.MethodInfo(methodName)
	var sendMsg remote.Message
	if m == nil {
		return fmt.Errorf("method info is nil, methodName=%s, serviceInfo=%+v", methodName, svcInfo)
	} else if m.OneWay() {
		sendMsg = remote.NewMessage(req, svcInfo, ri, remote.Oneway, remote.Client)
	} else {
		sendMsg = remote.NewMessage(req, svcInfo, ri, remote.Call, remote.Client)
	}
	defer func() {
		remote.RecycleMessage(sendMsg)
	}()
	_, err = s.rw.Write(ctx, s.conn, sendMsg)
	return err
}

func (s *cliStream) Recycle() {
	*s = emptyCliStream
	cliStreamPool.Put(s)
}

func (s *cliStream) Header() (streaming.Header, error) {
	return nil, nil
}

func (s *cliStream) Trailer() (streaming.Trailer, error) {
	return nil, nil
}

func (s *cliStream) CloseSend(ctx context.Context) error {
	return nil
}

func (s *cliStream) Context() context.Context {
	return s.ctx
}

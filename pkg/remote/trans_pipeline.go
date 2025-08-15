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
)

// BoundHandler is used to abstract the bound handler.
type BoundHandler interface{}

// OutboundHandler is used to process write event.
type OutboundHandler interface {
	BoundHandler
	Write(ctx context.Context, conn net.Conn, send Message) (context.Context, error)
}

// InboundHandler is used to process read event.
type InboundHandler interface {
	BoundHandler
	OnActive(ctx context.Context, conn net.Conn) (context.Context, error)
	OnInactive(ctx context.Context, conn net.Conn) context.Context
	OnRead(ctx context.Context, conn net.Conn) (context.Context, error)
	OnMessage(ctx context.Context, args, result Message) (context.Context, error)
}

// DuplexBoundHandler can process both inbound and outbound connections.
type DuplexBoundHandler interface {
	OutboundHandler
	InboundHandler
}

// TransPipeline contains TransHandlers.
type TransPipeline struct {
	inboundHdrls  []InboundHandler
	outboundHdrls []OutboundHandler
}

type CliTransPipeline struct {
	TransPipeline
	cliNetHdlr ClientTransHandler
}

type SvrTransPipeline struct {
	TransPipeline
	svrNetHdlr ServerTransHandler
}

var (
	_ ClientTransHandler = &CliTransPipeline{}
	_ ServerTransHandler = &SvrTransPipeline{}
)

// NewCliTransPipeline is used to create a new TransPipeline for client.
// Only used for ping pong handlers.
func NewCliTransPipeline(netHdlr ClientTransHandler) *CliTransPipeline {
	transPl := &CliTransPipeline{}
	transPl.cliNetHdlr = netHdlr
	return transPl
}

// NewSvrTransPipeline is used to create a new TransPipeline for server.
// Only used for ping pong handlers.
func NewSvrTransPipeline(netHdlr ServerTransHandler) *SvrTransPipeline {
	transPl := &SvrTransPipeline{}
	transPl.svrNetHdlr = netHdlr
	netHdlr.SetPipeline(transPl)
	return transPl
}

// AddInboundHandler adds an InboundHandler to the pipeline.
func (p *TransPipeline) AddInboundHandler(hdlr InboundHandler) *TransPipeline {
	p.inboundHdrls = append(p.inboundHdrls, hdlr)
	return p
}

// AddOutboundHandler adds an OutboundHandler to the pipeline.
func (p *TransPipeline) AddOutboundHandler(hdlr OutboundHandler) *TransPipeline {
	p.outboundHdrls = append(p.outboundHdrls, hdlr)
	return p
}

// Write implements the OutboundHandler interface.
func (p *TransPipeline) Write(ctx context.Context, conn net.Conn, sendMsg Message) (nctx context.Context, err error) {
	for _, h := range p.outboundHdrls {
		ctx, err = h.Write(ctx, conn, sendMsg)
		if err != nil {
			return ctx, err
		}
	}
	return ctx, err
}

func (p *TransPipeline) OnMessage(ctx context.Context, args, result Message) (context.Context, error) {
	var err error
	for _, h := range p.inboundHdrls {
		ctx, err = h.OnMessage(ctx, args, result)
		if err != nil {
			return ctx, err
		}
	}
	if result.MessageType() == Exception {
		return ctx, nil
	}
	return ctx, err
}

func (p *CliTransPipeline) Write(ctx context.Context, conn net.Conn, sendMsg Message) (nctx context.Context, err error) {
	if ctx, err = p.TransPipeline.Write(ctx, conn, sendMsg); err != nil {
		return ctx, err
	}
	return p.cliNetHdlr.Write(ctx, conn, sendMsg)
}

// Read reads from conn.
func (p *CliTransPipeline) Read(ctx context.Context, conn net.Conn, msg Message) (nctx context.Context, err error) {
	return p.cliNetHdlr.Read(ctx, conn, msg)
}

func (p *CliTransPipeline) OnMessage(ctx context.Context, args, result Message) (nctx context.Context, err error) {
	if ctx, err = p.TransPipeline.OnMessage(ctx, args, result); err != nil {
		return ctx, err
	}
	return p.cliNetHdlr.OnMessage(ctx, args, result)
}

// OnError calls
func (p *CliTransPipeline) OnError(ctx context.Context, err error, conn net.Conn) {
	p.cliNetHdlr.OnError(ctx, err, conn)
}

// OnActive implements the InboundHandler interface.
func (p *SvrTransPipeline) OnActive(ctx context.Context, conn net.Conn) (context.Context, error) {
	var err error
	for _, h := range p.inboundHdrls {
		ctx, err = h.OnActive(ctx, conn)
		if err != nil {
			return ctx, err
		}
	}
	return p.svrNetHdlr.OnActive(ctx, conn)
}

// OnInactive implements the InboundHandler interface.
func (p *SvrTransPipeline) OnInactive(ctx context.Context, conn net.Conn) {
	for _, h := range p.inboundHdrls {
		ctx = h.OnInactive(ctx, conn)
	}
	p.svrNetHdlr.OnInactive(ctx, conn)
}

// OnRead implements the InboundHandler interface.
func (p *SvrTransPipeline) OnRead(ctx context.Context, conn net.Conn) error {
	var err error
	for _, h := range p.inboundHdrls {
		ctx, err = h.OnRead(ctx, conn)
		if err != nil {
			return err
		}
	}
	return p.svrNetHdlr.OnRead(ctx, conn)
}

func (p *SvrTransPipeline) Write(ctx context.Context, conn net.Conn, sendMsg Message) (nctx context.Context, err error) {
	if ctx, err = p.TransPipeline.Write(ctx, conn, sendMsg); err != nil {
		return ctx, err
	}
	return p.svrNetHdlr.Write(ctx, conn, sendMsg)
}

// Read reads from conn.
func (p *SvrTransPipeline) Read(ctx context.Context, conn net.Conn, msg Message) (nctx context.Context, err error) {
	return p.svrNetHdlr.Read(ctx, conn, msg)
}

// OnError calls
func (p *SvrTransPipeline) OnError(ctx context.Context, err error, conn net.Conn) {
	p.svrNetHdlr.OnError(ctx, err, conn)
}

// OnMessage implements the InboundHandler interface.
func (p *SvrTransPipeline) OnMessage(ctx context.Context, args, result Message) (nctx context.Context, err error) {
	if ctx, err = p.TransPipeline.OnMessage(ctx, args, result); err != nil {
		return ctx, err
	}
	return p.svrNetHdlr.OnMessage(ctx, args, result)
}

func (p *SvrTransPipeline) SetInvokeHandleFunc(inkHdlFunc endpoint.Endpoint) {
	p.svrNetHdlr.SetInvokeHandleFunc(inkHdlFunc)
}

// SetPipeline does nothing now.
func (p *SvrTransPipeline) SetPipeline(transPipe *SvrTransPipeline) {
	// do nothing
}

// GracefulShutdown implements the GracefulShutdown interface.
func (p *SvrTransPipeline) GracefulShutdown(ctx context.Context) error {
	if g, ok := p.svrNetHdlr.(GracefulShutdown); ok {
		return g.GracefulShutdown(ctx)
	}
	return nil
}

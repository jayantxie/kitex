package http1

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"sync"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/network"
	"github.com/cloudwego/hertz/pkg/protocol/http1"
	"github.com/cloudwego/netpoll"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/remote"
)

type svrTransHandlerFactory struct{}

// NewSvrTransHandlerFactory ...
func NewSvrTransHandlerFactory() remote.ServerTransHandlerFactory {
	return &svrTransHandlerFactory{}
}

func (f *svrTransHandlerFactory) NewTransHandler(opt *remote.ServerOption) (remote.ServerTransHandler, error) {
	return newSvrTransHandler(opt)
}

func newSvrTransHandler(opt *remote.ServerOption) (*svrTransHandler, error) {
	svr := initHertz(opt)
	return &svrTransHandler{
		opt:         opt,
		svcSearcher: opt.SvcSearcher,
		svr:         svr,
	}, nil
}

var _ remote.ServerTransHandler = &svrTransHandler{}

type svrTransHandler struct {
	opt         *remote.ServerOption
	svcSearcher remote.ServiceSearcher
	inkHdlFunc  endpoint.Endpoint
	codec       remote.Codec
	svr         *http1.Server
}

var httpReg = regexp.MustCompile(`^(?:GET |POST|PUT|DELE|HEAD|OPTI|CONN|TRAC|PATC)$`)

func (t *svrTransHandler) ProtocolMatch(ctx context.Context, conn net.Conn) error {
	c, ok := conn.(netpoll.Connection)
	if ok {
		pre, _ := c.Reader().Peek(4)
		if httpReg.Match(pre) {
			return nil
		}
	}
	return errors.New("error protocol not match")
}

func (t *svrTransHandler) OnRead(ctx context.Context, conn net.Conn) error {
	err := t.svr.Serve(ctx, conn.(network.Conn))
	if err != nil {
		err = errors.New(fmt.Sprintf("HERTZ: %s", err.Error()))
	}
	return err
}

func (t *svrTransHandler) Write(ctx context.Context, conn net.Conn, send remote.Message) (nctx context.Context, err error) {
	return ctx, nil
}
func (t *svrTransHandler) Read(ctx context.Context, conn net.Conn, msg remote.Message) (nctx context.Context, err error) {
	return ctx, nil
}

func (t *svrTransHandler) OnActive(ctx context.Context, conn net.Conn) (context.Context, error) {
	return ctx, nil
}

func (t *svrTransHandler) OnInactive(ctx context.Context, conn net.Conn) {

}
func (t *svrTransHandler) OnError(ctx context.Context, err error, conn net.Conn) {

}
func (t *svrTransHandler) OnMessage(ctx context.Context, args, result remote.Message) (context.Context, error) {
	return ctx, nil
}
func (t *svrTransHandler) SetPipeline(pipeline *remote.TransPipeline) {

}

func (t *svrTransHandler) SetInvokeHandleFunc(inkHdlFunc endpoint.Endpoint) {
	t.svr.Core.(*core).inkHdlFunc = inkHdlFunc
}

func initHertz(opt *remote.ServerOption) (svr *http1.Server) {
	svr = http1.NewServer()
	svr.Core = &core{
		opt: opt,
		pool: &sync.Pool{New: func() interface{} {
			return &app.RequestContext{}
		}},
	}
	return
}

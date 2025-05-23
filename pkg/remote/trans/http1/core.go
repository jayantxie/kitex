package http1

import (
	"fmt"
	"context"
	"io"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server/binding"
	"github.com/cloudwego/hertz/pkg/common/tracer"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/cloudwego/kitex/transport"
)

type core struct {
	opt        *remote.ServerOption
	pool       *sync.Pool
	inkHdlFunc endpoint.Endpoint
}

func (c *core) IsRunning() bool {
	return true
}

func (c *core) GetCtxPool() *sync.Pool {
	return c.pool
}

func (c *core) ServeHTTP(ctx context.Context, rctx *app.RequestContext) {
	// new rpcinfo if reuse is disabled
	ri := c.opt.InitOrResetRPCInfoFunc(nil, rctx.GetConn().RemoteAddr())
	ctx = rpcinfo.NewCtxWithRPCInfo(ctx, ri)

	ink := ri.Invocation().(rpcinfo.InvocationSetter)

	rPath := string(rctx.Request.URI().Path())
	paths := strings.Split(rPath, "/")
	if len(paths) != 4 {
		rctx.JSON(404, NewResponse(404, "not found", nil))
		return
	}
	serviceName, methodName := paths[2], paths[3]
	ink.SetServiceName(serviceName)
	ink.SetMethodName(methodName)
	if mutableTo := rpcinfo.AsMutableEndpointInfo(ri.To()); mutableTo != nil {
		_ = mutableTo.SetMethod(methodName)
	}

	// set grpc transport flag before execute metahandler
	rpcinfo.AsMutableRPCConfig(ri.Config()).SetTransportProtocol(transport.HTTP)

	var err error
	ctx = c.startTracer(ctx, ri)
	defer func() {
		panicErr := recover()
		if panicErr != nil {
			if rctx.GetConn() != nil {
				klog.CtxErrorf(ctx, "KITEX: http panic happened, close conn, remoteAddress=%s, error=%s\nstack=%s", rctx.GetConn().RemoteAddr(), panicErr, string(debug.Stack()))
			} else {
				klog.CtxErrorf(ctx, "KITEX: http panic happened, error=%v\nstack=%s", panicErr, string(debug.Stack()))
			}
			rctx.JSON(500, NewResponse(500, fmt.Sprintf("http server handler panic, err: %v, stack: %v", panicErr, string(debug.Stack())), nil))
		}
		c.finishTracer(ctx, ri, err, panicErr)
	}()

	svcInfo := c.opt.SvcSearcher.SearchService(serviceName, methodName, true)
	var methodInfo serviceinfo.MethodInfo
	if svcInfo != nil {
		methodInfo = svcInfo.MethodInfo(methodName)
	}

	realArgs, realResp := methodInfo.NewArgs(), methodInfo.NewResult()
	req := realArgs.(utils.KitexArgs).GetFirstArgument()
	reqv := reflect.New(reflect.TypeOf(req).Elem())
	req = reqv.Interface()
	err = rctx.BindJSON(req)
	if err != nil && err != io.EOF {
		rctx.JSON(400, NewResponse(400, "bind json error", nil))
		return
	}
	err = binding.Bind(&rctx.Request, req, nil)
	if err != nil {
		rctx.JSON(400, NewResponse(400, "bind params error", nil))
		return
	}
	reflect.ValueOf(realArgs).Elem().Field(0).Set(reqv)
	err = c.inkHdlFunc(ctx, realArgs, realResp)
	if err != nil {
		rctx.JSON(500, NewResponse(500, err.Error(), nil))
		return
	}
	if bizErr := ri.Invocation().BizStatusErr(); bizErr != nil {
		rctx.JSON(200, NewResponse(int(bizErr.BizStatusCode()), bizErr.BizMessage(), nil))
		return
	}
	rctx.JSON(200, NewResponse(0, "success", realResp.(utils.KitexResult).GetResult()))
}

func (c *core) GetTracer() tracer.Controller {
	return &mockController{}
}

func (t *core) startTracer(ctx context.Context, ri rpcinfo.RPCInfo) context.Context {
	c := t.opt.TracerCtl.DoStart(ctx, ri)
	return c
}

func (t *core) finishTracer(ctx context.Context, ri rpcinfo.RPCInfo, err error, panicErr interface{}) {
	rpcStats := rpcinfo.AsMutableRPCStats(ri.Stats())
	if rpcStats == nil {
		return
	}
	if panicErr != nil {
		rpcStats.SetPanicked(panicErr)
	}
	t.opt.TracerCtl.DoFinish(ctx, ri, err)
	rpcStats.Reset()
}

type mockController struct {
	FinishTimes int
}

func (m *mockController) Append(col tracer.Tracer) {}

func (m *mockController) DoStart(ctx context.Context, c *app.RequestContext) context.Context {
	return ctx
}

func (m *mockController) DoFinish(ctx context.Context, c *app.RequestContext, err error) {
	m.FinishTimes++
}

func (m *mockController) HasTracer() bool { return true }

type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

func NewResponse(code int, message string, data interface{}) *Response {
	return &Response{
		Code:    code,
		Message: message,
		Data:    data,
	}
}

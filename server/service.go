/*
 * Copyright 2023 CloudWeGo Authors
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

package server

import (
	"context"
	"errors"
	"fmt"
	"sync"

	igeneric "github.com/cloudwego/kitex/internal/generic"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/generic"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
)

var (
	notAllowBinaryGenericCtx = igeneric.WithGenericStreamingMode(context.Background(), serviceinfo.StreamingMode(-1))
)

type serviceMiddlewares struct {
	MW endpoint.Middleware
}

type service struct {
	svcInfo              *serviceinfo.ServiceInfo
	handler              interface{}
	unknownMethodHandler interface{}
	serviceMiddlewares
}

func newService(svcInfo *serviceinfo.ServiceInfo, handler interface{}, smw serviceMiddlewares) *service {
	return &service{svcInfo: svcInfo, handler: handler, serviceMiddlewares: smw}
}

func (s *service) getHandler(methodName string) interface{} {
	if s.unknownMethodHandler == nil {
		return s.handler
	}
	if s.svcInfo.Methods[methodName] != nil {
		return s.handler
	}
	return s.unknownMethodHandler
}

type unknownService struct {
	mutex   sync.RWMutex
	svcs    map[string]*service
	handler interface{}
}

func (u *unknownService) getSvc(svcName string) *service {
	u.mutex.RLock()
	svc := u.svcs[svcName]
	u.mutex.RUnlock()
	return svc
}

func (u *unknownService) getOrStoreSvc(svcName string, codecType serviceinfo.PayloadCodec) *service {
	u.mutex.RLock()
	svc, ok := u.svcs[svcName]
	u.mutex.RUnlock()
	if ok {
		return svc
	}
	u.mutex.Lock()
	defer u.mutex.Unlock()
	svc, ok = u.svcs[svcName]
	if ok {
		return svc
	}
	var g generic.Generic
	switch codecType {
	case serviceinfo.Thrift:
		g = generic.BinaryThriftGenericV2(svcName)
	case serviceinfo.Protobuf:
		g = generic.BinaryPbGeneric(svcName, "")
	default:
		u.svcs[svcName] = nil
		return nil
	}
	svc = &service{
		svcInfo: generic.ServiceInfoWithGeneric(g),
		handler: u.handler,
	}
	u.svcs[svcName] = svc
	return svc
}

type services struct {
	methodSvcsMap map[string][]*service // key: method name, only effective to generated code
	knownSvcMap   map[string]*service   // key: service name
	fallbackSvc   *service
	unknownSvc    *unknownService

	// be compatible with binary thrift generic v1
	binaryThriftGenericV1SvcInfo *serviceinfo.ServiceInfo

	refuseTrafficWithoutServiceName bool
}

func newServices() *services {
	return &services{
		methodSvcsMap: map[string][]*service{},
		knownSvcMap:   map[string]*service{},
	}
}

func (s *services) addService(svcInfo *serviceinfo.ServiceInfo, handler interface{}, registerOpts *RegisterOptions) error {
	// prepare serviceMiddlewares
	var serviceMWs serviceMiddlewares
	if len(registerOpts.Middlewares) > 0 {
		serviceMWs.MW = endpoint.Chain(registerOpts.Middlewares...)
	}

	// unknown service
	if registerOpts.IsUnknownService {
		if s.unknownSvc != nil {
			return errors.New("multiple unknown services cannot be registered")
		}
		s.unknownSvc = &unknownService{svcs: map[string]*service{}, handler: handler}
		return nil
	}

	svc := newService(svcInfo, handler, serviceMWs)
	if registerOpts.IsFallbackService {
		if s.fallbackSvc != nil {
			return fmt.Errorf("multiple fallback services cannot be registered. [%s] is already registered as a fallback service", s.fallbackSvc.svcInfo.ServiceName)
		}
		s.fallbackSvc = svc
	}
	if _, ok := s.knownSvcMap[svcInfo.ServiceName]; ok {
		return fmt.Errorf("service [%s] has already been registered", svcInfo.ServiceName)
	}
	s.knownSvcMap[svcInfo.ServiceName] = svc
	// method search map
	for methodName := range svcInfo.Methods {
		svcs := s.methodSvcsMap[methodName]
		if registerOpts.IsFallbackService {
			svcs = append([]*service{svc}, svcs...)
		} else {
			svcs = append(svcs, svc)
		}
		s.methodSvcsMap[methodName] = svcs
	}
	return nil
}

func (s *services) getKnownSvcInfoMap() map[string]*serviceinfo.ServiceInfo {
	svcInfoMap := map[string]*serviceinfo.ServiceInfo{}
	for name, svc := range s.knownSvcMap {
		svcInfoMap[name] = svc.svcInfo
	}
	return svcInfoMap
}

func (s *services) check(refuseTrafficWithoutServiceName bool) error {
	if !s.hasRegisteredService() {
		return errors.New("run: no service. Use RegisterService to set one")
	}
	for _, svc := range s.knownSvcMap {
		// special treatment for binary thrift generic v1
		if generic.IsBinaryThriftGenericV1(svc.svcInfo) {
			s.binaryThriftGenericV1SvcInfo = svc.svcInfo
			break
		}
	}
	if s.unknownSvc != nil {
		for _, svc := range s.knownSvcMap {
			svc.svcInfo = generic.RegisterBinaryGenericMethodFunc(svc.svcInfo)
			svc.unknownMethodHandler = s.unknownSvc.handler
		}
	}
	if refuseTrafficWithoutServiceName {
		s.refuseTrafficWithoutServiceName = true
		return nil
	}
	for name, svcs := range s.methodSvcsMap {
		if len(svcs) > 1 && svcs[0] != s.fallbackSvc {
			return fmt.Errorf("method name [%s] is conflicted between services but no fallback service is specified", name)
		}
	}
	return nil
}

func (s *services) getService(svcName string) *service {
	if svc := s.knownSvcMap[svcName]; svc != nil {
		return svc
	}
	if s.unknownSvc != nil {
		return s.unknownSvc.getSvc(svcName)
	}
	return nil
}

func (s *services) SearchService(svcName, methodName string, strict bool, codecType serviceinfo.PayloadCodec) *serviceinfo.ServiceInfo {
	if strict {
		if svc := s.knownSvcMap[svcName]; svc != nil {
			return svc.svcInfo
		}
		if s.unknownSvc != nil {
			return s.unknownSvc.getOrStoreSvc(svcName, codecType).svcInfo
		}
		return nil
	}
	if s.binaryThriftGenericV1SvcInfo != nil {
		// be compatible with binary thrift generic
		return s.binaryThriftGenericV1SvcInfo
	}
	if s.refuseTrafficWithoutServiceName {
		if svc := s.knownSvcMap[svcName]; svc != nil {
			return svc.svcInfo
		}
		if s.unknownSvc != nil {
			return s.unknownSvc.getOrStoreSvc(svcName, codecType).svcInfo
		}
		return nil
	}
	var svc *service
	if svcName == "" {
		if svcs := s.methodSvcsMap[methodName]; len(svcs) > 0 {
			svc = svcs[0]
		} else {
			// json/map generic don't expose methods map because they support dynamic updating,
			// so we can only use MethodInfo to search, and disable binary generic lookup to
			// prevent finding binary methods when the unknown handler is enabled.
			for _, _svc := range s.knownSvcMap {
				mi := _svc.svcInfo.MethodInfo(notAllowBinaryGenericCtx, methodName)
				if mi != nil {
					svc = _svc
					break
				}
			}
		}
	} else {
		svc = s.knownSvcMap[svcName]
		if svc == nil {
			// maybe combine or generic service name, TODO: remove this logic
			svcs := s.methodSvcsMap[methodName]
			if len(svcs) > 0 {
				if len(svcs) == 1 || svcName[0] == '$' {
					svc = svcs[0]
				}
			} else {
				for _, _svc := range s.knownSvcMap {
					mi := _svc.svcInfo.MethodInfo(notAllowBinaryGenericCtx, methodName)
					if mi != nil {
						svc = _svc
						break
					}
				}
			}
		}
	}
	if svc != nil {
		return svc.svcInfo
	}
	if s.unknownSvc != nil {
		return s.unknownSvc.getOrStoreSvc(svcName, codecType).svcInfo
	}
	return nil
}

func (s *services) hasRegisteredService() bool {
	return len(s.knownSvcMap) > 0 || s.unknownSvc != nil
}

// getTargetSvcInfo returns the service info if there is only one service registered.
func (s *services) getTargetSvcInfo() *serviceinfo.ServiceInfo {
	if len(s.knownSvcMap) != 1 {
		return nil
	}
	for _, svc := range s.knownSvcMap {
		return svc.svcInfo
	}
	return nil
}

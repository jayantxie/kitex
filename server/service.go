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
	// set generic streaming mode to -1, which means not allow binary generic fallback.
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
	knownSvcMap     map[string]*service // key: service name
	nonFallbackSvcs []*service

	fallbackSvc *service

	unknownSvc *unknownService

	// be compatible with binary thrift generic v1
	binaryThriftGenericV1SvcInfo *serviceinfo.ServiceInfo

	refuseTrafficWithoutServiceName bool
}

func newServices() *services {
	return &services{
		knownSvcMap: map[string]*service{},
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
	if !registerOpts.IsFallbackService {
		s.nonFallbackSvcs = append(s.nonFallbackSvcs, svc)
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
	if len(s.knownSvcMap) == 0 && s.unknownSvc == nil {
		return errors.New("run: no service. Use RegisterService to set one")
	}
	for _, svc := range s.knownSvcMap {
		// special treatment for binary thrift generic v1, it doesn't support multi services.
		if svc.svcInfo.ServiceName == serviceinfo.GenericService {
			s.binaryThriftGenericV1SvcInfo = svc.svcInfo
			if len(s.knownSvcMap) > 1 {
				return fmt.Errorf("binary thrift generic v1 doesn't support multi services")
			}
			if s.unknownSvc != nil {
				return fmt.Errorf("binary thrift generic v1 doesn't support unknown service")
			}
			return nil
		}
	}
	if s.unknownSvc != nil {
		for _, svc := range s.knownSvcMap {
			// register binary fallback method for every service info
			svc.svcInfo = generic.RegisterBinaryGenericMethodFunc(svc.svcInfo)
			svc.unknownMethodHandler = s.unknownSvc.handler
		}
	}
	if refuseTrafficWithoutServiceName {
		s.refuseTrafficWithoutServiceName = true
		return nil
	}
	// checking whether conflicting methods have fallback service
	fallbackCheckingMap := make(map[string]int)
	for _, svc := range s.knownSvcMap {
		for method := range svc.svcInfo.Methods {
			if svc == s.fallbackSvc {
				fallbackCheckingMap[method] = -1
			} else if num := fallbackCheckingMap[method]; num >= 0 {
				fallbackCheckingMap[method] = num + 1
			}
		}
	}
	for methodName, serviceNum := range fallbackCheckingMap {
		if serviceNum > 1 {
			return fmt.Errorf("method name [%s] is conflicted between services but no fallback service is specified", methodName)
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

func (s *services) searchByMethodName(methodName string) *serviceinfo.ServiceInfo {
	if s.fallbackSvc != nil {
		// check whether fallback service has the method
		if mi := s.fallbackSvc.svcInfo.MethodInfo(notAllowBinaryGenericCtx, methodName); mi != nil {
			return s.fallbackSvc.svcInfo
		}
	}
	// check other services
	for _, svc := range s.nonFallbackSvcs {
		if mi := svc.svcInfo.MethodInfo(notAllowBinaryGenericCtx, methodName); mi != nil {
			return svc.svcInfo
		}
	}
	return nil
}

func (s *services) SearchService(svcName, methodName string, strict bool, codecType serviceinfo.PayloadCodec) *serviceinfo.ServiceInfo {
	if s.binaryThriftGenericV1SvcInfo != nil {
		// be compatible with binary thrift generic
		return s.binaryThriftGenericV1SvcInfo
	}
	if strict || s.refuseTrafficWithoutServiceName {
		if svc := s.knownSvcMap[svcName]; svc != nil {
			return svc.svcInfo
		}
	} else {
		if svcName == "" {
			// for non ttheader traffic, service name might be empty, we must fall back to method searching.
			if svcInfo := s.searchByMethodName(methodName); svcInfo != nil {
				return svcInfo
			}
		} else {
			if svc := s.knownSvcMap[svcName]; svc != nil {
				return svc.svcInfo
			}
			if svcName == serviceinfo.GenericService || svcName == serviceinfo.CombineService {
				// Maybe combine or generic service name,
				// because Kitex client will write these two service name if the version is between v0.9.0-v0.9.1
				// TODO: remove this logic if this version range is converged
				if svcInfo := s.searchByMethodName(methodName); svcInfo != nil {
					return svcInfo
				}
			}
		}
	}
	if s.unknownSvc != nil {
		return s.unknownSvc.getOrStoreSvc(svcName, codecType).svcInfo
	}
	return nil
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

/*
 * Copyright 2024 CloudWeGo Authors
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
	"testing"

	"github.com/cloudwego/kitex/internal/mocks"
	"github.com/cloudwego/kitex/internal/test"
	"github.com/cloudwego/kitex/pkg/generic"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
)

func TestSearchService(t *testing.T) {
	type svc struct {
		svcInfo           *serviceinfo.ServiceInfo
		isFallbackService bool
		isUnknownService  bool
	}
	testcases := []struct {
		svcs                            []svc
		refuseTrafficWithoutServiceName bool

		serviceName, methodName string
		strict                  bool
		expectSvcInfo           *serviceinfo.ServiceInfo
	}{
		{
			svcs: []svc{
				{
					svcInfo:           mocks.ServiceInfo(),
					isFallbackService: false,
				},
				{
					svcInfo:           mocks.Service3Info(),
					isFallbackService: true,
				},
			},
			serviceName:   mocks.MockServiceName,
			methodName:    mocks.MockMethod,
			expectSvcInfo: mocks.ServiceInfo(),
		},
		{
			svcs: []svc{
				{
					svcInfo:           mocks.ServiceInfo(),
					isFallbackService: false,
				},
				{
					svcInfo:           mocks.Service3Info(),
					isFallbackService: true,
				},
			},
			serviceName:   "",
			methodName:    mocks.MockMethod,
			expectSvcInfo: mocks.Service3Info(),
		},
		{
			svcs: []svc{
				{
					svcInfo:           mocks.ServiceInfo(),
					isFallbackService: true,
				},
				{
					svcInfo:           mocks.Service3Info(),
					isFallbackService: false,
				},
			},
			serviceName:   mocks.MockService3Name,
			methodName:    mocks.MockMethod,
			expectSvcInfo: mocks.Service3Info(),
		},
		{
			svcs: []svc{
				{
					svcInfo:           mocks.ServiceInfo(),
					isFallbackService: true,
				},
				{
					svcInfo:           mocks.Service3Info(),
					isFallbackService: false,
				},
			},
			serviceName:   "",
			methodName:    mocks.MockMethod,
			expectSvcInfo: mocks.ServiceInfo(),
		},
		{
			svcs: []svc{
				{
					svcInfo:           mocks.ServiceInfo(),
					isFallbackService: false,
				},
				{
					svcInfo:           mocks.Service3Info(),
					isFallbackService: false,
				},
			},
			refuseTrafficWithoutServiceName: true,
			serviceName:                     "",
			methodName:                      mocks.MockMethod,
			expectSvcInfo:                   nil,
		},
		{
			svcs: []svc{
				{
					svcInfo:           mocks.ServiceInfo(),
					isFallbackService: true,
				},
				{
					svcInfo:           mocks.Service3Info(),
					isFallbackService: false,
				},
			},
			serviceName:   serviceinfo.GenericService,
			methodName:    mocks.MockMethod,
			expectSvcInfo: mocks.ServiceInfo(),
		},
		{
			svcs: []svc{
				{
					svcInfo:           mocks.ServiceInfo(),
					isFallbackService: true,
				},
				{
					svcInfo:           mocks.Service3Info(),
					isFallbackService: false,
				},
			},
			serviceName:   "xxxxxx",
			methodName:    mocks.MockExceptionMethod,
			expectSvcInfo: nil,
		},
		{
			svcs: []svc{
				{
					svcInfo:           mocks.ServiceInfo(),
					isFallbackService: true,
				},
				{
					svcInfo:           mocks.Service3Info(),
					isFallbackService: false,
				},
			},
			serviceName:   serviceinfo.GenericService,
			methodName:    mocks.MockExceptionMethod,
			expectSvcInfo: mocks.ServiceInfo(),
		},
		{
			svcs: []svc{
				{
					svcInfo:           mocks.ServiceInfo(),
					isFallbackService: true,
				},
				{
					svcInfo:           mocks.Service3Info(),
					isFallbackService: false,
				},
			},
			serviceName:   serviceinfo.CombineService,
			methodName:    mocks.MockExceptionMethod,
			expectSvcInfo: mocks.ServiceInfo(),
		},
		{
			svcs: []svc{
				{
					svcInfo:           mocks.ServiceInfo(),
					isFallbackService: true,
				},
				{
					svcInfo:           mocks.Service3Info(),
					isFallbackService: false,
				},
			},
			serviceName:   "xxxxxx",
			methodName:    mocks.MockMethod,
			expectSvcInfo: nil,
		},
		{
			svcs: []svc{
				{
					svcInfo: generic.ServiceInfoWithGeneric(generic.BinaryThriftGeneric()),
				},
			},
			serviceName:   "xxxxxx",
			methodName:    mocks.MockMethod,
			expectSvcInfo: generic.ServiceInfoWithGeneric(generic.BinaryThriftGeneric()),
		},
		{
			svcs: []svc{
				{
					svcInfo: mocks.ServiceInfo(),
				},
				{
					isUnknownService: true,
				},
			},
			serviceName:   mocks.MockServiceName,
			methodName:    mocks.MockMethod,
			expectSvcInfo: mocks.ServiceInfo(),
		},
		{
			svcs: []svc{
				{
					svcInfo: mocks.ServiceInfo(),
				},
				{
					svcInfo: mocks.Service2Info(),
				},
			},
			strict:        true,
			serviceName:   mocks.MockServiceName,
			methodName:    mocks.MockMethod,
			expectSvcInfo: mocks.ServiceInfo(),
		},
		{
			svcs: []svc{
				{
					svcInfo: mocks.ServiceInfo(),
				},
				{
					isUnknownService: true,
				},
			},
			strict:        true,
			serviceName:   "xxxxxx",
			methodName:    mocks.MockMethod,
			expectSvcInfo: &serviceinfo.ServiceInfo{ServiceName: "xxxxxx"},
		},
	}
	for i, tcase := range testcases {
		svcs := newServices()
		for _, svc := range tcase.svcs {
			svcs.addService(svc.svcInfo, mocks.MyServiceHandler(), &RegisterOptions{IsFallbackService: svc.isFallbackService, IsUnknownService: svc.isUnknownService})
		}
		test.Assert(t, svcs.check(tcase.refuseTrafficWithoutServiceName) == nil)
		svcInfo := svcs.SearchService(tcase.serviceName, tcase.methodName, tcase.strict, serviceinfo.Thrift)
		if tcase.expectSvcInfo == nil {
			test.Assert(t, svcInfo == nil, i)
		} else {
			test.Assert(t, svcInfo.ServiceName == tcase.expectSvcInfo.ServiceName, i)
		}
	}
}

func TestAddService(t *testing.T) {
	type svc struct {
		svcInfo           *serviceinfo.ServiceInfo
		isFallbackService bool
		isUnknownService  bool
	}
	testcases := []struct {
		svcs            []svc
		expectAddSvcErr bool
	}{
		{
			svcs: []svc{
				{
					isUnknownService: true,
				},
				{
					isUnknownService: true,
				},
			},
			expectAddSvcErr: true,
		},
		{
			svcs: []svc{
				{
					svcInfo:           mocks.ServiceInfo(),
					isFallbackService: true,
				},
				{
					svcInfo:           mocks.Service3Info(),
					isFallbackService: true,
				},
			},
			expectAddSvcErr: true,
		},
		{
			svcs: []svc{
				{
					svcInfo:           mocks.ServiceInfo(),
					isFallbackService: true,
				},
				{
					svcInfo: mocks.ServiceInfo(),
				},
			},
			expectAddSvcErr: true,
		},
	}
	for i, tcase := range testcases {
		svcs := newServices()
		var hasAddErr bool
		for _, svc := range tcase.svcs {
			err := svcs.addService(svc.svcInfo, mocks.MyServiceHandler(), &RegisterOptions{IsFallbackService: svc.isFallbackService, IsUnknownService: svc.isUnknownService})
			if err != nil {
				hasAddErr = true
			}
		}
		test.Assert(t, tcase.expectAddSvcErr == hasAddErr, i)
	}
}

func TestCheckService(t *testing.T) {
	type svc struct {
		svcInfo           *serviceinfo.ServiceInfo
		isFallbackService bool
		isUnknownService  bool
	}
	testcases := []struct {
		svcs                            []svc
		refuseTrafficWithoutServiceName bool
		expectCheckErr                  bool
	}{
		{
			svcs:           nil,
			expectCheckErr: true,
		},
		{
			svcs: []svc{
				{
					svcInfo:           mocks.ServiceInfo(),
					isFallbackService: false,
				},
				{
					svcInfo:           mocks.Service2Info(),
					isFallbackService: true,
				},
				{
					svcInfo:           mocks.Service3Info(),
					isFallbackService: false,
				},
			},
			expectCheckErr: true,
		},
		{
			svcs: []svc{
				{
					svcInfo:           mocks.ServiceInfo(),
					isFallbackService: false,
				},
				{
					svcInfo:           mocks.Service3Info(),
					isFallbackService: false,
				},
			},
			expectCheckErr: true,
		},
		{
			svcs: []svc{
				{
					svcInfo: generic.ServiceInfoWithGeneric(generic.BinaryThriftGeneric()),
				},
				{
					svcInfo: mocks.ServiceInfo(),
				},
			},
			expectCheckErr: true,
		},
		{
			svcs: []svc{
				{
					svcInfo: generic.ServiceInfoWithGeneric(generic.BinaryThriftGeneric()),
				},
				{
					isUnknownService: true,
				},
			},
			expectCheckErr: true,
		},
	}
	for i, tcase := range testcases {
		svcs := newServices()
		for _, svc := range tcase.svcs {
			svcs.addService(svc.svcInfo, mocks.MyServiceHandler(), &RegisterOptions{IsFallbackService: svc.isFallbackService, IsUnknownService: svc.isUnknownService})
		}
		test.Assert(t, svcs.check(tcase.refuseTrafficWithoutServiceName) != nil == tcase.expectCheckErr, i)
	}
}

// Copyright Red Hat, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ior

import (
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/kube"
	"istio.io/pkg/log"
)

// IORLog is IOR-scoped log
var IORLog = log.RegisterScope("ior", "IOR logging", 0)

type IOR struct {
	route
}

func Run(
	kubeClient kube.Client,
	store model.ConfigStoreCache,
	pilotNamespace string,
	stop <-chan struct{},
	errorChannel chan error,
) {
	IORLog.Info("setting up IOR")
	rc, err := newRouterClient()
	if err != nil {
		return
	}

	r, err := newRoute(NewKubeClient(kubeClient), rc, store, pilotNamespace, kubeClient.GetMemberRoll(), stop, errorChannel)
	if err != nil {
		return
	}

	r.Run(stop)
}

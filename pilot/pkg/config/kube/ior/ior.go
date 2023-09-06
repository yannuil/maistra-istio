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
	"istio.io/istio/pkg/log"
)

// iorLog is IOR-scoped log
var iorLog = log.RegisterScope("ior", "IOR controller log")

func Run(kubeClient kube.Client, store model.ConfigStoreController, stop <-chan struct{}) {
	iorLog.Info("setting up IOR")
	r := newRouteController(NewKubeClient(kubeClient), store)
	r.Run(stop)
}

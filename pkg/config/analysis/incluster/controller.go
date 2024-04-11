/*
 Copyright Istio Authors

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package incluster

import (
	"fmt"
	"strings"
	"time"

	v1alpha12 "istio.io/api/analysis/v1alpha1"
	"istio.io/api/meta/v1alpha1"
	"istio.io/istio/pilot/pkg/config/kube/crdclient"
	"istio.io/istio/pilot/pkg/features"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/status"
	"istio.io/istio/pkg/config"
	"istio.io/istio/pkg/config/analysis/analyzers"
	"istio.io/istio/pkg/config/analysis/diag"
	"istio.io/istio/pkg/config/analysis/local"
	"istio.io/istio/pkg/config/legacy/util/kuberesource"
	"istio.io/istio/pkg/config/resource"
	"istio.io/istio/pkg/config/schema/collection"
	"istio.io/istio/pkg/config/schema/collections"
	"istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/log"
	"istio.io/istio/pkg/util/concurrent"
	"istio.io/istio/pkg/util/sets"
)

// Controller manages repeatedly running analyzers in istiod, and reporting results
// via istio status fields.
type Controller struct {
	analyzer  *local.IstiodAnalyzer
	statusctl *status.Controller
}

func NewController(stop <-chan struct{}, rwConfigStore model.ConfigStoreController,
	kubeClient kube.Client, revision, namespace string, statusManager *status.Manager, domainSuffix string,
) (*Controller, error) {
	analyzer := analyzers.AllCombined()

	ia := local.NewIstiodAnalyzer(analyzer, "", resource.Namespace(namespace), func(name config.GroupVersionKind) {})
	ia.AddSource(rwConfigStore)

	schemas := kuberesource.ConvertInputsToSchemas(analyzer.Metadata().Inputs).
		Remove(collections.MeshConfig).  // this is not an actual resource
		Remove(collections.MeshNetworks) // this is not an actual resource
	if kubeClient.IsMultiTenant() {
		schemas = removeClusterScoped(schemas)
	}
	// Filter out configs watched by rwConfigStore so we don't watch multiple times
	schemas = schemas.Remove(rwConfigStore.Schemas().All()...)
	store := crdclient.NewForSchemas(kubeClient,
		crdclient.Option{
			Revision:     revision,
			DomainSuffix: domainSuffix,
			Identifier:   "analysis-controller",
			FiltersByGVK: ia.GetFiltersByGVK(),
		},
		schemas)
	ia.AddSource(store)
	kubeClient.RunAndWait(stop)
	err := ia.Init(stop)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize analysis controller, releasing lease: %s", err)
	}
	ctl := statusManager.CreateIstioStatusController(func(status *v1alpha1.IstioStatus, context any) *v1alpha1.IstioStatus {
		msgs := context.(diag.Messages)
		// zero out analysis messages, as this is the sole controller for those
		status.ValidationMessages = []*v1alpha12.AnalysisMessageBase{}
		for _, msg := range msgs {
			status.ValidationMessages = append(status.ValidationMessages, msg.AnalysisMessageBase())
		}
		return status
	})
	return &Controller{analyzer: ia, statusctl: ctl}, nil
}

func removeClusterScoped(schemas collection.Schemas) collection.Schemas {
	b := collection.NewSchemasBuilder()
	for _, s := range schemas.All() {
		if !s.IsClusterScoped() {
			b.MustAdd(s)
		}
	}
	return b.Build()
}

// Run is blocking
func (c *Controller) Run(stop <-chan struct{}) {
	db := concurrent.Debouncer[config.GroupVersionKind]{}
	chKind := make(chan config.GroupVersionKind, 10)

	for _, k := range c.analyzer.Schemas().All() {
		c.analyzer.RegisterEventHandler(k.GroupVersionKind(), func(oldcfg config.Config, newcfg config.Config, ev model.Event) {
			gvk := oldcfg.GroupVersionKind
			if (gvk == config.GroupVersionKind{}) {
				gvk = newcfg.GroupVersionKind
			}
			chKind <- gvk
		})
	}
	oldmsgs := map[string]diag.Messages{}
	pushFn := func(combinedKinds sets.Set[config.GroupVersionKind]) {
		res, err := c.analyzer.ReAnalyzeSubset(combinedKinds, stop)
		if err != nil {
			log.Errorf("In-cluster analysis has failed: %s", err)
			return
		}
		// reorganize messages to map
		index := map[status.Resource]diag.Messages{}
		for _, m := range res.Messages {
			key := status.ResourceFromMetadata(m.Resource.Metadata)
			index[key] = append(index[key], m)
		}
		// if we previously had a message that has been removed, ensure it is removed
		// TODO: this creates a state destruction problem when istiod crashes
		// in that old messages may not be removed.  Not sure how to fix this
		// other than write every object's status every loop.
		for _, a := range res.ExecutedAnalyzers {
			for _, m := range oldmsgs[a] {
				key := status.ResourceFromMetadata(m.Resource.Metadata)
				if _, ok := index[key]; !ok {
					index[key] = diag.Messages{}
				}
			}
			for r, m := range index {
				// don't try to write status for non-istio types
				if strings.HasSuffix(r.Group, "istio.io") {
					log.Debugf("enqueueing update for %s/%s", r.Namespace, r.Name)
					c.statusctl.EnqueueStatusUpdateResource(m, r)
				}
			}
			oldmsgs[a] = res.MappedMessages[a]
		}
		log.Debugf("finished enqueueing all statuses")
	}
	db.Run(chKind, stop, 1*time.Second, features.AnalysisInterval, pushFn)
}

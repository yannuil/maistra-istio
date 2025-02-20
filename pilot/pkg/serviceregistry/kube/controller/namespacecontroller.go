// Copyright Istio Authors
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

package controller

import (
	"fmt"
	"time"

	xnsinformers "github.com/maistra/xns-informer/pkg/informers"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	listerv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"istio.io/istio/pilot/pkg/keycertbundle"
	"istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/kube/inject"
	"istio.io/istio/security/pkg/k8s"
)

const (
	// CACertNamespaceConfigMap is the name of the ConfigMap in each namespace storing the root cert of non-Kube CA.
	CACertNamespaceConfigMap = "istio-ca-root-cert"
)

var configMapLabel = map[string]string{"istio.io/config": "true"}

// NamespaceController manages reconciles a configmap in each namespace with a desired set of data.
type NamespaceController struct {
	client          corev1.CoreV1Interface
	caBundleWatcher *keycertbundle.Watcher

	queue              workqueue.RateLimitingInterface
	namespacesInformer cache.SharedInformer
	configMapInformer  cache.SharedInformer
	namespaceLister    listerv1.NamespaceLister
	configmapLister    listerv1.ConfigMapLister

	usesMemberRollController bool
	namespaces               xnsinformers.NamespaceSet
}

// NewNamespaceController returns a pointer to a newly constructed NamespaceController instance.
func NewNamespaceController(kubeClient kube.Client, caBundleWatcher *keycertbundle.Watcher) *NamespaceController {
	c := &NamespaceController{
		client:          kubeClient.CoreV1(),
		caBundleWatcher: caBundleWatcher,
		queue:           workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
	}

	c.configMapInformer = kubeClient.KubeInformer().Core().V1().ConfigMaps().Informer()
	c.configmapLister = kubeClient.KubeInformer().Core().V1().ConfigMaps().Lister()

	c.configMapInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(_, obj interface{}) {
			c.configMapChange(obj)
		},
		DeleteFunc: func(obj interface{}) {
			c.configMapChange(obj)
		},
	})

	// If a MemberRoll controller is configured on the client, skip creating the
	// namespace informer and just respond to changes in the MemberRoll.
	if mrc := kubeClient.GetMemberRoll(); mrc != nil {
		c.usesMemberRollController = true
		c.namespaces = xnsinformers.NewNamespaceSet()
		c.namespaces.AddHandler(xnsinformers.NamespaceSetHandlerFuncs{
			AddFunc: func(ns string) {
				if err := c.insertDataForNamespace(ns); err != nil {
					log.Errorf("error inserting data for namespace: %v", err)
				}
			},
		})

		mrc.Register(c.namespaces, "namespace-controller")
		return c
	}

	c.namespaceLister = kubeClient.KubeInformer().Core().V1().Namespaces().Lister()
	c.namespacesInformer = kubeClient.KubeInformer().Core().V1().Namespaces().Informer()
	c.namespacesInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.namespaceChange(obj.(*v1.Namespace))
		},
		UpdateFunc: func(_, obj interface{}) {
			c.namespaceChange(obj.(*v1.Namespace))
		},
	})

	return c
}

// Run starts the NamespaceController until a value is sent to stopCh.
func (nc *NamespaceController) Run(stopCh <-chan struct{}) {
	defer nc.queue.ShutDown()

	syncFuncs := []cache.InformerSynced{nc.configMapInformer.HasSynced}
	if nc.namespacesInformer != nil {
		syncFuncs = append(syncFuncs, nc.namespacesInformer.HasSynced)
	}

	if !cache.WaitForCacheSync(stopCh, syncFuncs...) {
		log.Error("Failed to sync namespace controller cache")
		return
	}
	log.Infof("Namespace controller started")
	go wait.Until(nc.runWorker, time.Second, stopCh)
	go nc.startCaBundleWatcher(stopCh)

	<-stopCh
}

func (nc *NamespaceController) runWorker() {
	for nc.processNextWorkItem() {
	}
}

// processNextWorkItem deals with one key off the queue. It returns false when
// it's time to quit.
func (nc *NamespaceController) processNextWorkItem() bool {
	key, quit := nc.queue.Get()
	if quit {
		return false
	}
	defer nc.queue.Done(key)

	if err := nc.insertDataForNamespace(key.(string)); err != nil {
		utilruntime.HandleError(fmt.Errorf("insertDataForNamespace %q failed: %v", key, err))
		nc.queue.AddRateLimited(key)
		return true
	}

	nc.queue.Forget(key)
	return true
}

// startCaBundleWatcher listens for updates to the CA bundle and update cm in each namespace
func (nc *NamespaceController) startCaBundleWatcher(stop <-chan struct{}) {
	id, watchCh := nc.caBundleWatcher.AddWatcher()
	defer nc.caBundleWatcher.RemoveWatcher(id)
	for {
		select {
		case <-watchCh:
			namespaceList, _ := nc.namespaceLister.List(labels.Everything())
			for _, ns := range namespaceList {
				nc.namespaceChange(ns)
			}
		case <-stop:
			return
		}
	}
}

// insertDataForNamespace will add data into the configmap for the specified namespace
// If the configmap is not found, it will be created.
// If you know the current contents of the configmap, using UpdateDataInConfigMap is more efficient.
func (nc *NamespaceController) insertDataForNamespace(ns string) error {
	meta := metav1.ObjectMeta{
		Name:      CACertNamespaceConfigMap,
		Namespace: ns,
		Labels:    configMapLabel,
	}
	return k8s.InsertDataToConfigMap(nc.client, nc.configmapLister, meta, nc.caBundleWatcher.GetCABundle())
}

// On namespace change, update the config map.
// If terminating, this will be skipped
func (nc *NamespaceController) namespaceChange(ns *v1.Namespace) {
	if ns.Status.Phase != v1.NamespaceTerminating {
		nc.syncNamespace(ns.Name)
	}
}

// On configMap change(update or delete), try to create or update the config map.
func (nc *NamespaceController) configMapChange(obj interface{}) {
	cm, err := convertToConfigMap(obj)
	if err != nil {
		log.Errorf("failed to convert to configmap: %v", err)
		return
	}
	// This is a change to a configmap we don't watch, ignore it
	if cm.Name != CACertNamespaceConfigMap {
		return
	}
	nc.syncNamespace(cm.Namespace)
}

func (nc *NamespaceController) syncNamespace(ns string) {
	// skip special kubernetes system namespaces
	for _, namespace := range inject.IgnoredNamespaces {
		if ns == namespace {
			return
		}
	}

	// If a MemberRoll controller is in use, and the set of
	// namespaces still includes the one for this ConfigMap,
	// then recreate the ConfigMap, otherwise do nothing.
	if nc.usesMemberRollController && !nc.namespaces.Contains(ns) {
		return
	}
	nc.queue.Add(ns)
}

func convertToConfigMap(obj interface{}) (*v1.ConfigMap, error) {
	cm, ok := obj.(*v1.ConfigMap)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return nil, fmt.Errorf("couldn't get object from tombstone %#v", obj)
		}
		cm, ok = tombstone.Obj.(*v1.ConfigMap)
		if !ok {
			return nil, fmt.Errorf("tombstone contained object that is not a ConfigMap %#v", obj)
		}
	}
	return cm, nil
}

// Copyright Red Hat, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package discovery

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	maistrainformersfederationv1 "maistra.io/api/client/informers/externalversions/federation/v1"
	maistraclient "maistra.io/api/client/versioned"
	"maistra.io/api/client/versioned/fake"
	v1 "maistra.io/api/federation/v1"

	"istio.io/api/mesh/v1alpha1"
	configmemory "istio.io/istio/pilot/pkg/config/memory"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/serviceregistry/aggregate"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/config/mesh"
	"istio.io/istio/pkg/config/schema/collections"
	"istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/kube/kclient"
	"istio.io/istio/pkg/servicemesh/federation/common"
	"istio.io/istio/pkg/servicemesh/federation/status"
)

type fakeManager struct{}

func (m *fakeManager) AddPeer(_ *v1.ServiceMeshPeer, _ *v1.ExportedServiceSet, _ status.Handler) error {
	return nil
}
func (m *fakeManager) DeletePeer(_ string) {}
func (m *fakeManager) UpdateExportsForMesh(_ *v1.ExportedServiceSet) error {
	return nil
}
func (m *fakeManager) DeleteExportsForMesh(_ string) {}

type fakeStatusManager struct{}

func (m *fakeStatusManager) IsLeader() bool {
	return true
}

func (m *fakeStatusManager) PeerAdded(mesh types.NamespacedName) status.Handler {
	return &common.FakeStatusHandler{}
}

func (m *fakeStatusManager) PeerDeleted(mesh types.NamespacedName) {
}

func (m *fakeStatusManager) HandlerFor(mesh types.NamespacedName) status.Handler {
	return &common.FakeStatusHandler{}
}

func (m *fakeStatusManager) PushStatus() error {
	return nil
}

type fakeResourceManager struct{}

func (m *fakeResourceManager) MaistraClientSet() maistraclient.Interface {
	panic("not implemented") // TODO: Implement
}

func (m *fakeResourceManager) ConfigMapInformer() kclient.Client[*corev1.ConfigMap] {
	panic("not implemented") // TODO: Implement
}

func (m *fakeResourceManager) EndpointSliceInformer() kclient.Client[*discoveryv1.EndpointSlice] {
	panic("not implemented") // TODO: Implement
}

func (m *fakeResourceManager) PodInformer() kclient.Client[*corev1.Pod] {
	panic("not implemented") // TODO: Implement
}

func (m *fakeResourceManager) ServiceInformer() kclient.Client[*corev1.Service] {
	panic("not implemented") // TODO: Implement
}

func (m *fakeResourceManager) PeerInformer() maistrainformersfederationv1.ServiceMeshPeerInformer {
	panic("not implemented") // TODO: Implement
}

func (m *fakeResourceManager) ExportsInformer() maistrainformersfederationv1.ExportedServiceSetInformer {
	panic("not implemented") // TODO: Implement
}

func (m *fakeResourceManager) ImportsInformer() maistrainformersfederationv1.ImportedServiceSetInformer {
	panic("not implemented") // TODO: Implement
}

func (m *fakeResourceManager) Start(stopCh <-chan struct{}) {
	panic("not implemented") // TODO: Implement
}

func (m *fakeResourceManager) HasSynced() bool {
	panic("not implemented") // TODO: Implement
}

func (m *fakeResourceManager) WaitForCacheSync(stopCh <-chan struct{}) map[reflect.Type]bool {
	panic("not implemented") // TODO: Implement
}

func TestValidOptions(t *testing.T) {
	opt := Options{
		ResourceManager:   &fakeResourceManager{},
		ServiceController: &aggregate.Controller{},
		XDSUpdater:        &model.EndpointIndexUpdater{},
		Env:               &model.Environment{},
		FederationManager: &fakeManager{},
	}
	if err := opt.validate(); err != nil {
		t.Errorf("unexpected error")
	}
}

func TestInvalidOptions(t *testing.T) {
	testCases := []struct {
		name string
		opt  Options
	}{
		{
			name: "resource-manager",
			opt: Options{
				ResourceManager:   nil,
				ServiceController: &aggregate.Controller{},
				XDSUpdater:        &model.EndpointIndexUpdater{},
				Env:               &model.Environment{},
			},
		},
		{
			name: "service-controller",
			opt: Options{
				ResourceManager:   &fakeResourceManager{},
				ServiceController: nil,
				XDSUpdater:        &model.EndpointIndexUpdater{},
				Env:               &model.Environment{},
			},
		},
		{
			name: "xds-updater",
			opt: Options{
				ResourceManager:   &fakeResourceManager{},
				ServiceController: &aggregate.Controller{},
				XDSUpdater:        nil,
				Env:               &model.Environment{},
			},
		},
		{
			name: "env",
			opt: Options{
				ResourceManager:   &fakeResourceManager{},
				ServiceController: &aggregate.Controller{},
				XDSUpdater:        &model.EndpointIndexUpdater{},
				Env:               nil,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewController(tc.opt); err == nil {
				t.Errorf("expected error")
			}
		})
	}
}

type options struct {
	client            kube.Client
	serviceController *aggregate.Controller
	xdsUpdater        model.XDSUpdater
	env               *model.Environment
}

func newTestOptions(discoveryAddress string) options {
	client := kube.NewFakeClient()
	meshConfig := &v1alpha1.MeshConfig{
		DefaultConfig: &v1alpha1.ProxyConfig{
			DiscoveryAddress: discoveryAddress,
		},
	}
	meshWatcher := mesh.NewFixedWatcher(meshConfig)
	serviceController := aggregate.NewController(aggregate.Options{
		MeshHolder: meshWatcher,
	})
	env := &model.Environment{
		ServiceDiscovery: serviceController,
		Watcher:          meshWatcher,
	}
	xdsUpdater := &model.EndpointIndexUpdater{Index: env.EndpointIndex}
	return options{
		client:            client,
		serviceController: serviceController,
		xdsUpdater:        xdsUpdater,
		env:               env,
	}
}

func TestReconcile(t *testing.T) {
	name := "test"
	namespace := "test"
	resyncPeriod := 30 * time.Second
	options := newTestOptions("test.address")
	newCrdWatcher := kube.NewCrdWatcher
	kube.NewCrdWatcher = kube.NewFastCrdWatcher
	kubeClient := kube.NewFakeClient(
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "dummy",
				Namespace: namespace,
			},
			Data: map[string]string{
				"root-cert.pem": "dummy-cert-pem",
			},
		},
	)
	kube.NewCrdWatcher = newCrdWatcher
	rm, err := common.NewResourceManager(common.ControllerOptions{
		KubeClient:   kubeClient,
		MaistraCS:    fake.NewSimpleClientset(),
		ResyncPeriod: resyncPeriod,
		Namespace:    namespace,
	}, nil)
	if err != nil {
		t.Fatalf("unable to create ResourceManager: %s", err)
	}
	controller, err := NewController(Options{
		ResourceManager:   rm,
		ResyncPeriod:      resyncPeriod,
		ServiceController: options.serviceController,
		XDSUpdater:        options.xdsUpdater,
		Env:               options.env,
		FederationManager: &fakeManager{},
		StatusManager:     &fakeStatusManager{},
		ConfigStore:       configmemory.NewController(configmemory.Make(Schemas)),
	})
	if err != nil {
		t.Fatalf("unable to create Controller: %s", err)
	}

	federation := &v1.ServiceMeshPeer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.ServiceMeshPeerSpec{
			Remote: v1.ServiceMeshPeerRemote{
				Addresses: []string{"test.mesh"},
			},
			Gateways: v1.ServiceMeshPeerGateways{
				Ingress: corev1.LocalObjectReference{
					Name: "test-ingress",
				},
				Egress: corev1.LocalObjectReference{
					Name: "test-egress",
				},
			},
			Security: v1.ServiceMeshPeerSecurity{
				ClientID:    "cluster.local/ns/test-mesh/sa/test-egress-service-account",
				TrustDomain: "test.local",
				CertificateChain: corev1.TypedLocalObjectReference{
					Name: "dummy",
				},
				AllowDirectInbound:  false,
				AllowDirectOutbound: false,
			},
		},
	}
	stopCh := make(chan struct{})
	defer close(stopCh)
	go kubeClient.RunAndWait(stopCh)
	go rm.Start(stopCh)
	fedAdded := make(chan struct{})
	fedDeleted := make(chan struct{})
	rm.PeerInformer().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			close(fedAdded)
		},
		DeleteFunc: func(obj interface{}) {
			close(fedDeleted)
		},
	})
	cs := rm.MaistraClientSet()
	newFederation, err := cs.FederationV1().ServiceMeshPeers(namespace).Create(context.TODO(), federation, metav1.CreateOptions{})
	if err != nil {
		t.Errorf("failed to create ServiceMeshPeer")
		return
	}
	// wait for object to show up
	select {
	case <-fedAdded:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for watch event")
	}
	if err := controller.reconcile(fmt.Sprintf("%s/%s", namespace, name)); err != nil {
		t.Fatalf("unexpected error reconciling new ServiceMeshPeer: %s", err)
	}
	// verify registry has been created
	if controller.getRegistry(cluster.ID(newFederation.Name)) == nil {
		t.Errorf("failed to create service registry for federation")
	}
	// verify resources have been created
	if resource := controller.Get(collections.ServiceEntry.GroupVersionKind(),
		discoveryResourceName(federation), namespace); resource == nil {
		t.Errorf("resource doesn't exist")
	}
	if resource := controller.Get(collections.VirtualService.GroupVersionKind(),
		discoveryEgressResourceName(federation), namespace); resource == nil {
		t.Errorf("resource doesn't exist")
	}
	if resource := controller.Get(collections.VirtualService.GroupVersionKind(),
		discoveryIngressResourceName(federation), namespace); resource == nil {
		t.Errorf("resource doesn't exist")
	}
	if resource := controller.Get(collections.Gateway.GroupVersionKind(),
		discoveryIngressResourceName(federation), namespace); resource == nil {
		t.Errorf("resource doesn't exist")
	}
	if resource := controller.Get(collections.Gateway.GroupVersionKind(),
		discoveryEgressResourceName(federation), namespace); resource == nil {
		t.Errorf("resource doesn't exist")
	}
	if resource := controller.Get(collections.DestinationRule.GroupVersionKind(),
		discoveryEgressResourceName(federation), namespace); resource == nil {
		t.Errorf("resource doesn't exist")
	}
	if resource := controller.Get(collections.AuthorizationPolicy.GroupVersionKind(),
		discoveryResourceName(federation), namespace); resource == nil {
		t.Errorf("resource doesn't exist")
	}

	// now delete
	if err = cs.FederationV1().ServiceMeshPeers(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{}); err != nil {
		t.Errorf("error deleting ServiceMeshPeer")
		return
	}

	// wait for deletion to show up
	select {
	case <-fedDeleted:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for watch event")
	}

	if err := controller.reconcile(fmt.Sprintf("%s/%s", namespace, name)); err != nil {
		t.Errorf("unexpected error reconciling new ServiceMeshPeer: %#v", err)
	}
	// verify registry has been deleted
	if controller.getRegistry(cluster.ID(newFederation.Name)) != nil {
		t.Errorf("failed to delete service registry for federation")
	}
	// verify resources have been deleted
	if resource := controller.Get(collections.ServiceEntry.GroupVersionKind(),
		discoveryResourceName(federation), namespace); resource != nil {
		t.Errorf("resource not deleted")
	}
	if resource := controller.Get(collections.VirtualService.GroupVersionKind(),
		discoveryResourceName(federation), namespace); resource != nil {
		t.Errorf("resource not deleted")
	}
	if resource := controller.Get(collections.Gateway.GroupVersionKind(),
		discoveryIngressResourceName(federation), namespace); resource != nil {
		t.Errorf("resource not deleted")
	}
	if resource := controller.Get(collections.Gateway.GroupVersionKind(),
		discoveryEgressResourceName(federation), namespace); resource != nil {
		t.Errorf("resource not deleted")
	}
	if resource := controller.Get(collections.DestinationRule.GroupVersionKind(),
		discoveryResourceName(federation), namespace); resource != nil {
		t.Errorf("resource not deleted")
	}
	if resource := controller.Get(collections.AuthorizationPolicy.GroupVersionKind(),
		discoveryResourceName(federation), namespace); resource != nil {
		t.Errorf("resource not deleted")
	}
}

// Code generated by pkg/config/schema/codegen/tools/collections.main.go. DO NOT EDIT.

package kubeclient

import (
	"context"
	"fmt"

	githubcomopenshiftapiroutev1 "github.com/openshift/api/route/v1"
	k8sioapiadmissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	k8sioapiappsv1 "k8s.io/api/apps/v1"
	k8sioapicertificatesv1 "k8s.io/api/certificates/v1"
	k8sioapicoordinationv1 "k8s.io/api/coordination/v1"
	k8sioapicorev1 "k8s.io/api/core/v1"
	k8sioapidiscoveryv1 "k8s.io/api/discovery/v1"
	k8sioapinetworkingv1 "k8s.io/api/networking/v1"
	k8sioapiextensionsapiserverpkgapisapiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	sigsk8siogatewayapiapisv1alpha2 "sigs.k8s.io/gateway-api/apis/v1alpha2"
	sigsk8siogatewayapiapisv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	apiistioioapiextensionsv1alpha1 "istio.io/client-go/pkg/apis/extensions/v1alpha1"
	apiistioioapinetworkingv1alpha3 "istio.io/client-go/pkg/apis/networking/v1alpha3"
	apiistioioapinetworkingv1beta1 "istio.io/client-go/pkg/apis/networking/v1beta1"
	apiistioioapisecurityv1beta1 "istio.io/client-go/pkg/apis/security/v1beta1"
	apiistioioapitelemetryv1alpha1 "istio.io/client-go/pkg/apis/telemetry/v1alpha1"
	"istio.io/istio/pkg/config/schema/gvr"
	"istio.io/istio/pkg/kube/informerfactory"
	ktypes "istio.io/istio/pkg/kube/kubetypes"
	"istio.io/istio/pkg/ptr"
)

func GetWriteClient[T runtime.Object](c ClientGetter, namespace string) ktypes.WriteAPI[T] {
	switch any(ptr.Empty[T]()).(type) {
	case *apiistioioapisecurityv1beta1.AuthorizationPolicy:
		return c.Istio().SecurityV1beta1().AuthorizationPolicies(namespace).(ktypes.WriteAPI[T])
	case *k8sioapicertificatesv1.CertificateSigningRequest:
		return c.Kube().CertificatesV1().CertificateSigningRequests().(ktypes.WriteAPI[T])
	case *k8sioapicorev1.ConfigMap:
		return c.Kube().CoreV1().ConfigMaps(namespace).(ktypes.WriteAPI[T])
	case *k8sioapiextensionsapiserverpkgapisapiextensionsv1.CustomResourceDefinition:
		return c.Ext().ApiextensionsV1().CustomResourceDefinitions().(ktypes.WriteAPI[T])
	case *k8sioapiappsv1.DaemonSet:
		return c.Kube().AppsV1().DaemonSets(namespace).(ktypes.WriteAPI[T])
	case *k8sioapiappsv1.Deployment:
		return c.Kube().AppsV1().Deployments(namespace).(ktypes.WriteAPI[T])
	case *apiistioioapinetworkingv1alpha3.DestinationRule:
		return c.Istio().NetworkingV1alpha3().DestinationRules(namespace).(ktypes.WriteAPI[T])
	case *k8sioapidiscoveryv1.EndpointSlice:
		return c.Kube().DiscoveryV1().EndpointSlices(namespace).(ktypes.WriteAPI[T])
	case *k8sioapicorev1.Endpoints:
		return c.Kube().CoreV1().Endpoints(namespace).(ktypes.WriteAPI[T])
	case *apiistioioapinetworkingv1alpha3.EnvoyFilter:
		return c.Istio().NetworkingV1alpha3().EnvoyFilters(namespace).(ktypes.WriteAPI[T])
	case *sigsk8siogatewayapiapisv1alpha2.GRPCRoute:
		return c.GatewayAPI().GatewayV1alpha2().GRPCRoutes(namespace).(ktypes.WriteAPI[T])
	case *apiistioioapinetworkingv1alpha3.Gateway:
		return c.Istio().NetworkingV1alpha3().Gateways(namespace).(ktypes.WriteAPI[T])
	case *sigsk8siogatewayapiapisv1beta1.GatewayClass:
		return c.GatewayAPI().GatewayV1beta1().GatewayClasses().(ktypes.WriteAPI[T])
	case *sigsk8siogatewayapiapisv1beta1.HTTPRoute:
		return c.GatewayAPI().GatewayV1beta1().HTTPRoutes(namespace).(ktypes.WriteAPI[T])
	case *k8sioapinetworkingv1.Ingress:
		return c.Kube().NetworkingV1().Ingresses(namespace).(ktypes.WriteAPI[T])
	case *k8sioapinetworkingv1.IngressClass:
		return c.Kube().NetworkingV1().IngressClasses().(ktypes.WriteAPI[T])
	case *sigsk8siogatewayapiapisv1beta1.Gateway:
		return c.GatewayAPI().GatewayV1beta1().Gateways(namespace).(ktypes.WriteAPI[T])
	case *k8sioapicoordinationv1.Lease:
		return c.Kube().CoordinationV1().Leases(namespace).(ktypes.WriteAPI[T])
	case *k8sioapiadmissionregistrationv1.MutatingWebhookConfiguration:
		return c.Kube().AdmissionregistrationV1().MutatingWebhookConfigurations().(ktypes.WriteAPI[T])
	case *k8sioapicorev1.Namespace:
		return c.Kube().CoreV1().Namespaces().(ktypes.WriteAPI[T])
	case *k8sioapicorev1.Node:
		return c.Kube().CoreV1().Nodes().(ktypes.WriteAPI[T])
	case *apiistioioapisecurityv1beta1.PeerAuthentication:
		return c.Istio().SecurityV1beta1().PeerAuthentications(namespace).(ktypes.WriteAPI[T])
	case *k8sioapicorev1.Pod:
		return c.Kube().CoreV1().Pods(namespace).(ktypes.WriteAPI[T])
	case *apiistioioapinetworkingv1beta1.ProxyConfig:
		return c.Istio().NetworkingV1beta1().ProxyConfigs(namespace).(ktypes.WriteAPI[T])
	case *sigsk8siogatewayapiapisv1beta1.ReferenceGrant:
		return c.GatewayAPI().GatewayV1beta1().ReferenceGrants(namespace).(ktypes.WriteAPI[T])
	case *apiistioioapisecurityv1beta1.RequestAuthentication:
		return c.Istio().SecurityV1beta1().RequestAuthentications(namespace).(ktypes.WriteAPI[T])
	case *githubcomopenshiftapiroutev1.Route:
		return c.Route().RouteV1().Routes(namespace).(ktypes.WriteAPI[T])
	case *k8sioapicorev1.Secret:
		return c.Kube().CoreV1().Secrets(namespace).(ktypes.WriteAPI[T])
	case *k8sioapicorev1.Service:
		return c.Kube().CoreV1().Services(namespace).(ktypes.WriteAPI[T])
	case *k8sioapicorev1.ServiceAccount:
		return c.Kube().CoreV1().ServiceAccounts(namespace).(ktypes.WriteAPI[T])
	case *apiistioioapinetworkingv1alpha3.ServiceEntry:
		return c.Istio().NetworkingV1alpha3().ServiceEntries(namespace).(ktypes.WriteAPI[T])
	case *apiistioioapinetworkingv1alpha3.Sidecar:
		return c.Istio().NetworkingV1alpha3().Sidecars(namespace).(ktypes.WriteAPI[T])
	case *k8sioapiappsv1.StatefulSet:
		return c.Kube().AppsV1().StatefulSets(namespace).(ktypes.WriteAPI[T])
	case *sigsk8siogatewayapiapisv1alpha2.TCPRoute:
		return c.GatewayAPI().GatewayV1alpha2().TCPRoutes(namespace).(ktypes.WriteAPI[T])
	case *sigsk8siogatewayapiapisv1alpha2.TLSRoute:
		return c.GatewayAPI().GatewayV1alpha2().TLSRoutes(namespace).(ktypes.WriteAPI[T])
	case *apiistioioapitelemetryv1alpha1.Telemetry:
		return c.Istio().TelemetryV1alpha1().Telemetries(namespace).(ktypes.WriteAPI[T])
	case *sigsk8siogatewayapiapisv1alpha2.UDPRoute:
		return c.GatewayAPI().GatewayV1alpha2().UDPRoutes(namespace).(ktypes.WriteAPI[T])
	case *k8sioapiadmissionregistrationv1.ValidatingWebhookConfiguration:
		return c.Kube().AdmissionregistrationV1().ValidatingWebhookConfigurations().(ktypes.WriteAPI[T])
	case *apiistioioapinetworkingv1alpha3.VirtualService:
		return c.Istio().NetworkingV1alpha3().VirtualServices(namespace).(ktypes.WriteAPI[T])
	case *apiistioioapiextensionsv1alpha1.WasmPlugin:
		return c.Istio().ExtensionsV1alpha1().WasmPlugins(namespace).(ktypes.WriteAPI[T])
	case *apiistioioapinetworkingv1alpha3.WorkloadEntry:
		return c.Istio().NetworkingV1alpha3().WorkloadEntries(namespace).(ktypes.WriteAPI[T])
	case *apiistioioapinetworkingv1alpha3.WorkloadGroup:
		return c.Istio().NetworkingV1alpha3().WorkloadGroups(namespace).(ktypes.WriteAPI[T])
	default:
		panic(fmt.Sprintf("Unknown type %T", ptr.Empty[T]()))
	}
}

func GetClient[T, TL runtime.Object](c ClientGetter, namespace string) ktypes.ReadWriteAPI[T, TL] {
	switch any(ptr.Empty[T]()).(type) {
	case *apiistioioapisecurityv1beta1.AuthorizationPolicy:
		return c.Istio().SecurityV1beta1().AuthorizationPolicies(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapicertificatesv1.CertificateSigningRequest:
		return c.Kube().CertificatesV1().CertificateSigningRequests().(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapicorev1.ConfigMap:
		return c.Kube().CoreV1().ConfigMaps(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapiextensionsapiserverpkgapisapiextensionsv1.CustomResourceDefinition:
		return c.Ext().ApiextensionsV1().CustomResourceDefinitions().(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapiappsv1.DaemonSet:
		return c.Kube().AppsV1().DaemonSets(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapiappsv1.Deployment:
		return c.Kube().AppsV1().Deployments(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *apiistioioapinetworkingv1alpha3.DestinationRule:
		return c.Istio().NetworkingV1alpha3().DestinationRules(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapidiscoveryv1.EndpointSlice:
		return c.Kube().DiscoveryV1().EndpointSlices(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapicorev1.Endpoints:
		return c.Kube().CoreV1().Endpoints(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *apiistioioapinetworkingv1alpha3.EnvoyFilter:
		return c.Istio().NetworkingV1alpha3().EnvoyFilters(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *sigsk8siogatewayapiapisv1alpha2.GRPCRoute:
		return c.GatewayAPI().GatewayV1alpha2().GRPCRoutes(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *apiistioioapinetworkingv1alpha3.Gateway:
		return c.Istio().NetworkingV1alpha3().Gateways(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *sigsk8siogatewayapiapisv1beta1.GatewayClass:
		return c.GatewayAPI().GatewayV1beta1().GatewayClasses().(ktypes.ReadWriteAPI[T, TL])
	case *sigsk8siogatewayapiapisv1beta1.HTTPRoute:
		return c.GatewayAPI().GatewayV1beta1().HTTPRoutes(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapinetworkingv1.Ingress:
		return c.Kube().NetworkingV1().Ingresses(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapinetworkingv1.IngressClass:
		return c.Kube().NetworkingV1().IngressClasses().(ktypes.ReadWriteAPI[T, TL])
	case *sigsk8siogatewayapiapisv1beta1.Gateway:
		return c.GatewayAPI().GatewayV1beta1().Gateways(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapicoordinationv1.Lease:
		return c.Kube().CoordinationV1().Leases(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapiadmissionregistrationv1.MutatingWebhookConfiguration:
		return c.Kube().AdmissionregistrationV1().MutatingWebhookConfigurations().(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapicorev1.Namespace:
		return c.Kube().CoreV1().Namespaces().(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapicorev1.Node:
		return c.Kube().CoreV1().Nodes().(ktypes.ReadWriteAPI[T, TL])
	case *apiistioioapisecurityv1beta1.PeerAuthentication:
		return c.Istio().SecurityV1beta1().PeerAuthentications(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapicorev1.Pod:
		return c.Kube().CoreV1().Pods(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *apiistioioapinetworkingv1beta1.ProxyConfig:
		return c.Istio().NetworkingV1beta1().ProxyConfigs(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *sigsk8siogatewayapiapisv1beta1.ReferenceGrant:
		return c.GatewayAPI().GatewayV1beta1().ReferenceGrants(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *apiistioioapisecurityv1beta1.RequestAuthentication:
		return c.Istio().SecurityV1beta1().RequestAuthentications(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *githubcomopenshiftapiroutev1.Route:
		return c.Route().RouteV1().Routes(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapicorev1.Secret:
		return c.Kube().CoreV1().Secrets(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapicorev1.Service:
		return c.Kube().CoreV1().Services(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapicorev1.ServiceAccount:
		return c.Kube().CoreV1().ServiceAccounts(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *apiistioioapinetworkingv1alpha3.ServiceEntry:
		return c.Istio().NetworkingV1alpha3().ServiceEntries(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *apiistioioapinetworkingv1alpha3.Sidecar:
		return c.Istio().NetworkingV1alpha3().Sidecars(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapiappsv1.StatefulSet:
		return c.Kube().AppsV1().StatefulSets(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *sigsk8siogatewayapiapisv1alpha2.TCPRoute:
		return c.GatewayAPI().GatewayV1alpha2().TCPRoutes(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *sigsk8siogatewayapiapisv1alpha2.TLSRoute:
		return c.GatewayAPI().GatewayV1alpha2().TLSRoutes(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *apiistioioapitelemetryv1alpha1.Telemetry:
		return c.Istio().TelemetryV1alpha1().Telemetries(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *sigsk8siogatewayapiapisv1alpha2.UDPRoute:
		return c.GatewayAPI().GatewayV1alpha2().UDPRoutes(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *k8sioapiadmissionregistrationv1.ValidatingWebhookConfiguration:
		return c.Kube().AdmissionregistrationV1().ValidatingWebhookConfigurations().(ktypes.ReadWriteAPI[T, TL])
	case *apiistioioapinetworkingv1alpha3.VirtualService:
		return c.Istio().NetworkingV1alpha3().VirtualServices(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *apiistioioapiextensionsv1alpha1.WasmPlugin:
		return c.Istio().ExtensionsV1alpha1().WasmPlugins(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *apiistioioapinetworkingv1alpha3.WorkloadEntry:
		return c.Istio().NetworkingV1alpha3().WorkloadEntries(namespace).(ktypes.ReadWriteAPI[T, TL])
	case *apiistioioapinetworkingv1alpha3.WorkloadGroup:
		return c.Istio().NetworkingV1alpha3().WorkloadGroups(namespace).(ktypes.ReadWriteAPI[T, TL])
	default:
		panic(fmt.Sprintf("Unknown type %T", ptr.Empty[T]()))
	}
}

func gvrToObject(g schema.GroupVersionResource) runtime.Object {
	switch g {
	case gvr.AuthorizationPolicy:
		return &apiistioioapisecurityv1beta1.AuthorizationPolicy{}
	case gvr.CertificateSigningRequest:
		return &k8sioapicertificatesv1.CertificateSigningRequest{}
	case gvr.ConfigMap:
		return &k8sioapicorev1.ConfigMap{}
	case gvr.CustomResourceDefinition:
		return &k8sioapiextensionsapiserverpkgapisapiextensionsv1.CustomResourceDefinition{}
	case gvr.DaemonSet:
		return &k8sioapiappsv1.DaemonSet{}
	case gvr.Deployment:
		return &k8sioapiappsv1.Deployment{}
	case gvr.DestinationRule:
		return &apiistioioapinetworkingv1alpha3.DestinationRule{}
	case gvr.EndpointSlice:
		return &k8sioapidiscoveryv1.EndpointSlice{}
	case gvr.Endpoints:
		return &k8sioapicorev1.Endpoints{}
	case gvr.EnvoyFilter:
		return &apiistioioapinetworkingv1alpha3.EnvoyFilter{}
	case gvr.GRPCRoute:
		return &sigsk8siogatewayapiapisv1alpha2.GRPCRoute{}
	case gvr.Gateway:
		return &apiistioioapinetworkingv1alpha3.Gateway{}
	case gvr.GatewayClass:
		return &sigsk8siogatewayapiapisv1beta1.GatewayClass{}
	case gvr.HTTPRoute:
		return &sigsk8siogatewayapiapisv1beta1.HTTPRoute{}
	case gvr.Ingress:
		return &k8sioapinetworkingv1.Ingress{}
	case gvr.IngressClass:
		return &k8sioapinetworkingv1.IngressClass{}
	case gvr.KubernetesGateway:
		return &sigsk8siogatewayapiapisv1beta1.Gateway{}
	case gvr.Lease:
		return &k8sioapicoordinationv1.Lease{}
	case gvr.MutatingWebhookConfiguration:
		return &k8sioapiadmissionregistrationv1.MutatingWebhookConfiguration{}
	case gvr.Namespace:
		return &k8sioapicorev1.Namespace{}
	case gvr.Node:
		return &k8sioapicorev1.Node{}
	case gvr.PeerAuthentication:
		return &apiistioioapisecurityv1beta1.PeerAuthentication{}
	case gvr.Pod:
		return &k8sioapicorev1.Pod{}
	case gvr.ProxyConfig:
		return &apiistioioapinetworkingv1beta1.ProxyConfig{}
	case gvr.ReferenceGrant:
		return &sigsk8siogatewayapiapisv1beta1.ReferenceGrant{}
	case gvr.RequestAuthentication:
		return &apiistioioapisecurityv1beta1.RequestAuthentication{}
	case gvr.Route:
		return &githubcomopenshiftapiroutev1.Route{}
	case gvr.Secret:
		return &k8sioapicorev1.Secret{}
	case gvr.Service:
		return &k8sioapicorev1.Service{}
	case gvr.ServiceAccount:
		return &k8sioapicorev1.ServiceAccount{}
	case gvr.ServiceEntry:
		return &apiistioioapinetworkingv1alpha3.ServiceEntry{}
	case gvr.Sidecar:
		return &apiistioioapinetworkingv1alpha3.Sidecar{}
	case gvr.StatefulSet:
		return &k8sioapiappsv1.StatefulSet{}
	case gvr.TCPRoute:
		return &sigsk8siogatewayapiapisv1alpha2.TCPRoute{}
	case gvr.TLSRoute:
		return &sigsk8siogatewayapiapisv1alpha2.TLSRoute{}
	case gvr.Telemetry:
		return &apiistioioapitelemetryv1alpha1.Telemetry{}
	case gvr.UDPRoute:
		return &sigsk8siogatewayapiapisv1alpha2.UDPRoute{}
	case gvr.ValidatingWebhookConfiguration:
		return &k8sioapiadmissionregistrationv1.ValidatingWebhookConfiguration{}
	case gvr.VirtualService:
		return &apiistioioapinetworkingv1alpha3.VirtualService{}
	case gvr.WasmPlugin:
		return &apiistioioapiextensionsv1alpha1.WasmPlugin{}
	case gvr.WorkloadEntry:
		return &apiistioioapinetworkingv1alpha3.WorkloadEntry{}
	case gvr.WorkloadGroup:
		return &apiistioioapinetworkingv1alpha3.WorkloadGroup{}
	default:
		panic(fmt.Sprintf("Unknown type %v", g))
	}
}

func getInformerFiltered(c ClientGetter, opts ktypes.InformerOptions, g schema.GroupVersionResource) informerfactory.StartableInformer {
	var l func(namespace string, options metav1.ListOptions) (runtime.Object, error)
	var w func(namespace string, options metav1.ListOptions) (watch.Interface, error)

	switch g {
	case gvr.AuthorizationPolicy:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Istio().SecurityV1beta1().AuthorizationPolicies(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Istio().SecurityV1beta1().AuthorizationPolicies(namespace).Watch(context.Background(), options)
		}
	case gvr.CertificateSigningRequest:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().CertificatesV1().CertificateSigningRequests().List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().CertificatesV1().CertificateSigningRequests().Watch(context.Background(), options)
		}
	case gvr.ConfigMap:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().CoreV1().ConfigMaps(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().CoreV1().ConfigMaps(namespace).Watch(context.Background(), options)
		}
	case gvr.CustomResourceDefinition:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Ext().ApiextensionsV1().CustomResourceDefinitions().List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Ext().ApiextensionsV1().CustomResourceDefinitions().Watch(context.Background(), options)
		}
	case gvr.DaemonSet:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().AppsV1().DaemonSets(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().AppsV1().DaemonSets(namespace).Watch(context.Background(), options)
		}
	case gvr.Deployment:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().AppsV1().Deployments(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().AppsV1().Deployments(namespace).Watch(context.Background(), options)
		}
	case gvr.DestinationRule:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Istio().NetworkingV1alpha3().DestinationRules(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Istio().NetworkingV1alpha3().DestinationRules(namespace).Watch(context.Background(), options)
		}
	case gvr.EndpointSlice:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().DiscoveryV1().EndpointSlices(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().DiscoveryV1().EndpointSlices(namespace).Watch(context.Background(), options)
		}
	case gvr.Endpoints:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().CoreV1().Endpoints(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().CoreV1().Endpoints(namespace).Watch(context.Background(), options)
		}
	case gvr.EnvoyFilter:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Istio().NetworkingV1alpha3().EnvoyFilters(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Istio().NetworkingV1alpha3().EnvoyFilters(namespace).Watch(context.Background(), options)
		}
	case gvr.GRPCRoute:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.GatewayAPI().GatewayV1alpha2().GRPCRoutes(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.GatewayAPI().GatewayV1alpha2().GRPCRoutes(namespace).Watch(context.Background(), options)
		}
	case gvr.Gateway:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Istio().NetworkingV1alpha3().Gateways(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Istio().NetworkingV1alpha3().Gateways(namespace).Watch(context.Background(), options)
		}
	case gvr.GatewayClass:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.GatewayAPI().GatewayV1beta1().GatewayClasses().List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.GatewayAPI().GatewayV1beta1().GatewayClasses().Watch(context.Background(), options)
		}
	case gvr.HTTPRoute:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.GatewayAPI().GatewayV1beta1().HTTPRoutes(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.GatewayAPI().GatewayV1beta1().HTTPRoutes(namespace).Watch(context.Background(), options)
		}
	case gvr.Ingress:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().NetworkingV1().Ingresses(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().NetworkingV1().Ingresses(namespace).Watch(context.Background(), options)
		}
	case gvr.IngressClass:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().NetworkingV1().IngressClasses().List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().NetworkingV1().IngressClasses().Watch(context.Background(), options)
		}
	case gvr.KubernetesGateway:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.GatewayAPI().GatewayV1beta1().Gateways(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.GatewayAPI().GatewayV1beta1().Gateways(namespace).Watch(context.Background(), options)
		}
	case gvr.Lease:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().CoordinationV1().Leases(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().CoordinationV1().Leases(namespace).Watch(context.Background(), options)
		}
	case gvr.MutatingWebhookConfiguration:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().AdmissionregistrationV1().MutatingWebhookConfigurations().List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().AdmissionregistrationV1().MutatingWebhookConfigurations().Watch(context.Background(), options)
		}
	case gvr.Namespace:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().CoreV1().Namespaces().List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().CoreV1().Namespaces().Watch(context.Background(), options)
		}
	case gvr.Node:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().CoreV1().Nodes().List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().CoreV1().Nodes().Watch(context.Background(), options)
		}
	case gvr.PeerAuthentication:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Istio().SecurityV1beta1().PeerAuthentications(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Istio().SecurityV1beta1().PeerAuthentications(namespace).Watch(context.Background(), options)
		}
	case gvr.Pod:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().CoreV1().Pods(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().CoreV1().Pods(namespace).Watch(context.Background(), options)
		}
	case gvr.ProxyConfig:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Istio().NetworkingV1beta1().ProxyConfigs(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Istio().NetworkingV1beta1().ProxyConfigs(namespace).Watch(context.Background(), options)
		}
	case gvr.ReferenceGrant:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.GatewayAPI().GatewayV1beta1().ReferenceGrants(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.GatewayAPI().GatewayV1beta1().ReferenceGrants(namespace).Watch(context.Background(), options)
		}
	case gvr.RequestAuthentication:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Istio().SecurityV1beta1().RequestAuthentications(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Istio().SecurityV1beta1().RequestAuthentications(namespace).Watch(context.Background(), options)
		}
	case gvr.Route:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Route().RouteV1().Routes(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Route().RouteV1().Routes(namespace).Watch(context.Background(), options)
		}
	case gvr.Secret:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().CoreV1().Secrets(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().CoreV1().Secrets(namespace).Watch(context.Background(), options)
		}
	case gvr.Service:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().CoreV1().Services(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().CoreV1().Services(namespace).Watch(context.Background(), options)
		}
	case gvr.ServiceAccount:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().CoreV1().ServiceAccounts(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().CoreV1().ServiceAccounts(namespace).Watch(context.Background(), options)
		}
	case gvr.ServiceEntry:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Istio().NetworkingV1alpha3().ServiceEntries(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Istio().NetworkingV1alpha3().ServiceEntries(namespace).Watch(context.Background(), options)
		}
	case gvr.Sidecar:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Istio().NetworkingV1alpha3().Sidecars(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Istio().NetworkingV1alpha3().Sidecars(namespace).Watch(context.Background(), options)
		}
	case gvr.StatefulSet:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().AppsV1().StatefulSets(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().AppsV1().StatefulSets(namespace).Watch(context.Background(), options)
		}
	case gvr.TCPRoute:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.GatewayAPI().GatewayV1alpha2().TCPRoutes(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.GatewayAPI().GatewayV1alpha2().TCPRoutes(namespace).Watch(context.Background(), options)
		}
	case gvr.TLSRoute:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.GatewayAPI().GatewayV1alpha2().TLSRoutes(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.GatewayAPI().GatewayV1alpha2().TLSRoutes(namespace).Watch(context.Background(), options)
		}
	case gvr.Telemetry:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Istio().TelemetryV1alpha1().Telemetries(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Istio().TelemetryV1alpha1().Telemetries(namespace).Watch(context.Background(), options)
		}
	case gvr.UDPRoute:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.GatewayAPI().GatewayV1alpha2().UDPRoutes(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.GatewayAPI().GatewayV1alpha2().UDPRoutes(namespace).Watch(context.Background(), options)
		}
	case gvr.ValidatingWebhookConfiguration:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Kube().AdmissionregistrationV1().ValidatingWebhookConfigurations().List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Kube().AdmissionregistrationV1().ValidatingWebhookConfigurations().Watch(context.Background(), options)
		}
	case gvr.VirtualService:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Istio().NetworkingV1alpha3().VirtualServices(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Istio().NetworkingV1alpha3().VirtualServices(namespace).Watch(context.Background(), options)
		}
	case gvr.WasmPlugin:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Istio().ExtensionsV1alpha1().WasmPlugins(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Istio().ExtensionsV1alpha1().WasmPlugins(namespace).Watch(context.Background(), options)
		}
	case gvr.WorkloadEntry:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Istio().NetworkingV1alpha3().WorkloadEntries(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Istio().NetworkingV1alpha3().WorkloadEntries(namespace).Watch(context.Background(), options)
		}
	case gvr.WorkloadGroup:
		l = func(namespace string, options metav1.ListOptions) (runtime.Object, error) {
			return c.Istio().NetworkingV1alpha3().WorkloadGroups(namespace).List(context.Background(), options)
		}
		w = func(namespace string, options metav1.ListOptions) (watch.Interface, error) {
			return c.Istio().NetworkingV1alpha3().WorkloadGroups(namespace).Watch(context.Background(), options)
		}
	default:
		panic(fmt.Sprintf("Unknown type %v", g))
	}
	return c.Informers().InformerFor(g, opts, func(namespace string) cache.SharedIndexInformer {
		inf := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
					options.FieldSelector = opts.FieldSelector
					options.LabelSelector = opts.LabelSelector
					return l(namespace, options)
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					options.FieldSelector = opts.FieldSelector
					options.LabelSelector = opts.LabelSelector
					return w(namespace, options)
				},
			},
			gvrToObject(g),
			0,
			cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		)
		setupInformer(opts, inf)
		return inf
	})
}

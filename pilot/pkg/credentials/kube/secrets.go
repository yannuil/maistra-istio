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

package kube

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	informersv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	authorizationv1client "k8s.io/client-go/kubernetes/typed/authorization/v1"
	listersv1 "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"istio.io/istio/pilot/pkg/credentials"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/kube"
	"istio.io/pkg/log"
)

const (
	// The ID/name for the certificate chain in kubernetes generic secret.
	GenericScrtCert = "cert"
	// The ID/name for the private key in kubernetes generic secret.
	GenericScrtKey = "key"
	// The ID/name for the CA certificate in kubernetes generic secret.
	GenericScrtCaCert = "cacert"

	// The ID/name for the certificate chain in kubernetes tls secret.
	TLSSecretCert = "tls.crt"
	// The ID/name for the k8sKey in kubernetes tls secret.
	TLSSecretKey = "tls.key"
	// The ID/name for the CA certificate in kubernetes tls secret
	TLSSecretCaCert = "ca.crt"

	// GatewaySdsCaSuffix is the suffix of the sds resource name for root CA. All resource
	// names for gateway root certs end with "-cacert".
	GatewaySdsCaSuffix = "-cacert"
)

type CredentialsController struct {
	secrets informersv1.SecretInformer
	sar     authorizationv1client.SubjectAccessReviewInterface

	clusterID cluster.ID

	mu                 sync.RWMutex
	authorizationCache map[authorizationKey]authorizationResponse
}

type authorizationKey string

type authorizationResponse struct {
	expiration time.Time
	authorized error
}

var _ credentials.Controller = &CredentialsController{}

func NewCredentialsController(client kube.Client, clusterID cluster.ID) *CredentialsController {
	informer := client.KubeInformer().Core().V1().Secrets().Informer()

	return &CredentialsController{
		secrets: informerAdapter{listersv1.NewSecretLister(informer.GetIndexer()), informer},

		sar:                client.AuthorizationV1().SubjectAccessReviews(),
		clusterID:          clusterID,
		authorizationCache: make(map[authorizationKey]authorizationResponse),
	}
}

func toUser(serviceAccount, namespace string) string {
	return fmt.Sprintf("system:serviceaccount:%s:%s", namespace, serviceAccount)
}

const cacheTTL = time.Minute

// clearExpiredCache iterates through the cache and removes all expired entries. Should be called with mutex held.
func (s *CredentialsController) clearExpiredCache() {
	for k, v := range s.authorizationCache {
		if v.expiration.Before(time.Now()) {
			delete(s.authorizationCache, k)
		}
	}
}

// cachedAuthorization checks the authorization cache
// nolint
func (s *CredentialsController) cachedAuthorization(user string) (error, bool) {
	key := authorizationKey(user)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clearExpiredCache()
	// No need to check expiration, we will evict expired entries above
	got, f := s.authorizationCache[key]
	if !f {
		return nil, false
	}
	return got.authorized, true
}

// cachedAuthorization checks the authorization cache
func (s *CredentialsController) insertCache(user string, response error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := authorizationKey(user)
	expDelta := cacheTTL
	if response == nil {
		// Cache success a bit longer, there is no need to quickly revoke access
		expDelta *= 5
	}
	log.Debugf("cached authorization for user %s: %v", user, response)
	s.authorizationCache[key] = authorizationResponse{
		expiration: time.Now().Add(expDelta),
		authorized: response,
	}
}

// DisableAuthorizationForTest makes the authorization check always pass. Should be used only for tests.
func DisableAuthorizationForTest(fake *fake.Clientset) {
	fake.Fake.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authorizationv1.SubjectAccessReview{
			Status: authorizationv1.SubjectAccessReviewStatus{
				Allowed: true,
			},
		}, nil
	})
}

func (s *CredentialsController) Authorize(serviceAccount, namespace string) error {
	user := toUser(serviceAccount, namespace)
	if cached, f := s.cachedAuthorization(user); f {
		return cached
	}
	resp := func() error {
		resp, err := s.sar.Create(context.Background(), &authorizationv1.SubjectAccessReview{
			ObjectMeta: metav1.ObjectMeta{},
			Spec: authorizationv1.SubjectAccessReviewSpec{
				ResourceAttributes: &authorizationv1.ResourceAttributes{
					Namespace: namespace,
					Verb:      "list",
					Resource:  "secrets",
				},
				User: user,
			},
		}, metav1.CreateOptions{})
		if err != nil {
			return err
		}
		if !resp.Status.Allowed {
			return fmt.Errorf("%s/%s is not authorized to read secrets: %v", serviceAccount, namespace, resp.Status.Reason)
		}
		return nil
	}()
	s.insertCache(user, resp)
	return resp
}

func (s *CredentialsController) GetKeyAndCert(name, namespace string) (key []byte, cert []byte, err error) {
	k8sSecret, err := s.secrets.Lister().Secrets(namespace).Get(name)
	if err != nil {
		return nil, nil, fmt.Errorf("secret %v/%v not found", namespace, name)
	}

	return extractKeyAndCert(k8sSecret)
}

func (s *CredentialsController) GetCaCert(name, namespace string) (cert []byte, err error) {
	strippedName := strings.TrimSuffix(name, GatewaySdsCaSuffix)
	k8sSecret, err := s.secrets.Lister().Secrets(namespace).Get(name)
	if err != nil {
		// Could not fetch cert, look for secret without -cacert suffix
		k8sSecret, caCertErr := s.secrets.Lister().Secrets(namespace).Get(strippedName)
		if caCertErr != nil {
			return nil, fmt.Errorf("secret %v/%v not found", namespace, strippedName)
		}
		return extractRoot(k8sSecret)
	}
	return extractRoot(k8sSecret)
}

func hasKeys(d map[string][]byte, keys ...string) bool {
	for _, k := range keys {
		_, f := d[k]
		if !f {
			return false
		}
	}
	return true
}

func hasValue(d map[string][]byte, keys ...string) bool {
	for _, k := range keys {
		v := d[k]
		if len(v) == 0 {
			return false
		}
	}
	return true
}

// extractKeyAndCert extracts server key, certificate
func extractKeyAndCert(scrt *v1.Secret) (key, cert []byte, err error) {
	if hasValue(scrt.Data, GenericScrtCert, GenericScrtKey) {
		return scrt.Data[GenericScrtKey], scrt.Data[GenericScrtCert], nil
	}
	if hasValue(scrt.Data, TLSSecretCert, TLSSecretKey) {
		return scrt.Data[TLSSecretKey], scrt.Data[TLSSecretCert], nil
	}
	// No cert found. Try to generate a helpful error messsage
	if hasKeys(scrt.Data, GenericScrtCert, GenericScrtKey) {
		return nil, nil, fmt.Errorf("found keys %q and %q, but they were empty", GenericScrtCert, GenericScrtKey)
	}
	if hasKeys(scrt.Data, TLSSecretCert, TLSSecretKey) {
		return nil, nil, fmt.Errorf("found keys %q and %q, but they were empty", TLSSecretCert, TLSSecretKey)
	}
	found := truncatedKeysMessage(scrt.Data)
	return nil, nil, fmt.Errorf("found secret, but didn't have expected keys (%s and %s) or (%s and %s); found: %s",
		GenericScrtCert, GenericScrtKey, TLSSecretCert, TLSSecretKey, found)
}

func truncatedKeysMessage(data map[string][]byte) string {
	keys := []string{}
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) < 3 {
		return strings.Join(keys, ", ")
	}
	return fmt.Sprintf("%s, and %d more...", strings.Join(keys[:3], ", "), len(keys)-3)
}

// extractRoot extracts the root certificate
func extractRoot(scrt *v1.Secret) (cert []byte, err error) {
	if hasValue(scrt.Data, GenericScrtCaCert) {
		return scrt.Data[GenericScrtCaCert], nil
	}
	if hasValue(scrt.Data, TLSSecretCaCert) {
		return scrt.Data[TLSSecretCaCert], nil
	}
	// No cert found. Try to generate a helpful error messsage
	if hasKeys(scrt.Data, GenericScrtCaCert) {
		return nil, fmt.Errorf("found key %q, but it was empty", GenericScrtCaCert)
	}
	if hasKeys(scrt.Data, TLSSecretCaCert) {
		return nil, fmt.Errorf("found key %q, but it was empty", TLSSecretCaCert)
	}
	found := truncatedKeysMessage(scrt.Data)
	return nil, fmt.Errorf("found secret, but didn't have expected keys %s or %s; found: %s",
		GenericScrtCaCert, TLSSecretCaCert, found)
}

func (s *CredentialsController) AddEventHandler(f func(name string, namespace string)) {
	handler := func(obj interface{}) {
		scrt, ok := obj.(*v1.Secret)
		if !ok {
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				if cast, ok := tombstone.Obj.(*v1.Secret); ok {
					scrt = cast
				} else {
					log.Errorf("Failed to convert to tombstoned secret object: %v", obj)
					return
				}
			} else {
				log.Errorf("Failed to convert to secret object: %v", obj)
				return
			}
		}
		f(scrt.Name, scrt.Namespace)
	}
	s.secrets.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				handler(obj)
			},
			UpdateFunc: func(old, cur interface{}) {
				handler(cur)
			},
			DeleteFunc: func(obj interface{}) {
				handler(obj)
			},
		})
}

// informerAdapter allows treating a generic informer as an informersv1.SecretInformer
type informerAdapter struct {
	listersv1.SecretLister
	cache.SharedIndexInformer
}

func (s informerAdapter) Informer() cache.SharedIndexInformer {
	return s.SharedIndexInformer
}

func (s informerAdapter) Lister() listersv1.SecretLister {
	return s.SecretLister
}

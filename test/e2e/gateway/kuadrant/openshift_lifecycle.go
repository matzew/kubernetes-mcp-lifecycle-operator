//go:build e2e && openshift

/*
Copyright 2026 The Kubernetes Authors

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

package kuadrant

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	"sigs.k8s.io/e2e-framework/klient/k8s"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	kuadrantapi "github.com/kubernetes-sigs/mcp-lifecycle-operator/internal/controller/providers/kuadrant/api"
)

const (
	openshiftGatewayClass = "istio"
	openshiftPublicHost   = "mcp.e2e.test"
)

// rhclLifecycle uses the shared RHCL and Istio installations. The fixture
// namespaces can contain shared resources and are left in place.
type rhclLifecycle struct {
	fixtures []k8s.Object
}

func (l *rhclLifecycle) Setup(ctx context.Context, cfg *envconf.Config) (err error) {
	r := cfg.Client().Resources()

	log.Println("waiting for the shared Istio GatewayClass...")
	if err := waitForGatewayClass(ctx, r, openshiftGatewayClass, 5*time.Minute); err != nil {
		return fmt.Errorf("wait for GatewayClass %s: %w", openshiftGatewayClass, err)
	}

	log.Println("waiting for mcp-gateway-controller (provided by RHCL)...")
	if err := waitForDeploymentAvailable(ctx, r, "app.kubernetes.io/name=mcp-gateway,component=controller", 5*time.Minute); err != nil {
		return fmt.Errorf("mcp-gateway-controller not available: %w", err)
	}

	defer func() {
		// RunPackage only tears down lifecycles whose setup completed.
		if err != nil {
			err = errors.Join(err, l.Teardown(context.WithoutCancel(ctx), cfg))
		}
	}()
	if err := l.setupGatewayInfrastructure(ctx, r); err != nil {
		return err
	}

	log.Println("waiting for MCPGatewayExtension to become ready...")
	if err := waitForExtensionReady(ctx, r, "mcp-gateway-extension", mcpSystemNamespace, 5*time.Minute); err != nil {
		return err
	}
	log.Println("setup complete")
	return nil
}

func (l *rhclLifecycle) Teardown(ctx context.Context, cfg *envconf.Config) error {
	return l.deleteFixtures(ctx, cfg.Client().Resources())
}

func (l *rhclLifecycle) createFixture(ctx context.Context, r *resources.Resources, obj k8s.Object) error {
	if err := r.Create(ctx, obj); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}
	l.fixtures = append(l.fixtures, obj)
	return nil
}

func (l *rhclLifecycle) deleteFixtures(ctx context.Context, r *resources.Resources) error {
	var cleanupErr error
	// Reverse creation order: the extension goes first so its finalizer can
	// still access the Gateway and ReferenceGrant.
	for _, fixture := range slices.Backward(l.fixtures) {
		if err := deleteRHCLFixture(ctx, r, fixture); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}
	l.fixtures = nil
	return cleanupErr
}

func deleteRHCLFixture(ctx context.Context, r *resources.Resources, obj k8s.Object) error {
	uid := obj.GetUID()
	if uid == "" {
		return fmt.Errorf("missing UID for fixture %T %s/%s", obj, obj.GetNamespace(), obj.GetName())
	}
	err := r.Delete(ctx, obj, func(opts *metav1.DeleteOptions) {
		opts.Preconditions = &metav1.Preconditions{UID: &uid}
	})
	// NotFound means it is already gone. Conflict means the UID precondition
	// failed: a same-name replacement exists that this run does not own.
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete fixture %T %s/%s: %w", obj, obj.GetNamespace(), obj.GetName(), err)
	}
	// Wait until the object is gone so its finalizers finish before the
	// fixtures it depends on are deleted.
	err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 120*time.Second, true,
		func(ctx context.Context) (bool, error) {
			current, ok := obj.DeepCopyObject().(k8s.Object)
			if !ok {
				return false, fmt.Errorf("fixture %T is not a k8s.Object", obj)
			}
			err := r.Get(ctx, obj.GetName(), obj.GetNamespace(), current)
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			if err != nil {
				// Keep polling through transient API errors.
				return false, nil
			}
			return current.GetUID() != uid, nil
		})
	if err != nil {
		return fmt.Errorf("wait for deletion of fixture %T %s/%s: %w", obj, obj.GetNamespace(), obj.GetName(), err)
	}
	return nil
}

func (l *rhclLifecycle) setupGatewayInfrastructure(ctx context.Context, r *resources.Resources) error {
	log.Println("creating gateway infrastructure (RHCL mode)...")

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: mcpSystemNamespace}}
	if err := r.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create namespace %s: %w", mcpSystemNamespace, err)
	}

	gwNs := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: gatewayNamespace}}
	if err := r.Create(ctx, gwNs); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create namespace %s: %w", gatewayNamespace, err)
	}

	if err := gatewayv1beta1.Install(r.GetScheme()); err != nil {
		return fmt.Errorf("register Gateway API v1beta1 types: %w", err)
	}

	grant := &gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      referenceGrantName,
			Namespace: gatewayNamespace,
		},
		Spec: gatewayv1beta1.ReferenceGrantSpec{
			From: []gatewayv1beta1.ReferenceGrantFrom{
				{
					Group:     "gateway.networking.k8s.io",
					Kind:      "HTTPRoute",
					Namespace: gatewayv1beta1.Namespace(mcpSystemNamespace),
				},
				{
					Group:     "mcp.kuadrant.io",
					Kind:      "MCPGatewayExtension",
					Namespace: gatewayv1beta1.Namespace(mcpSystemNamespace),
				},
			},
			To: []gatewayv1beta1.ReferenceGrantTo{
				{Group: "", Kind: "Service"},
				{Group: "gateway.networking.k8s.io", Kind: "Gateway"},
			},
		},
	}
	if err := l.createFixture(ctx, r, grant); err != nil {
		return fmt.Errorf("create ReferenceGrant: %w", err)
	}

	fromAll := gatewayv1.NamespacesFromAll
	wildcardHost := gatewayv1.Hostname("*.mcp.e2e.test")
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gatewayName,
			Namespace: gatewayNamespace,
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: openshiftGatewayClass,
			Listeners: []gatewayv1.Listener{
				{
					Name:     "mcp",
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
					AllowedRoutes: &gatewayv1.AllowedRoutes{
						Namespaces: &gatewayv1.RouteNamespaces{From: &fromAll},
					},
				},
				{
					Name:     "mcps",
					Hostname: &wildcardHost,
					Port:     8080,
					Protocol: gatewayv1.HTTPProtocolType,
					AllowedRoutes: &gatewayv1.AllowedRoutes{
						Namespaces: &gatewayv1.RouteNamespaces{From: &fromAll},
					},
				},
			},
		},
	}
	if err := l.createFixture(ctx, r, gw); err != nil {
		return fmt.Errorf("create Gateway %s/%s: %w", gatewayNamespace, gatewayName, err)
	}

	if err := kuadrantapi.AddToScheme(r.GetScheme()); err != nil {
		return fmt.Errorf("register Kuadrant types: %w", err)
	}

	ext := &kuadrantapi.MCPGatewayExtension{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "mcp.kuadrant.io/v1",
			Kind:       "MCPGatewayExtension",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mcp-gateway-extension",
			Namespace: mcpSystemNamespace,
		},
		Spec: kuadrantapi.MCPGatewayExtensionSpec{
			PublicHost: openshiftPublicHost,
			TargetRef: kuadrantapi.TargetReference{
				Group:       gatewayv1.GroupName,
				Kind:        "Gateway",
				Name:        gatewayName,
				Namespace:   gatewayNamespace,
				SectionName: "mcp",
			},
		},
	}
	if err := l.createFixture(ctx, r, ext); err != nil {
		return fmt.Errorf("create MCPGatewayExtension: %w", err)
	}

	return nil
}

func waitForDeploymentAvailable(ctx context.Context, r *resources.Resources, labelSelector string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			depList := &unstructured.UnstructuredList{}
			depList.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "apps", Version: "v1", Kind: "DeploymentList",
			})
			if err := r.List(ctx, depList, resources.WithLabelSelector(labelSelector)); err != nil {
				return false, nil
			}
			for i := range depList.Items {
				conditions, _, _ := unstructured.NestedSlice(depList.Items[i].Object, "status", "conditions")
				for _, c := range conditions {
					cond, ok := c.(map[string]any)
					if !ok {
						continue
					}
					if cond["type"] == "Available" && cond["status"] == "True" {
						return true, nil
					}
				}
			}
			return false, nil
		})
}

func waitForGatewayClass(ctx context.Context, r *resources.Resources, name string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			gc := &gatewayv1.GatewayClass{}
			if err := r.Get(ctx, name, "", gc); err != nil {
				return false, nil
			}
			for _, c := range gc.Status.Conditions {
				if c.Type == "Accepted" && c.Status == metav1.ConditionTrue {
					return true, nil
				}
			}
			return false, nil
		})
}

func waitForExtensionReady(ctx context.Context, r *resources.Resources, name, ns string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			ext := &kuadrantapi.MCPGatewayExtension{}
			if err := r.Get(ctx, name, ns, ext); err != nil {
				return false, nil
			}
			for _, c := range ext.Status.Conditions {
				if c.Type == "Ready" && c.Status == metav1.ConditionTrue {
					return true, nil
				}
			}
			return false, nil
		})
}

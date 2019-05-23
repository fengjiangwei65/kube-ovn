package controller

import (
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/api/networking/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	"reflect"

	netv1 "k8s.io/api/networking/v1"
)

func (c *Controller) enqueueAddNp(obj interface{}) {
	if !c.isLeader() {
		return
	}
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	klog.V(3).Infof("enqueue add np %s", key)
	c.addNpQueue.AddRateLimited(key)
}

func (c *Controller) enqueueDeleteNp(obj interface{}) {
	if !c.isLeader() {
		return
	}
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	klog.V(3).Infof("enqueue delete np %s", key)
	c.deleteNpQueue.AddRateLimited(key)
}

func (c *Controller) enqueueUpdateNp(old, new interface{}) {
	if !c.isLeader() {
		return
	}
	oldNp := old.(*v1.NetworkPolicy)
	newNp := new.(*v1.NetworkPolicy)
	if !reflect.DeepEqual(oldNp.Spec, newNp.Spec) {
		var key string
		var err error
		if key, err = cache.MetaNamespaceKeyFunc(new); err != nil {
			utilruntime.HandleError(err)
			return
		}
		klog.V(3).Infof("enqueue update np %s", key)
		c.updateNpQueue.AddRateLimited(key)
	}
}

func (c *Controller) runAddNpWorker() {
	for c.processNextAddNpWorkItem() {
	}
}

func (c *Controller) processNextAddNpWorkItem() bool {
	obj, shutdown := c.addNpQueue.Get()

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.addNpQueue.Done(obj)
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			c.addNpQueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		if err := c.handleAddNp(key); err != nil {
			c.addNpQueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		c.addNpQueue.Forget(obj)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}
	return true
}

func (c *Controller) handleAddNp(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}
	np, err := c.npsLister.NetworkPolicies(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	defer func() {
		if err != nil {
			c.recorder.Eventf(np, corev1.EventTypeWarning, "CreateACLFailed", err.Error())
		}
	}()

	ingressPorts := []netv1.NetworkPolicyPort{}
	egressPorts := []netv1.NetworkPolicyPort{}
	for _, npr := range np.Spec.Ingress {
		ingressPorts = append(ingressPorts, npr.Ports...)
	}
	for _, npr := range np.Spec.Egress {
		egressPorts = append(egressPorts, npr.Ports...)
	}

	pgName := fmt.Sprintf("%s.%s", np.Name, np.Namespace)
	ingressAllowAsName := fmt.Sprintf("%s.%s.ingress.allow", np.Name, np.Namespace)
	ingressExceptAsName := fmt.Sprintf("%s.%s.ingress.except", np.Name, np.Namespace)
	egressAllowAsName := fmt.Sprintf("%s.%s.egress.allow", np.Name, np.Namespace)
	egressExceptAsName := fmt.Sprintf("%s.%s.egress.except", np.Name, np.Namespace)

	if err := c.ovnClient.CreatePortGroup(pgName); err != nil {
		klog.Errorf("failed to create port group for np %s, %v", key, err)
		return err
	}

	if len(np.Spec.Ingress) > 0 {
		if err := c.ovnClient.CreateAddressSet(ingressAllowAsName); err != nil {
			klog.Errorf("failed to create address_set %s, %v", ingressAllowAsName, err)
			return err
		}

		if err := c.ovnClient.CreateAddressSet(ingressExceptAsName); err != nil {
			klog.Errorf("failed to create address_set %s, %v", ingressExceptAsName, err)
			return err
		}

		if err := c.ovnClient.CreateIngressACL(pgName, ingressAllowAsName, ingressExceptAsName, ingressPorts); err != nil {
			klog.Errorf("failed to create ingress acls for np %s, %v", key, err)
			return err
		}
	}

	if len(np.Spec.Egress) > 0 {
		if err := c.ovnClient.CreateAddressSet(egressAllowAsName); err != nil {
			klog.Errorf("failed to create address_set %s, %v", egressAllowAsName, err)
			return err
		}

		if err := c.ovnClient.CreateAddressSet(egressExceptAsName); err != nil {
			klog.Errorf("failed to create address_set %s, %v", egressExceptAsName, err)
			return err
		}

		if err := c.ovnClient.CreateEgressACL(pgName, egressAllowAsName, egressExceptAsName, egressPorts); err != nil {
			klog.Errorf("failed to create egress acls for np %s, %v", key, err)
			return err
		}
	}

	return nil
}

package controller

import (
	"fmt"
	"k8s.io/api/networking/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	"reflect"
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
		klog.V(3).Infof("enqueue delete np %s", key)
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

	pgName := fmt.Sprintf("%s.%s", np.Name, np.Namespace)
	ingressAllowAsName := fmt.Sprintf("%s.%s.ingress.allow", np.Name, np.Namespace)
	exceptAsName := fmt.Sprintf("%s.%s.ingress.except", np.Name, np.Namespace)

	return nil
}

/*
Copyright 2024  .

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

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/magicsong/kidecar/pkg/constants"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/go-logr/logr"
	"github.com/magicsong/kidecar/api"
	"github.com/magicsong/kidecar/pkg/info"
	"gomodules.xyz/jsonpatch/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

var _ Storage = &inKube{}
var rfc6901Encoder = strings.NewReplacer("~", "~0", "/", "~1")

type inKube struct {
	log     logr.Logger
	dynamic dynamic.Interface
	kubernetes.Interface
}

// IsInitialized implements Storage.
func (c *inKube) IsInitialized() bool {
	return c.Interface != nil
}

// SetupWithManager implements Storage.
func (c *inKube) SetupWithManager(mgr api.SidecarManager) error {
	dynClient, err := dynamic.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}
	c.log = mgr.GetLogger().WithName("in_kube")
	c.dynamic = dynClient
	c.Interface = mgr
	return nil
}

func (c *inKube) storeInCurrentPod(data string, config *InKubeConfig) error {
	if config.AnnotationKey == nil && config.LabelKey == nil {
		if len(config.MarkerPolices) == 0 {
			return nil
		}
		for _, m := range config.MarkerPolices {
			if m.GameServerOpsState != "" {
				return nil
			}
		}
	}

	currentPod, err := info.GetCurrentPod()
	if err != nil {
		return fmt.Errorf("failed to get current pod: %w", err)
	}
	c.log.Info("store data in current pod", "data", data, "name", currentPod.Name)
	defer c.log.Info("store data done", "data", data, "pod", currentPod.Name)
	// get pod
	metadata := make(map[string]interface{})
	patchData := map[string]interface{}{
		"metadata": map[string]interface{}{},
	}
	annotaions := make(map[string]interface{})
	labels := make(map[string]interface{})
	if config.AnnotationKey != nil {
		annotaions[*config.AnnotationKey] = data
	}
	if config.LabelKey != nil {
		labels[*config.LabelKey] = data
	}
	if policy, ok := config.GetPolicyOfState(data); ok {
		if len(policy.Annotations) > 0 {
			for key, value := range policy.Annotations {
				annotaions[key] = value
			}
		}
		if len(policy.Labels) > 0 {
			for key, value := range policy.Labels {
				labels[key] = value
			}
		}
	}
	if len(annotaions) > 0 {
		metadata["annotations"] = annotaions
	}
	if len(labels) > 0 {
		metadata["labels"] = labels
	}
	patchData["metadata"] = metadata
	patchBytes, _ := json.Marshal(patchData)
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err = c.CoreV1().Pods(currentPod.Namespace).Patch(context.Background(), currentPod.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to patch pod after mant retries: %w", err)
	}
	return nil
}

// Store implements Storage.
func (c *inKube) Store(data string, config interface{}) error {
	myconfig, ok := config.(*InKubeConfig)
	if !ok || myconfig == nil {
		return fmt.Errorf("invalid in kube config type")
	}
	c.log.Info("store data", "data", data, "inKube", myconfig)
	defer c.log.Info("store data done", "data", data)
	if err := myconfig.IsValid(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	myconfig.Preprocess()
	if err := c.storeInCurrentPod(data, myconfig); err != nil {
		return fmt.Errorf("failed to store in current pod: %w", err)
	}

	if err := c.storeProbeInGameServer(data, myconfig); err != nil {
		return fmt.Errorf("failed to store in gameserver: %w", err)
	}

	return c.storeInOtherObject(data, myconfig)
}

func (c *inKube) storeProbeInGameServer(data string, config *InKubeConfig) error {
	if len(config.MarkerPolices) == 0 {
		return nil
	}
	for _, policy := range config.MarkerPolices {
		if policy.GameServerOpsState == "" {
			return nil
		}
	}

	// store probe result in gameserver
	gvr := schema.GroupVersionResource{
		Group:    constants.GameServersGroup,
		Version:  constants.GameServersVersion,
		Resource: constants.GameServersResource,
	}
	ns, err := info.GetCurrentPodNamespaceAndName()
	if err != nil {
		return fmt.Errorf("failed to get current pod namespace and name: %w", err)
	}
	patch := generatePatch(data, config)
	patchBytes, _ := json.Marshal(patch)

	c.log.Info("store data in gameservers object", "data", data, "patch", string(patchBytes))

	_, err = c.dynamic.Resource(gvr).Namespace(ns.Namespace).Patch(context.TODO(), ns.Name, types.JSONPatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch inKube: %w", err)
	}
	return nil

}

func (c *inKube) storeInOtherObject(data string, myconfig *InKubeConfig) error {
	if myconfig.Target == nil || len(myconfig.MarkerPolices) < 1 {
		return nil
	}
	gvr := myconfig.Target.ToGvr()
	c.log.Info("store data in other object", "data", data, "inKube", myconfig, "gvr", gvr)
	patch := generatePatch(data, myconfig)
	patchBytes, _ := json.Marshal(patch)
	c.log.Info("patch inKube", "inKube", myconfig, "patch", string(patchBytes), "gvr", gvr)
	_, err := c.dynamic.Resource(gvr).Namespace(myconfig.Target.Namespace).Patch(context.TODO(), myconfig.Target.Name, types.JSONPatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch inKube: %w", err)
	}
	return nil
}

func generatePatch(data string, myconfig *InKubeConfig) []jsonpatch.JsonPatchOperation {
	patch := []jsonpatch.JsonPatchOperation{}
	if myconfig.AnnotationKey != nil {
		patch = append(patch, jsonpatch.NewOperation("replace", "/metadata/annotations/"+rfc6901Encoder.Replace(*myconfig.AnnotationKey), data))
	}
	if myconfig.LabelKey != nil {
		patch = append(patch, jsonpatch.NewOperation("replace", "/metadata/labels/"+rfc6901Encoder.Replace(*myconfig.LabelKey), data))
	}
	if policy, ok := myconfig.GetPolicyOfState(data); ok {
		if len(policy.Annotations) > 0 {
			for key, value := range policy.Annotations {
				patch = append(patch, jsonpatch.NewOperation("replace", "/metadata/annotations/"+rfc6901Encoder.Replace(key), value))
			}
		}
		if len(policy.Labels) > 0 {
			for key, value := range policy.Labels {
				patch = append(patch, jsonpatch.NewOperation("replace", "/metadata/labels/"+rfc6901Encoder.Replace(key), value))
			}
		}
		if len(policy.JsonPathConfigs) > 0 {
			for _, jsonPathConfig := range policy.JsonPathConfigs {
				patch = append(patch, jsonpatch.NewOperation("replace", jsonPathConfig.JSONPath, jsonPathConfig.Value))
			}
		}
	}
	if myconfig.JsonPath != nil {
		patch = append(patch, jsonpatch.NewOperation("replace", *myconfig.JsonPath, data))
	}
	return patch
}

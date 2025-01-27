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
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

type StorageType string

const (
	// StorageTypeInKube represent store in kube object
	StorageTypeInKube     StorageType = "InKube"
	StorageTypeHTTPMetric StorageType = "HTTPMetric"
)

// InKubeConfig is the configuration for storing data in kube object
type InKubeConfig struct {
	// Target is the target kube object, if is empty, means current pod
	Target        *TargetKubeObject   `json:"target,omitempty"`
	JsonPath      *string             `json:"jsonPath,omitempty"`      // The path in JsonPatch
	AnnotationKey *string             `json:"annotationKey,omitempty"` // Pod Anno Key
	LabelKey      *string             `json:"labelKey,omitempty"`      // Pod Label Key
	MarkerPolices []ProbeMarkerPolicy `json:"markerPolices,omitempty"` // Configurations applicable to ProbeMarker
	// inner field
	policyMap   map[string]ProbeMarkerPolicy
	preprocessd bool
}

// TargetKubeObject is the target kube object
type TargetKubeObject struct {
	Group     string `json:"group,omitempty"`
	Version   string `json:"version"`
	Resource  string `json:"resource"`
	Namespace string `json:"namespace,omitempty" parse:"true"`
	Name      string `json:"name" parse:"true"`
	PodOwner  bool   `json:"podOwner,omitempty"` // Whether it is the owner of the Pod
}
type HTTPMetricConfig struct {
	MetricName string `json:"metricName"`
}

type StorageConfig struct {
	Type       StorageType       `json:"type"`
	InKube     *InKubeConfig     `json:"inKube,omitempty"`
	HTTPMetric *HTTPMetricConfig `json:"httpMetric,omitempty"`
}

// ProbeMarkerPolicy convert prob value to user defined values
type ProbeMarkerPolicy struct {
	// probe status,
	// For example: State=Succeeded, annotations[controller.kubernetes.io/pod-deletion-cost] = '10'.
	// State=Failed, annotations[controller.kubernetes.io/pod-deletion-cost] = '-10'.
	// In addition, if State=Failed is not defined, probe execution fails, and the annotations[controller.kubernetes.io/pod-deletion-cost] will be Deleted
	State              string `json:"state"`
	GameServerOpsState string `json:"gameServerOpsState"`
	// Patch Labels pod.labels
	Labels map[string]string `json:"labels,omitempty"`
	// Patch annotations pod.annotations
	Annotations map[string]string `json:"annotations,omitempty"`
	// Patch JSONPath
	JsonPathConfigs []JSONPathConfig `json:"jsonPathConfigs,omitempty"`
}

type FieldType string

type JSONPathConfig struct {
	JSONPath  string      `json:"jsonPath"`  // JSONPath 表达式
	FieldType FieldType   `json:"fieldType"` // 提取结果的数据类型
	Value     interface{} `json:"value"`     // 填的值
}

func (s *StorageConfig) StoreData(factory StorageFactory, data string) error {
	storage, err := factory.GetStorage(s.Type)
	if err != nil {
		return fmt.Errorf("failed to get storage: %w", err)
	}
	switch s.Type {
	case StorageTypeInKube:
		return storage.Store(data, s.InKube)
	case StorageTypeHTTPMetric:
		return storage.Store(data, s.HTTPMetric)
	default:
		return fmt.Errorf("unsupported storage type: %s", s.Type)
	}
}

func (t *TargetKubeObject) IsValid() error {
	if t.Version == "" {
		return fmt.Errorf("invalid version")
	}
	if t.Resource == "" {
		return fmt.Errorf("invalid resource")
	}
	if t.Name == "" {
		return fmt.Errorf("invalid name")
	}
	return nil
}

func (t *TargetKubeObject) ToGvr() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    t.Group,
		Version:  t.Version,
		Resource: t.Resource,
	}
}

func (c *InKubeConfig) IsValid() error {
	if c.Target != nil {
		if err := c.Target.IsValid(); err != nil {
			return fmt.Errorf("invalid target: %w", err)
		}
	}
	if c.JsonPath == nil && c.AnnotationKey == nil && c.LabelKey == nil && len(c.MarkerPolices) == 0 {
		return fmt.Errorf("invalid annotationKey or labelKey or markerPolices")
	}
	return nil
}

func (c *InKubeConfig) buildPolicyMap() {
	if len(c.MarkerPolices) < 1 {
		return
	}
	if c.preprocessd {
		return
	}
	c.policyMap = make(map[string]ProbeMarkerPolicy)
	for _, policy := range c.MarkerPolices {
		c.policyMap[policy.State] = policy
	}
}

func (c *InKubeConfig) GetPolicyOfState(state string) (*ProbeMarkerPolicy, bool) {
	if len(c.policyMap) < 1 {
		return nil, false
	}
	p, ok := c.policyMap[state]
	if !ok {
		return nil, false
	}
	return &p, true
}

func (c *InKubeConfig) Preprocess() {
	if c.preprocessd {
		return
	}
	c.buildPolicyMap()
	c.preprocessd = true
}

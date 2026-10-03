/*
Copyright 2024 The Kubernetes Authors.

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

package vcloud

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/informers"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider/builder"
	coreoptions "sigs.k8s.io/cluster-autoscaler/pkg/core/options"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/gpu"
)

// ProviderName is the cloud provider name for VCloud
const ProviderName = "vcloud"

func init() {
	builder.RegisterCloudProvider(ProviderName, func(opts *coreoptions.AutoscalerOptions, do cloudprovider.NodeGroupDiscoveryOptions, rl *cloudprovider.ResourceLimiter, _ informers.SharedInformerFactory) cloudprovider.CloudProvider {
		return BuildVcloud(opts, do, rl)
	})
	builder.SetDefaultCloudProvider(ProviderName)
}

var _ cloudprovider.CloudProvider = (*vcloudCloudProvider)(nil)

const (
	// GPULabel is the label added to nodes with GPU resource
	GPULabel = "k8s.io.infra.vnetwork.io/gpu-node"
)

// vcloudCloudProvider implements CloudProvider interface for VCloud
type vcloudCloudProvider struct {
	manager         *EnhancedManager
	resourceLimiter *cloudprovider.ResourceLimiter

	// Cache for node-to-nodegroup mapping
	nodeGroupCache      map[string]cloudprovider.NodeGroup
	nodeGroupCacheTime  time.Time
	nodeGroupCacheMutex sync.RWMutex
}

func newVcloudCloudProvider(manager *EnhancedManager, rl *cloudprovider.ResourceLimiter) *vcloudCloudProvider {
	return &vcloudCloudProvider{
		manager:         manager,
		resourceLimiter: rl,
		nodeGroupCache:  make(map[string]cloudprovider.NodeGroup),
	}
}

// Name returns name of the cloud provider
func (v *vcloudCloudProvider) Name() string {
	return ProviderName
}

// NodeGroups returns all node groups configured for this cloud provider
func (v *vcloudCloudProvider) NodeGroups(ctx context.Context) []cloudprovider.NodeGroup {
	nodeGroups := make([]cloudprovider.NodeGroup, len(v.manager.nodeGroups))
	for i, ng := range v.manager.nodeGroups {
		nodeGroups[i] = ng
	}
	return nodeGroups
}

// NodeGroupForNode returns the node group for the given node
func (v *vcloudCloudProvider) NodeGroupForNode(ctx context.Context, node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	providerID := node.Spec.ProviderID

	klog.V(5).Infof("checking nodegroup for node with provider ID: %q", providerID)

	// Extract instance ID from provider ID
	instanceID, err := fromProviderID(providerID)
	if err != nil {
		klog.V(4).Infof("failed to parse provider ID %q: %v", providerID, err)
		return nil, nil
	}

	// Check cache first
	v.nodeGroupCacheMutex.RLock()
	if time.Since(v.nodeGroupCacheTime) < 30*time.Second {
		if cachedGroup, found := v.nodeGroupCache[providerID]; found {
			klog.V(5).Infof("found cached node group %q for instance %q", cachedGroup.Id(), instanceID)
			v.nodeGroupCacheMutex.RUnlock()
			return cachedGroup, nil
		}
	}
	v.nodeGroupCacheMutex.RUnlock()

	// Cache miss or stale, search through all node groups
	v.nodeGroupCacheMutex.Lock()
	defer v.nodeGroupCacheMutex.Unlock()

	// Double-check cache after acquiring write lock
	if time.Since(v.nodeGroupCacheTime) < 30*time.Second {
		if cachedGroup, found := v.nodeGroupCache[providerID]; found {
			klog.V(5).Infof("found cached node group (double-check) %q for instance %q", cachedGroup.Id(), instanceID)
			return cachedGroup, nil
		}
	}

	// If cache is stale, rebuild it
	if time.Since(v.nodeGroupCacheTime) >= 30*time.Second {
		klog.V(4).Infof("rebuilding node group cache")
		v.nodeGroupCache = make(map[string]cloudprovider.NodeGroup)

		// Only update cache time if we have node groups to work with
		if len(v.manager.nodeGroups) > 0 {
			v.nodeGroupCacheTime = time.Now()

			// Build cache for all instances
			for _, group := range v.manager.nodeGroups {
				nodes, err := group.Nodes(ctx)
				if err != nil {
					klog.V(4).Infof("failed to get nodes for group %q: %v", group.Id(), err)
					continue
				}

				for _, instance := range nodes {
					v.nodeGroupCache[instance.Id] = group
				}
			}
		} else {
			klog.V(4).Infof("no node groups available yet, skipping cache rebuild")
		}
	}

	// Now check the cache for our node
	if group, found := v.nodeGroupCache[providerID]; found {
		klog.V(4).Infof("found node group %q for instance %q", group.Id(), instanceID)
		return group, nil
	}

	// If cache is empty and we have node groups, fall back to direct search
	if len(v.nodeGroupCache) == 0 && len(v.manager.nodeGroups) > 0 {
		klog.V(4).Infof("cache empty, falling back to direct search for instance %q", instanceID)
		for _, group := range v.manager.nodeGroups {
			nodes, err := group.Nodes(ctx)
			if err != nil {
				klog.V(4).Infof("failed to get nodes for group %q: %v", group.Id(), err)
				continue
			}

			for _, instance := range nodes {
				if instance.Id == providerID {
					klog.V(4).Infof("found node group %q for instance %q (fallback)", group.Id(), instanceID)
					// Update cache with this finding
					v.nodeGroupCache[instance.Id] = group
					return group, nil
				}
			}
		}
	}

	klog.V(4).Infof("no node group found for instance %q", instanceID)
	return nil, nil
}

// HasInstance returns whether a given node has a corresponding instance in this cloud provider
func (v *vcloudCloudProvider) HasInstance(ctx context.Context, node *apiv1.Node) (bool, error) {
	return true, cloudprovider.ErrNotImplemented
}

// Pricing returns pricing model for this cloud provider
func (v *vcloudCloudProvider) Pricing(ctx context.Context) (cloudprovider.PricingModel, errors.AutoscalerError) {
	return nil, cloudprovider.ErrNotImplemented
}

// GetAvailableMachineTypes gets all machine types that can be requested from the cloud provider
func (v *vcloudCloudProvider) GetAvailableMachineTypes(ctx context.Context) ([]string, error) {
	return []string{}, nil
}

// NewNodeGroup builds a theoretical node group based on the node definition provided
func (v *vcloudCloudProvider) NewNodeGroup(
	ctx context.Context,
	machineType string,
	labels map[string]string,
	systemLabels map[string]string,
	taints []apiv1.Taint,
	extraResources map[string]resource.Quantity,
) (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// GetResourceLimiter returns struct containing limits (max, min) for resources
func (v *vcloudCloudProvider) GetResourceLimiter(ctx context.Context) (*cloudprovider.ResourceLimiter, error) {
	return v.resourceLimiter, nil
}

// GPULabel returns the label added to nodes with GPU resource
func (v *vcloudCloudProvider) GPULabel(ctx context.Context) string {
	return GPULabel
}

// GetAvailableGPUTypes returns all available GPU types cloud provider supports
func (v *vcloudCloudProvider) GetAvailableGPUTypes(ctx context.Context) map[string]struct{} {
	return nil
}

// GetNodeGpuConfig returns the label, type and resource name for the GPU added to node
func (v *vcloudCloudProvider) GetNodeGpuConfig(ctx context.Context, node *apiv1.Node) *cloudprovider.GpuConfig {
	return gpu.GetNodeGPUFromCloudProvider(ctx, v, node)
}

// Cleanup cleans up open resources before the cloud provider is destroyed
func (v *vcloudCloudProvider) Cleanup(ctx context.Context) error {
	return nil
}

// Refresh is called before every main loop and can be used to dynamically update cloud provider state
func (v *vcloudCloudProvider) Refresh(ctx context.Context) error {
	klog.V(4).Info("refreshing VCloud node groups")

	// Invalidate node group cache when refreshing
	v.nodeGroupCacheMutex.Lock()
	v.nodeGroupCache = make(map[string]cloudprovider.NodeGroup)
	v.nodeGroupCacheTime = time.Time{}
	v.nodeGroupCacheMutex.Unlock()

	return v.manager.Refresh(ctx)
}

// BuildVcloud builds the VCloud cloud provider
func BuildVcloud(
	opts *coreoptions.AutoscalerOptions,
	do cloudprovider.NodeGroupDiscoveryOptions,
	rl *cloudprovider.ResourceLimiter,
) cloudprovider.CloudProvider {
	var configFile io.ReadCloser
	if opts.CloudConfig != "" {
		var err error
		configFile, err = os.Open(opts.CloudConfig)
		if err != nil {
			klog.Fatalf("couldn't open cloud provider configuration %s: %#v", opts.CloudConfig, err)
		}
		defer configFile.Close()
	}

	manager, err := newEnhancedManager(configFile)
	if err != nil {
		klog.Fatalf("failed to create VCloud manager: %v", err)
	}

	// Validate configuration
	if err := manager.ValidateConfig(); err != nil {
		klog.Fatalf("invalid VCloud configuration: %v", err)
	}

	provider := newVcloudCloudProvider(manager, rl)

	klog.V(2).Infof("VCloud cloud provider initialized successfully with proven NodePool APIs")
	return provider
}

/*
Copyright AppsCode Inc. and Contributors

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

package v1alpha2

import (
	"fmt"
	"strings"

	"kubedb.dev/apimachinery/apis/kubedb"
	"kubedb.dev/apimachinery/crds"

	promapi "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kmapi "kmodules.xyz/client-go/api/v1"
	"kmodules.xyz/client-go/apiextensions"
	meta_util "kmodules.xyz/client-go/meta"
	appcat "kmodules.xyz/custom-resources/apis/appcatalog/v1alpha1"
	mona "kmodules.xyz/monitoring-agent-api/api/v1"
)

func (k *KafkaConnectCluster) CustomResourceDefinition() *apiextensions.CustomResourceDefinition {
	return crds.MustCustomResourceDefinition(SchemeGroupVersion.WithResource(ResourcePluralKafkaConnectCluster))
}

func (k *KafkaConnectCluster) AsOwner() *meta.OwnerReference {
	return meta.NewControllerRef(k, SchemeGroupVersion.WithKind(ResourceKindKafkaConnectCluster))
}

func (k *KafkaConnectCluster) ResourceShortCode() string {
	return ResourceCodeKafkaConnectCluster
}

func (k *KafkaConnectCluster) ResourceKind() string {
	return ResourceKindKafkaConnectCluster
}

func (k *KafkaConnectCluster) ResourceSingular() string {
	return ResourceSingularKafkaConnectCluster
}

func (k *KafkaConnectCluster) ResourcePlural() string {
	return ResourcePluralKafkaConnectCluster
}

func (k *KafkaConnectCluster) ResourceFQN() string {
	return fmt.Sprintf("%s.%s", k.ResourcePlural(), kubedb.GroupName)
}

// Owner returns owner reference to resources
func (k *KafkaConnectCluster) Owner() *meta.OwnerReference {
	return meta.NewControllerRef(k, SchemeGroupVersion.WithKind(k.ResourceKind()))
}

func (k *KafkaConnectCluster) OffshootName() string {
	return k.Name
}

func (k *KafkaConnectCluster) ServiceName() string {
	return k.OffshootName()
}

func (k *KafkaConnectCluster) GoverningServiceName() string {
	return meta_util.NameWithSuffix(k.ServiceName(), "pods")
}

func (k *KafkaConnectCluster) offshootLabels(selector, override map[string]string) map[string]string {
	selector[meta_util.ComponentLabelKey] = ComponentDatabase
	return meta_util.FilterKeys(kubedb.GroupName, selector, meta_util.OverwriteKeys(nil, k.Labels, override))
}

func (k *KafkaConnectCluster) OffshootSelectors(extraSelectors ...map[string]string) map[string]string {
	selector := map[string]string{
		meta_util.NameLabelKey:      k.ResourceFQN(),
		meta_util.InstanceLabelKey:  k.Name,
		meta_util.ManagedByLabelKey: kubedb.GroupName,
	}
	return meta_util.OverwriteKeys(selector, extraSelectors...)
}

func (k *KafkaConnectCluster) OffshootLabels() map[string]string {
	return k.offshootLabels(k.OffshootSelectors(), nil)
}

func (k *KafkaConnectCluster) ServiceLabels(alias ServiceAlias, extraLabels ...map[string]string) map[string]string {
	svcTemplate := GetServiceTemplate(k.Spec.ServiceTemplates, alias)
	return k.offshootLabels(meta_util.OverwriteKeys(k.OffshootSelectors(), extraLabels...), svcTemplate.Labels)
}

func (k *KafkaConnectCluster) PodControllerLabels(extraLabels ...map[string]string) map[string]string {
	return k.offshootLabels(meta_util.OverwriteKeys(k.OffshootSelectors(), extraLabels...), k.Spec.PodTemplate.Controller.Labels)
}

type kafkaConnectClusterStatsService struct {
	*KafkaConnectCluster
}

func (ks kafkaConnectClusterStatsService) TLSConfig() *promapi.TLSConfig {
	return nil
}

func (ks kafkaConnectClusterStatsService) GetNamespace() string {
	return ks.KafkaConnectCluster.GetNamespace()
}

func (ks kafkaConnectClusterStatsService) ServiceName() string {
	return ks.OffshootName() + "-stats"
}

func (ks kafkaConnectClusterStatsService) ServiceMonitorName() string {
	return ks.ServiceName()
}

func (ks kafkaConnectClusterStatsService) ServiceMonitorAdditionalLabels() map[string]string {
	return ks.OffshootLabels()
}

func (ks kafkaConnectClusterStatsService) Path() string {
	return DefaultStatsPath
}

func (ks kafkaConnectClusterStatsService) Scheme() string {
	return ""
}

func (k *KafkaConnectCluster) StatsService() mona.StatsAccessor {
	return &kafkaConnectClusterStatsService{k}
}

func (k *KafkaConnectCluster) StatsServiceLabels() map[string]string {
	return k.ServiceLabels(StatsServiceAlias, map[string]string{LabelRole: RoleStats})
}

func (k *KafkaConnectCluster) PodLabels(extraLabels ...map[string]string) map[string]string {
	return k.offshootLabels(meta_util.OverwriteKeys(k.OffshootSelectors(), extraLabels...), k.Spec.PodTemplate.Labels)
}

func (k *KafkaConnectCluster) StatefulSetName() string {
	return k.OffshootName()
}

func (k *KafkaConnectCluster) ConfigSecretName() string {
	return meta_util.NameWithSuffix(k.OffshootName(), "config")
}

func (k *KafkaConnectCluster) KafkaClientCredentialsSecretName() string {
	return meta_util.NameWithSuffix(k.Name, "kafka-client-cred")
}

func (k *KafkaConnectCluster) DefaultUserCredSecretName(username string) string {
	return meta_util.NameWithSuffix(k.Name, strings.ReplaceAll(fmt.Sprintf("%s-cred", username), "_", "-"))
}

func (k *KafkaConnectCluster) DefaultKeystoreCredSecretName() string {
	return meta_util.NameWithSuffix(k.Name, strings.ReplaceAll("connect-keystore-cred", "_", "-"))
}

// CertificateName returns the default certificate name and/or certificate secret name for a certificate alias
func (k *KafkaConnectCluster) CertificateName(alias KafkaConnectClusterCertificateAlias) string {
	return meta_util.NameWithSuffix(k.Name, fmt.Sprintf("%s-connect-cert", string(alias)))
}

// GetCertSecretName returns the secret name for a certificate alias if any,
// otherwise returns default certificate secret name for the given alias.
func (k *KafkaConnectCluster) GetCertSecretName(alias KafkaConnectClusterCertificateAlias) string {
	if k.Spec.TLS != nil {
		name, ok := kmapi.GetCertificateSecretName(k.Spec.TLS.Certificates, string(alias))
		if ok {
			return name
		}
	}
	return k.CertificateName(alias)
}

func (k *KafkaConnectCluster) PVCName(alias string) string {
	return meta_util.NameWithSuffix(k.Name, alias)
}

func (k *KafkaConnectCluster) SetDefaults() {
	if k.Spec.TerminationPolicy == "" {
		k.Spec.TerminationPolicy = TerminationPolicyDelete
	}
}

type KafkaConnectClusterApp struct {
	*KafkaConnectCluster
}

func (r KafkaConnectClusterApp) Name() string {
	return r.KafkaConnectCluster.Name
}

func (r KafkaConnectClusterApp) Type() appcat.AppType {
	return appcat.AppType(fmt.Sprintf("%s/%s", kubedb.GroupName, ResourceSingularKafkaConnectCluster))
}

func (k *KafkaConnectCluster) AppBindingMeta() appcat.AppBindingMeta {
	return &KafkaConnectClusterApp{k}
}

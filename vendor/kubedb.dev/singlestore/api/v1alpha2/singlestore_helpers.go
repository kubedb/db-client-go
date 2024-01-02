package v1alpha2

import (
	"fmt"
	"gomodules.xyz/pointer"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kmapi "kmodules.xyz/client-go/api/v1"
	metautil "kmodules.xyz/client-go/meta"
	appcat "kmodules.xyz/custom-resources/apis/appcatalog/v1alpha1"
	ofst "kmodules.xyz/offshoot-api/api/v2"
	"kubedb.dev/apimachinery/apis/kubedb"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"kubedb.dev/singlestore/api/catalog/v1alpha1"
	"strings"
)

type singlestoreApp struct {
	*Singlestore
}

func (s singlestoreApp) Name() string {
	return s.Singlestore.Name
}

func (s singlestoreApp) Type() appcat.AppType {
	return appcat.AppType(fmt.Sprintf("%s/%s", kubedb.GroupName, ResourceSingularSinglestore))
}

func (s *Singlestore) ResourceKind() string {
	return ResourceKindSinglestore
}

func (s *Singlestore) ResourcePlural() string {
	return ResourcePluralSinglestore
}

func (s *Singlestore) ResourceFQN() string {
	return fmt.Sprintf("%s.%s", s.ResourcePlural(), kubedb.GroupName)
}

// Owner returns owner reference to resources
func (s *Singlestore) Owner() *meta.OwnerReference {
	return meta.NewControllerRef(s, api.SchemeGroupVersion.WithKind(s.ResourceKind()))
}

func (s *Singlestore) OffshootName() string {
	return s.Name
}

func (s *Singlestore) ServiceName() string {
	return s.OffshootName()
}

func (s *Singlestore) AppBindingMeta() appcat.AppBindingMeta {
	return &singlestoreApp{s}
}

func (s *Singlestore) StandbyServiceName() string {
	return metautil.NameWithPrefix(s.ServiceName(), "standby")
}

func (s *Singlestore) GoverningServiceName() string {
	return metautil.NameWithSuffix(s.ServiceName(), "pods")
}

func (s *Singlestore) PrimaryServiceDNS() string {
	return fmt.Sprintf("%s.%s.svc", s.ServiceName(), s.Namespace)
}

func (s *Singlestore) DefaultUserCredSecretName(username string) string {
	return metautil.NameWithSuffix(s.Name, strings.ReplaceAll(fmt.Sprintf("%s-cred", username), "_", "-"))
}

func (s *Singlestore) offshootLabels(selector, override map[string]string) map[string]string {
	selector[metautil.ComponentLabelKey] = ComponentDatabase
	return metautil.FilterKeys(kubedb.GroupName, selector, metautil.OverwriteKeys(nil, s.Labels, override))
}

func (s *Singlestore) ServiceLabels(alias api.ServiceAlias, extraLabels ...map[string]string) map[string]string {
	svcTemplate := api.GetServiceTemplate(s.Spec.ServiceTemplates, alias)
	return s.offshootLabels(metautil.OverwriteKeys(s.OffshootSelectors(), extraLabels...), svcTemplate.Labels)
}

func (s *Singlestore) OffshootLabels() map[string]string {
	return s.offshootLabels(s.OffshootSelectors(), nil)
}

func (s *Singlestore) GetNameSpacedName() string {
	return s.Namespace + "/" + s.Name
}

func (s *Singlestore) OffshootSelectors(extraSelectors ...map[string]string) map[string]string {
	selector := map[string]string{
		metautil.NameLabelKey:      s.ResourceFQN(),
		metautil.InstanceLabelKey:  s.Name,
		metautil.ManagedByLabelKey: kubedb.GroupName,
	}
	return metautil.OverwriteKeys(selector, extraSelectors...)
}

func (s *Singlestore) GetSecurityContext(sdbVersion *v1alpha1.SinglestoreVersion) *core.PodSecurityContext {
	runAsUser := sdbVersion.Spec.SecurityContext.RunAsUser
	if runAsUser == nil {
		runAsUser = pointer.Int64P(SinglestoreRunAsUser)
	}
	return &core.PodSecurityContext{
		FSGroup: runAsUser,
	}

}

func (s *Singlestore) IsClustering() bool {
	return s.Spec.Topology != nil
}

func (s *Singlestore) IsStandalone() bool {
	return s.Spec.Topology == nil
}

func (s *Singlestore) PVCName(alias string) string {
	return metautil.NameWithSuffix(s.OffshootName(), alias)
	//return s.OffshootName()
}

func (s *Singlestore) AggregatorStatefulSet() string {
	return metautil.NameWithSuffix(s.OffshootName(), StatefulSetTypeMasterAggregator)
}

func (s *Singlestore) LeafStatefulSet() string {
	return metautil.NameWithSuffix(s.OffshootName(), StatefulSetTypeLeaf)
}

func (s *Singlestore) PodLabels(extraLabels ...map[string]string) map[string]string {
	return s.offshootLabels(metautil.OverwriteKeys(s.OffshootSelectors(), extraLabels...), s.Spec.PodTemplate.Labels)
}

func (s *Singlestore) PodLabel(podTemplate *ofst.PodTemplateSpec) map[string]string {
	if podTemplate != nil && podTemplate.Labels != nil {
		return s.offshootLabels(s.OffshootSelectors(), s.Spec.PodTemplate.Labels)
	}
	return s.offshootLabels(s.OffshootSelectors(), nil)
}

func (s *Singlestore) ConfigSecretName() string {
	return metautil.NameWithSuffix(s.OffshootName(), "config")
}

func (s *Singlestore) StatefulSetName() string {
	return s.OffshootName()
}

func (s *Singlestore) ServiceAccountName() string {
	return s.OffshootName()
}

func (s *Singlestore) PodControllerLabels(extraLabels ...map[string]string) map[string]string {
	return s.offshootLabels(metautil.OverwriteKeys(s.OffshootSelectors(), extraLabels...), s.Spec.PodTemplate.Controller.Labels)
}

func (s *Singlestore) PodControllerLabel(podTemplate *ofst.PodTemplateSpec) map[string]string {
	if podTemplate != nil && &podTemplate.Controller != nil && podTemplate.Controller.Labels != nil {
		return s.offshootLabels(s.OffshootSelectors(), podTemplate.Controller.Labels)
	}
	return s.offshootLabels(s.OffshootSelectors(), nil)
}

func (s *Singlestore) SetHealthCheckerDefaults() {
	if s.Spec.HealthChecker.PeriodSeconds == nil {
		s.Spec.HealthChecker.PeriodSeconds = pointer.Int32P(10)
	}
	if s.Spec.HealthChecker.TimeoutSeconds == nil {
		s.Spec.HealthChecker.TimeoutSeconds = pointer.Int32P(10)
	}
	if s.Spec.HealthChecker.FailureThreshold == nil {
		s.Spec.HealthChecker.FailureThreshold = pointer.Int32P(1)
	}
}

func (s *Singlestore) GetAuthSecretName() string {
	if s.Spec.AuthSecret != nil && s.Spec.AuthSecret.Name != "" {
		return s.Spec.AuthSecret.Name
	}
	return metautil.NameWithSuffix(s.OffshootName(), "auth")
}

func (s *Singlestore) SetDefaults() {
	if s == nil {
		return
	}
	if s.Spec.StorageType == "" {
		s.Spec.StorageType = api.StorageTypeDurable
	}
	if s.Spec.TerminationPolicy == "" {
		s.Spec.TerminationPolicy = api.TerminationPolicyDelete
	}

	if s.Spec.Topology == nil {
		if s.Spec.Replicas == nil {
			s.Spec.Replicas = pointer.Int32P(1)
		}
	} else {
		if s.Spec.Topology.Aggregator.Replicas == nil {
			s.Spec.Topology.Aggregator.Replicas = pointer.Int32P(3)
		}

		if s.Spec.Topology.Leaf.Replicas == nil {
			s.Spec.Topology.Leaf.Replicas = pointer.Int32P(2)
		}
	}

	s.SetTLSDefaults()
	s.SetHealthCheckerDefaults()
}

func (s *Singlestore) SetTLSDefaults() {
	if s.Spec.TLS == nil || s.Spec.TLS.IssuerRef == nil {
		return
	}
	s.Spec.TLS.Certificates = kmapi.SetMissingSecretNameForCertificate(s.Spec.TLS.Certificates, string(SinglestoreServerCert), s.CertificateName(SinglestoreServerCert))
	s.Spec.TLS.Certificates = kmapi.SetMissingSecretNameForCertificate(s.Spec.TLS.Certificates, string(SinglestoreClientCert), s.CertificateName(SinglestoreClientCert))
	s.Spec.TLS.Certificates = kmapi.SetMissingSecretNameForCertificate(s.Spec.TLS.Certificates, string(SinglestoreMetricsExporterCert), s.CertificateName(SinglestoreMetricsExporterCert))
}

// CertificateName returns the default certificate name and/or certificate secret name for a certificate alias
func (s *Singlestore) CertificateName(alias SinglestoreCertificateAlias) string {
	return metautil.NameWithSuffix(s.Name, fmt.Sprintf("%s-cert", string(alias)))
}

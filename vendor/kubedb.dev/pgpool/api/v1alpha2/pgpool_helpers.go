package v1alpha2

import (
	"fmt"
	"gomodules.xyz/pointer"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	meta_util "kmodules.xyz/client-go/meta"
	"kubedb.dev/apimachinery/apis/kubedb"
	apis "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
)

func (p *Pgpool) ResourceFQN() string {
	return fmt.Sprintf("%s.%s", p.ResourcePlural(), kubedb.GroupName)
}

func (p *Pgpool) ResourceShortCode() string {
	return ResourceCodePgpool
}

func (p *Pgpool) ResourceKind() string {
	return ResourceKindPgpool
}

func (p *Pgpool) ResourceSingular() string {
	return ResourceSingularPgpool
}

func (p *Pgpool) ResourcePlural() string {
	return ResourcePluralPgpool
}

func (p *Pgpool) ConfigSecretName() string {
	return meta_util.NameWithSuffix(p.OffshootName(), "config")
}

func (p *Pgpool) ServiceAccountName() string {
	return p.OffshootName()
}

func (p *Pgpool) GoverningServiceName() string {
	return meta_util.NameWithSuffix(p.ServiceName(), "pods")
}

func (p *Pgpool) ServiceName() string {
	return p.OffshootName()
}

// Owner returns owner reference to resources
func (p *Pgpool) Owner() *meta.OwnerReference {
	return meta.NewControllerRef(p, apis.SchemeGroupVersion.WithKind(p.ResourceKind()))
}

func (p *Pgpool) PodLabels(extraLabels ...map[string]string) map[string]string {
	var labels map[string]string
	if p.Spec.PodTemplate != nil {
		return p.offshootLabels(meta_util.OverwriteKeys(p.OffshootSelectors(), extraLabels...), p.Spec.PodTemplate.Labels)
	} else {
		return p.offshootLabels(meta_util.OverwriteKeys(p.OffshootSelectors(), extraLabels...), labels)
	}
}

func (p *Pgpool) PodControllerLabels(extraLabels ...map[string]string) map[string]string {
	var labels map[string]string
	if p.Spec.PodTemplate != nil {
		return p.offshootLabels(meta_util.OverwriteKeys(p.OffshootSelectors(), extraLabels...), p.Spec.PodTemplate.Controller.Labels)
	} else {
		return p.offshootLabels(meta_util.OverwriteKeys(p.OffshootSelectors(), extraLabels...), labels)
	}
}

func (p *Pgpool) OffshootLabels() map[string]string {
	return p.offshootLabels(p.OffshootSelectors(), nil)
}

func (p *Pgpool) offshootLabels(selector, override map[string]string) map[string]string {
	selector[meta_util.ComponentLabelKey] = apis.ComponentConnectionPooler
	return meta_util.FilterKeys(kubedb.GroupName, selector, meta_util.OverwriteKeys(nil, p.Labels, override))
}

func (p *Pgpool) OffshootSelectors(extraSelectors ...map[string]string) map[string]string {
	selector := map[string]string{
		meta_util.NameLabelKey:      p.ResourceFQN(),
		meta_util.InstanceLabelKey:  p.Name,
		meta_util.ManagedByLabelKey: kubedb.GroupName,
	}
	return meta_util.OverwriteKeys(selector, extraSelectors...)
}

func (p *Pgpool) StatefulSetName() string {
	return p.OffshootName()
}

func (p *Pgpool) OffshootName() string {
	return p.Name
}

func (p *Pgpool) GetAuthSecretName() string {
	if p.Spec.AuthSecret != nil && p.Spec.AuthSecret.Name != "" {
		return p.Spec.AuthSecret.Name
	}
	return meta_util.NameWithSuffix(p.OffshootName(), "auth")
}

func (p *Pgpool) SetHealthCheckerDefaults() {
	if p.Spec.HealthChecker.PeriodSeconds == nil {
		p.Spec.HealthChecker.PeriodSeconds = pointer.Int32P(10)
	}
	if p.Spec.HealthChecker.TimeoutSeconds == nil {
		p.Spec.HealthChecker.TimeoutSeconds = pointer.Int32P(10)
	}
	if p.Spec.HealthChecker.FailureThreshold == nil {
		p.Spec.HealthChecker.FailureThreshold = pointer.Int32P(1)
	}
}

// PrimaryServiceDNS make primary host dns with require template
func (p *Pgpool) PrimaryServiceDNS() string {
	return fmt.Sprintf("%v.%v.svc", p.ServiceName(), p.Namespace)
}

func (p *Pgpool) GetNameSpacedName() string {
	return p.Namespace + "/" + p.Name
}

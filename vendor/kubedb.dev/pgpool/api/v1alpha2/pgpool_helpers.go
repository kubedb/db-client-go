package v1alpha2

import (
	"fmt"
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
	return p.offshootLabels(meta_util.OverwriteKeys(p.OffshootSelectors(), extraLabels...), p.Spec.PodTemplate.Labels)
}

func (p *Pgpool) PodControllerLabels(extraLabels ...map[string]string) map[string]string {
	return p.offshootLabels(meta_util.OverwriteKeys(p.OffshootSelectors(), extraLabels...), p.Spec.PodTemplate.Controller.Labels)
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

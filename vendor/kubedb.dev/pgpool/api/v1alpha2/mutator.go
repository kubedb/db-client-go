package v1alpha2

import (
	"gomodules.xyz/pointer"
	apm "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
)

func (p *Pgpool) MutatePgpool() {
	if p == nil {
		return
	}
	if p.Spec.TerminationPolicy == "" {
		p.Spec.TerminationPolicy = apm.TerminationPolicyDelete
	}
	// TODO ssl mode is not here for now

	if p.Spec.Replicas == nil {
		p.Spec.Replicas = pointer.Int32P(1)
	}
	//if p.Spec.PodTemplate != nil && p.Spec.PodTemplate.Spec.ServiceAccountName == "" {
	//	p.Spec.PodTemplate.Spec.ServiceAccountName = p.OffshootName()
	//}
	p.SetHealthCheckerDefaults()
}

package v1alpha2

import (
	"github.com/pkg/errors"
	ofst "kmodules.xyz/offshoot-api/api/v2"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
)

func (s *Singlestore) MutateSinglestore() error {
	if s.Spec.Version == "" {
		return errors.New(`'spec.version' is missing`)
	}

	if s.Spec.Halted {
		if s.Spec.TerminationPolicy == api.TerminationPolicyDoNotTerminate {
			return errors.New(`Can't halt, since termination policy is 'DoNotTerminate'`)
		}
		s.Spec.TerminationPolicy = api.TerminationPolicyHalt
	}

	if s.Spec.Topology == nil {
		if s.Spec.PodTemplate == nil {
			s.Spec.PodTemplate = &ofst.PodTemplateSpec{}
		}
	} else {
		if s.Spec.Topology.Aggregator.PodTemplate == nil {
			s.Spec.Topology.Aggregator.PodTemplate = &ofst.PodTemplateSpec{}
		}
		if s.Spec.Topology.Leaf.PodTemplate == nil {
			s.Spec.Topology.Leaf.PodTemplate = &ofst.PodTemplateSpec{}
		}
	}

	s.SetDefaults()

	return nil

}

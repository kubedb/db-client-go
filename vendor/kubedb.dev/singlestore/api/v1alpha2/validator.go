package v1alpha2

import (
	"fmt"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	ofst "kmodules.xyz/offshoot-api/api/v1"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	amv "kubedb.dev/apimachinery/pkg/validator"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var forbiddenEnvVars = []string{
	"ROOT_PASSWORD",
	"ROOT_USERNAME",
	"LICENSE_KEY",
	"SINGLESTORE_LICENSE",
}

// reserved volume and volumes mounts for mysql
var reservedVolumes = []string{
	SinglestoreVolumeNameUserInitScript,
	SinglestoreVolumeNameCustomConfig,
	SinglestoreVolmeNameInitScript,
	SinglestoreVolumeNameData,
}

var reservedVolumeMounts = []string{
	SinglestoreVolumeMountPathData,
	SinglestoreVolumeMountPathInitScript,
	SinglestoreVolumeMountPathCustomConfig,
	SinglestoreVolumeMountPathUserInitScript,
}

func (s *Singlestore) ValidateSinglestore(client kubernetes.Interface, KBClient client.Client) error {

	if s.Spec.Version == "" {
		return errors.New(`'spec.version' is missing`)
	}

	if s.Spec.StorageType == "" {
		return fmt.Errorf(`'spec.storageType' is missing`)
	}
	if s.Spec.StorageType != api.StorageTypeDurable && s.Spec.StorageType != api.StorageTypeEphemeral {
		return fmt.Errorf(`'spec.storageType' %s is invalid`, s.Spec.StorageType)
	}

	if s.Spec.Topology == nil {
		if *s.Spec.Replicas != 1 {
			return fmt.Errorf(`spec.replicas "%v" invalid. Must be one for standalone`, *s.Spec.Replicas)
		}
		if err := amv.ValidateStorage(client, s.Spec.StorageType, s.Spec.Storage); err != nil {
			return err
		}
		if s.Spec.PodTemplate != nil {
			if err := amv.ValidateEnvVar(getMainContainerEnvs(s), forbiddenEnvVars, s.ResourceKind()); err != nil {
				return err
			}
			if err := amv.ValidateVolumes(ofst.ConvertVolumes(s.Spec.PodTemplate.Spec.Volumes), reservedVolumes); err != nil {
				return err
			}

			/*if err := amv.ValidateMountPaths(s.Spec.PodTemplate.Spec.VolumeMounts, reservedVolumeMounts); err != nil {
				return err
			}*/
		}

	} else {
		if s.Spec.Topology.Aggregator.Replicas == nil || *s.Spec.Topology.Aggregator.Replicas < 1 {
			return fmt.Errorf(`Spec.Topology.Aggregator.Replicas "%v" invalid. Must be greater than zero`, *s.Spec.Topology.Aggregator.Replicas)
		}
		if err := amv.ValidateStorage(client, s.Spec.StorageType, s.Spec.Topology.Aggregator.Storage); err != nil {
			return err
		}
		if s.Spec.Topology.Aggregator.PodTemplate != nil {
			if err := amv.ValidateEnvVar(getAggMainContainerEnvs(s), forbiddenEnvVars, s.ResourceKind()); err != nil {
				return err
			}
			if err := amv.ValidateVolumes(ofst.ConvertVolumes(s.Spec.Topology.Aggregator.PodTemplate.Spec.Volumes), reservedVolumes); err != nil {
				return err
			}

			/*if err := amv.ValidateMountPaths(s.Spec.Topology.Aggregator.PodTemplate.Spec.VolumeMounts, reservedVolumeMounts); err != nil {
				return err
			}*/
		}

		if s.Spec.Topology.Leaf.Replicas == nil || *s.Spec.Topology.Leaf.Replicas < 1 {
			return fmt.Errorf(`Spec.Topology.Leaf.Replicas "%v" invalid. Must be greater than zero`, *s.Spec.Topology.Leaf.Replicas)
		}
		if err := amv.ValidateStorage(client, s.Spec.StorageType, s.Spec.Topology.Leaf.Storage); err != nil {
			return err
		}
		if s.Spec.Topology.Leaf.PodTemplate != nil {
			if err := amv.ValidateEnvVar(getLeafMainContainerEnvs(s), forbiddenEnvVars, s.ResourceKind()); err != nil {
				return err
			}
			if err := amv.ValidateVolumes(ofst.ConvertVolumes(s.Spec.Topology.Leaf.PodTemplate.Spec.Volumes), reservedVolumes); err != nil {
				return err
			}

			/*if err := amv.ValidateMountPaths(s.Spec.Topology.Leaf.PodTemplate.Spec.VolumeMounts, reservedVolumeMounts); err != nil {
				return err
			}*/
		}
	}

	// if secret managed externally verify auth secret name is not empty
	if s.Spec.AuthSecret != nil && s.Spec.AuthSecret.ExternallyManaged && s.Spec.AuthSecret.Name == "" {
		return fmt.Errorf("for externallyManaged auth secret, user need to provide \"Spec.AuthSecret.Name\"")
	}

	if s.Spec.TerminationPolicy == "" {
		return fmt.Errorf(`'spec.terminationPolicy' is missing`)
	}

	if s.Spec.StorageType == api.StorageTypeEphemeral && s.Spec.TerminationPolicy == api.TerminationPolicyHalt {
		return fmt.Errorf(`'spec.terminationPolicy: Halt' can not be used for 'Ephemeral' storage`)
	}

	if err := amv.ValidateHealth(&s.Spec.HealthChecker); err != nil {
		return err
	}

	return nil
}

func getMainContainerEnvs(s *Singlestore) []core.EnvVar {
	for _, container := range s.Spec.PodTemplate.Spec.Containers {
		if container.Name == SinglestoreContainerName {
			return container.Env
		}
	}
	return []core.EnvVar{}
}

func getAggMainContainerEnvs(s *Singlestore) []core.EnvVar {
	for _, container := range s.Spec.Topology.Aggregator.PodTemplate.Spec.Containers {
		if container.Name == SinglestoreContainerName {
			return container.Env
		}
	}
	return []core.EnvVar{}
}

func getLeafMainContainerEnvs(s *Singlestore) []core.EnvVar {
	for _, container := range s.Spec.Topology.Leaf.PodTemplate.Spec.Containers {
		if container.Name == SinglestoreContainerName {
			return container.Env
		}
	}
	return []core.EnvVar{}
}

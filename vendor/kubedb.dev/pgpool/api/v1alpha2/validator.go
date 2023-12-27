package v1alpha2

import (
	"context"
	"errors"
	"fmt"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	amv "kubedb.dev/apimachinery/pkg/validator"
	catalog "kubedb.dev/pgpool/api/catalog/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var forbiddenEnvVars = []string{
	EnvPostgresUsername, EnvPostgresPassword, EnvPgpoolPcpUser, EnvPgpoolPcpPassword,
	EnvPgpoolPasswordEncryptionMethod, EnvEnablePoolPasswd, EnvSkipPasswdEncryption,
}

func (p *Pgpool) ValidatePgpool(KBClient client.Client) error {
	if p.Spec.Version == "" {
		return errors.New(`'spec.version' is missing`)
	}
	version := &catalog.PgpoolVersion{}
	if err := KBClient.Get(context.TODO(), types.NamespacedName{
		Name: p.Spec.Version,
	}, version); err != nil {
		return err
	}

	if p.Spec.Replicas == nil || *p.Spec.Replicas < 1 {
		if p.Spec.Replicas == nil {
			return errors.New(`'spec.replicas' is missing`)
		}
		return fmt.Errorf(`spec.replicas "%v" invalid. Must be greater than zero`, *p.Spec.Replicas)
	}

	if p.Spec.PodTemplate != nil {
		if err := amv.ValidateEnvVar(getMainContainerEnvs(p), forbiddenEnvVars, p.ResourceKind()); err != nil {
			return err
		}
	}

	if p.Spec.AuthSecret != nil && p.Spec.AuthSecret.ExternallyManaged && p.Spec.AuthSecret.Name == "" {
		return fmt.Errorf(`for externallyManaged auth secret, user must configure "spec.authSecret.name"`)
	}

	if p.Spec.TerminationPolicy == "" {
		return fmt.Errorf(`'spec.terminationPolicy' is missing`)
	}

	if err := amv.ValidateHealth(p.Spec.HealthChecker); err != nil {
		return err
	}

	return nil
}

func getMainContainerEnvs(p *Pgpool) []core.EnvVar {
	for _, container := range p.Spec.PodTemplate.Spec.Containers {
		if container.Name == ContainerName {
			return container.Env
		}
	}
	return []core.EnvVar{}
}

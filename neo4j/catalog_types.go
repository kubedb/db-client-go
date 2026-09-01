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

package neo4j

// DatabaseCatalog is the set of physical databases, composites, and standalone aliases visible to Neo4j.
type DatabaseCatalog struct {
	Physical   map[string]struct{}
	Composites []CompositeDatabase
	Aliases    []DatabaseAlias
}

// CompositeDatabase describes a Neo4j composite database.
type CompositeDatabase struct {
	Name            string          `json:"name"`
	DefaultLanguage string          `json:"defaultLanguage,omitempty"`
	Aliases         []DatabaseAlias `json:"aliases,omitempty"`
}

// DatabaseAlias describes a local or remote Neo4j database alias.
type DatabaseAlias struct {
	Name           string           `json:"name"`
	Database       string           `json:"database"`
	Location       string           `json:"location"`
	URL            string           `json:"url,omitempty"`
	CredentialType string           `json:"credentialType,omitempty"`
	User           string           `json:"user,omitempty"`
	Driver         map[string]Value `json:"driver,omitempty"`
	Properties     map[string]Value `json:"properties,omitempty"`
}

// Value is a recursively typed Neo4j value suitable for serialization.
type Value struct {
	Type    string           `json:"type"`
	String  string           `json:"string,omitempty"`
	Boolean *bool            `json:"boolean,omitempty"`
	Integer *int64           `json:"integer,omitempty"`
	Float   *float64         `json:"float,omitempty"`
	List    []Value          `json:"list,omitempty"`
	Map     map[string]Value `json:"map,omitempty"`
}

type AliasCredential struct {
	Password string `json:"password"`
}

type CatalogRestoreOptions struct {
	Composites  []CompositeDatabase
	Aliases     []DatabaseAlias
	Credentials map[string]AliasCredential
	Overwrite   bool
}

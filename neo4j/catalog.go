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

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"k8s.io/klog/v2"
)

const systemDatabase = "system"

// GetDatabaseCatalog returns the physical databases, composite databases, and aliases visible in the system catalog.
func (c *Client) GetDatabaseCatalog(ctx context.Context) (DatabaseCatalog, error) {
	session := c.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead, DatabaseName: systemDatabase})
	defer closeCatalogSession(ctx, session, "catalog discovery")

	result, err := session.Run(ctx, "SHOW DATABASES YIELD name, type, defaultLanguage RETURN DISTINCT name, type, defaultLanguage", nil)
	if err != nil {
		return DatabaseCatalog{}, fmt.Errorf("failed to discover Neo4j databases: %w", err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return DatabaseCatalog{}, fmt.Errorf("failed to collect Neo4j databases: %w", err)
	}

	catalog := DatabaseCatalog{Physical: map[string]struct{}{}}
	compositeByName := map[string]int{}
	for _, record := range records {
		name, nameOK := recordString(record, "name")
		databaseType, typeOK := recordString(record, "type")
		if !nameOK || !typeOK {
			return DatabaseCatalog{}, fmt.Errorf("SHOW DATABASES returned invalid name/type values")
		}
		switch strings.ToLower(databaseType) {
		case "composite":
			composite := CompositeDatabase{Name: name}
			composite.DefaultLanguage, _ = recordString(record, "defaultLanguage")
			catalog.Composites = append(catalog.Composites, composite)
			compositeByName[name] = len(catalog.Composites) - 1
		case systemDatabase:
		default:
			catalog.Physical[name] = struct{}{}
		}
	}

	result, err = session.Run(ctx, "SHOW ALIASES FOR DATABASE YIELD * RETURN name, composite, database, location, url, credentials, user, driver, properties", nil)
	if err != nil {
		return DatabaseCatalog{}, fmt.Errorf("failed to discover Neo4j aliases: %w", err)
	}
	records, err = result.Collect(ctx)
	if err != nil {
		return DatabaseCatalog{}, fmt.Errorf("failed to collect Neo4j aliases: %w", err)
	}
	for _, record := range records {
		alias, err := aliasFromRecord(record)
		if err != nil {
			return DatabaseCatalog{}, err
		}
		compositeName, compositeAlias := recordString(record, "composite")
		if !compositeAlias || compositeName == "" {
			catalog.Aliases = append(catalog.Aliases, alias)
			continue
		}
		index, found := compositeByName[compositeName]
		if !found {
			return DatabaseCatalog{}, fmt.Errorf("alias refers to unknown composite database %q", compositeName)
		}
		catalog.Composites[index].Aliases = append(catalog.Composites[index].Aliases, alias)
	}

	sort.Slice(catalog.Composites, func(i, j int) bool { return catalog.Composites[i].Name < catalog.Composites[j].Name })
	for i := range catalog.Composites {
		sort.Slice(catalog.Composites[i].Aliases, func(a, b int) bool {
			return catalog.Composites[i].Aliases[a].Name < catalog.Composites[i].Aliases[b].Name
		})
	}
	sort.Slice(catalog.Aliases, func(i, j int) bool { return catalog.Aliases[i].Name < catalog.Aliases[j].Name })
	return catalog, nil
}

// RestoreCatalog creates the selected composites and standalone aliases, then verifies that they are visible.
func (c *Client) RestoreCatalog(ctx context.Context, options CatalogRestoreOptions) error {
	if len(options.Composites) == 0 && len(options.Aliases) == 0 {
		return nil
	}
	session := c.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite, DatabaseName: systemDatabase})
	defer closeCatalogSession(ctx, session, "catalog restore")

	for _, composite := range options.Composites {
		if err := waitForLocalTargets(ctx, session, composite.Aliases); err != nil {
			return err
		}
		if options.Overwrite {
			if err := replaceComposite(ctx, session, composite); err != nil {
				return err
			}
		} else if err := createComposite(ctx, session, composite, false); err != nil {
			return err
		}
		if err := createAliases(ctx, session, composite.Aliases, options.Credentials, options.Overwrite); err != nil {
			return err
		}
	}
	if err := createAliases(ctx, session, options.Aliases, options.Credentials, options.Overwrite); err != nil {
		return err
	}
	for _, composite := range options.Composites {
		if err := waitForComposite(ctx, session, composite.Name); err != nil {
			return err
		}
		for _, alias := range composite.Aliases {
			if err := waitForAlias(ctx, session, alias); err != nil {
				return err
			}
		}
	}
	for _, alias := range options.Aliases {
		if err := waitForAlias(ctx, session, alias); err != nil {
			return err
		}
	}
	return nil
}

// DetachAliases drops aliases and recreates any already dropped aliases if a later drop fails.
func (c *Client) DetachAliases(ctx context.Context, aliases []DatabaseAlias) error {
	if len(aliases) == 0 {
		return nil
	}
	session := c.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite, DatabaseName: systemDatabase})
	defer closeCatalogSession(ctx, session, "alias detach")

	detached := make([]DatabaseAlias, 0, len(aliases))
	for _, alias := range aliases {
		if err := dropAlias(ctx, session, alias.Name); err != nil {
			if recoveryErr := createAliases(ctx, session, detached, nil, true); recoveryErr != nil {
				return errors.Join(err, fmt.Errorf("failed to recover partially detached aliases: %w", recoveryErr))
			}
			return err
		}
		detached = append(detached, alias)
	}
	return nil
}

// RestoreAliases recreates aliases previously removed from the catalog.
func (c *Client) RestoreAliases(ctx context.Context, aliases []DatabaseAlias, credentials map[string]AliasCredential, overwrite bool) error {
	if len(aliases) == 0 {
		return nil
	}
	session := c.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite, DatabaseName: systemDatabase})
	defer closeCatalogSession(ctx, session, "alias restore")
	return createAliases(ctx, session, aliases, credentials, overwrite)
}

func aliasFromRecord(record *neo4jdriver.Record) (DatabaseAlias, error) {
	name, nameOK := recordString(record, "name")
	database, databaseOK := recordString(record, "database")
	location, locationOK := recordString(record, "location")
	if !nameOK || !databaseOK || !locationOK {
		return DatabaseAlias{}, fmt.Errorf("SHOW ALIASES returned invalid required fields")
	}
	alias := DatabaseAlias{Name: name, Database: database, Location: strings.ToLower(location)}
	alias.URL, _ = recordString(record, "url")
	alias.CredentialType, _ = recordString(record, "credentials")
	alias.User, _ = recordString(record, "user")
	for key, target := range map[string]*map[string]Value{"driver": &alias.Driver, "properties": &alias.Properties} {
		value, ok := record.Get(key)
		if !ok || value == nil {
			continue
		}
		values, ok := value.(map[string]any)
		if !ok {
			return DatabaseAlias{}, fmt.Errorf("alias %q has invalid %s value of type %T", name, key, value)
		}
		converted, err := valuesFromNative(values)
		if err != nil {
			return DatabaseAlias{}, fmt.Errorf("alias %q %s: %w", name, key, err)
		}
		*target = converted
	}
	return alias, nil
}

func recordString(record *neo4jdriver.Record, key string) (string, bool) {
	value, ok := record.Get(key)
	if !ok || value == nil {
		return "", false
	}
	result, ok := value.(string)
	return result, ok
}

func valuesFromNative(input map[string]any) (map[string]Value, error) {
	result := make(map[string]Value, len(input))
	for key, value := range input {
		converted, err := valueFromNative(value)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", key, err)
		}
		result[key] = converted
	}
	return result, nil
}

func valueFromNative(value any) (Value, error) {
	switch v := value.(type) {
	case nil:
		return Value{Type: "null"}, nil
	case string:
		return Value{Type: "string", String: v}, nil
	case bool:
		return Value{Type: "boolean", Boolean: &v}, nil
	case int64:
		return Value{Type: "integer", Integer: &v}, nil
	case float64:
		return Value{Type: "float", Float: &v}, nil
	case []any:
		result := Value{Type: "list", List: make([]Value, 0, len(v))}
		for _, item := range v {
			converted, err := valueFromNative(item)
			if err != nil {
				return Value{}, err
			}
			result.List = append(result.List, converted)
		}
		return result, nil
	case map[string]any:
		result, err := valuesFromNative(v)
		return Value{Type: "map", Map: result}, err
	case dbtype.Duration:
		return Value{Type: "duration", String: v.String()}, nil
	case dbtype.Date:
		return Value{Type: "date", String: v.String()}, nil
	case dbtype.Time:
		return Value{Type: "time", String: v.String()}, nil
	case dbtype.LocalTime:
		return Value{Type: "localTime", String: v.String()}, nil
	case dbtype.LocalDateTime:
		return Value{Type: "localDateTime", String: v.String()}, nil
	default:
		return Value{}, fmt.Errorf("unsupported Neo4j value type %T", value)
	}
}

func valuesToNative(input map[string]Value) (map[string]any, error) {
	result := make(map[string]any, len(input))
	for key, value := range input {
		converted, err := valueToNative(value)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", key, err)
		}
		result[key] = converted
	}
	return result, nil
}

func valueToNative(value Value) (any, error) {
	switch value.Type {
	case "null":
		return nil, nil
	case "string":
		return value.String, nil
	case "boolean":
		if value.Boolean == nil {
			return nil, fmt.Errorf("boolean value is empty")
		}
		return *value.Boolean, nil
	case "integer":
		if value.Integer == nil {
			return nil, fmt.Errorf("integer value is empty")
		}
		return *value.Integer, nil
	case "float":
		if value.Float == nil {
			return nil, fmt.Errorf("float value is empty")
		}
		return *value.Float, nil
	case "list":
		result := make([]any, 0, len(value.List))
		for _, item := range value.List {
			converted, err := valueToNative(item)
			if err != nil {
				return nil, err
			}
			result = append(result, converted)
		}
		return result, nil
	case "map":
		return valuesToNative(value.Map)
	case "duration":
		return parseDuration(value.String)
	case "date":
		parsed, err := time.Parse("2006-01-02", value.String)
		return dbtype.Date(parsed), err
	case "time":
		parsed, err := time.Parse("15:04:05.999999999Z07:00", value.String)
		return dbtype.Time(parsed), err
	case "localTime":
		parsed, err := time.ParseInLocation("15:04:05.999999999", value.String, time.Local)
		return dbtype.LocalTime(parsed), err
	case "localDateTime":
		parsed, err := time.ParseInLocation("2006-01-02T15:04:05.999999999", value.String, time.Local)
		return dbtype.LocalDateTime(parsed), err
	default:
		return nil, fmt.Errorf("unsupported Neo4j value type %q", value.Type)
	}
}

func replaceComposite(ctx context.Context, session neo4jdriver.SessionWithContext, composite CompositeDatabase) error {
	result, err := session.Run(ctx, "SHOW ALIASES FOR DATABASE YIELD name, composite WHERE composite = $composite RETURN DISTINCT name", map[string]any{"composite": composite.Name})
	if err != nil {
		return fmt.Errorf("failed to list current aliases for composite %q: %w", composite.Name, err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return fmt.Errorf("failed to collect current aliases for composite %q: %w", composite.Name, err)
	}
	for _, record := range records {
		name, ok := recordString(record, "name")
		if !ok {
			return fmt.Errorf("current composite %q contains an alias with an invalid name", composite.Name)
		}
		if err := dropAlias(ctx, session, name); err != nil {
			return err
		}
	}
	if err := consume(ctx, session, "DROP COMPOSITE DATABASE "+quoteIdentifier(composite.Name)+" IF EXISTS", nil); err != nil {
		return fmt.Errorf("failed to drop composite database %q: %w", composite.Name, err)
	}
	return createComposite(ctx, session, composite, false)
}

func createComposite(ctx context.Context, session neo4jdriver.SessionWithContext, composite CompositeDatabase, overwrite bool) error {
	verb := "CREATE COMPOSITE DATABASE "
	if overwrite {
		verb = "CREATE OR REPLACE COMPOSITE DATABASE "
	}
	query := verb + quoteIdentifier(composite.Name)
	if language, ok := normalizeLanguage(composite.DefaultLanguage); ok {
		query += " DEFAULT LANGUAGE CYPHER " + language
	} else if composite.DefaultLanguage != "" {
		return fmt.Errorf("composite %q has unsupported default language %q", composite.Name, composite.DefaultLanguage)
	}
	if err := consume(ctx, session, query, nil); err != nil {
		return fmt.Errorf("failed to create composite database %q: %w", composite.Name, err)
	}
	return nil
}

func createAliases(ctx context.Context, session neo4jdriver.SessionWithContext, aliases []DatabaseAlias, credentials map[string]AliasCredential, overwrite bool) error {
	for _, alias := range aliases {
		if err := createAlias(ctx, session, alias, credentials[alias.Name], overwrite); err != nil {
			return err
		}
	}
	return nil
}

func createAlias(ctx context.Context, session neo4jdriver.SessionWithContext, alias DatabaseAlias, credential AliasCredential, overwrite bool) error {
	verb := "CREATE ALIAS "
	if overwrite {
		verb = "CREATE OR REPLACE ALIAS "
	}
	query := verb + quoteIdentifier(alias.Name) + " FOR DATABASE " + quoteIdentifier(alias.Database)
	params := map[string]any{}
	if strings.EqualFold(alias.Location, "remote") {
		query += " AT $url"
		params["url"] = alias.URL
		if strings.Contains(strings.ToUpper(alias.CredentialType), "OIDC") {
			query += " OIDC CREDENTIAL FORWARDING"
		} else {
			query += " USER $user PASSWORD $password"
			params["user"] = alias.User
			params["password"] = credential.Password
		}
		driver, err := valuesToNative(alias.Driver)
		if err != nil {
			return fmt.Errorf("alias %q driver settings: %w", alias.Name, err)
		}
		if isUnsecuredURL(alias.URL) {
			driver["ssl_enforced"] = false
		}
		if len(driver) > 0 {
			query += " DRIVER $driver"
			params["driver"] = driver
		}
	}
	if len(alias.Properties) > 0 {
		properties, err := valuesToNative(alias.Properties)
		if err != nil {
			return fmt.Errorf("alias %q properties: %w", alias.Name, err)
		}
		query += " PROPERTIES $properties"
		params["properties"] = properties
	}
	if err := consume(ctx, session, query, params); err != nil {
		return fmt.Errorf("failed to create alias %q: %w", alias.Name, err)
	}
	return nil
}

func dropAlias(ctx context.Context, session neo4jdriver.SessionWithContext, name string) error {
	if err := consume(ctx, session, "DROP ALIAS "+quoteIdentifier(name)+" IF EXISTS FOR DATABASE", nil); err != nil {
		return fmt.Errorf("failed to drop alias %q: %w", name, err)
	}
	return nil
}

func waitForLocalTargets(ctx context.Context, session neo4jdriver.SessionWithContext, aliases []DatabaseAlias) error {
	for _, alias := range aliases {
		if !strings.EqualFold(alias.Location, "local") {
			continue
		}
		if err := waitForEntry(ctx, fmt.Sprintf("local target database %q for alias %q", alias.Database, alias.Name), func() (bool, error) {
			result, err := session.Run(ctx, "SHOW DATABASES YIELD name, type, currentStatus WHERE name = $name RETURN DISTINCT type, currentStatus", map[string]any{"name": alias.Database})
			if err != nil {
				return false, err
			}
			records, err := result.Collect(ctx)
			if err != nil {
				return false, err
			}
			online := false
			for _, record := range records {
				databaseType, _ := recordString(record, "type")
				status, _ := recordString(record, "currentStatus")
				if strings.EqualFold(databaseType, "composite") {
					return false, fmt.Errorf("database is composite, expected a physical database")
				}
				if databaseType == "" {
					return false, fmt.Errorf("SHOW DATABASES returned an invalid database type")
				}
				online = online || strings.EqualFold(status, "online")
			}
			return len(records) > 0 && online, nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func waitForComposite(ctx context.Context, session neo4jdriver.SessionWithContext, name string) error {
	return waitForEntry(ctx, fmt.Sprintf("composite database %q", name), func() (bool, error) {
		result, err := session.Run(ctx, "SHOW DATABASES YIELD name, type WHERE name = $name RETURN type", map[string]any{"name": name})
		if err != nil {
			return false, err
		}
		records, err := result.Collect(ctx)
		if err != nil {
			return false, err
		}
		for _, record := range records {
			if databaseType, _ := recordString(record, "type"); strings.EqualFold(databaseType, "composite") {
				return true, nil
			}
		}
		return false, nil
	})
}

func waitForAlias(ctx context.Context, session neo4jdriver.SessionWithContext, alias DatabaseAlias) error {
	return waitForEntry(ctx, fmt.Sprintf("alias %q with target %q", alias.Name, alias.Database), func() (bool, error) {
		result, err := session.Run(ctx, "SHOW ALIASES FOR DATABASE YIELD name, database WHERE name = $name AND database = $database RETURN name", map[string]any{"name": alias.Name, "database": alias.Database})
		if err != nil {
			return false, err
		}
		records, err := result.Collect(ctx)
		return len(records) > 0, err
	})
}

func waitForEntry(ctx context.Context, description string, check func() (bool, error)) error {
	for {
		ready, err := check()
		if err != nil {
			return fmt.Errorf("failed checking %s: %w", description, err)
		}
		if ready {
			return nil
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("timed out waiting for %s to become visible: %w", description, ctx.Err())
		case <-timer.C:
		}
	}
}

func consume(ctx context.Context, session neo4jdriver.SessionWithContext, query string, params map[string]any) error {
	result, err := session.Run(ctx, query, params)
	if err != nil {
		return err
	}
	_, err = result.Consume(ctx)
	return err
}

func quoteIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func normalizeLanguage(language string) (string, bool) {
	parts := strings.Fields(strings.ToUpper(language))
	if len(parts) == 1 && (parts[0] == "5" || parts[0] == "25") {
		return parts[0], true
	}
	if len(parts) == 2 && parts[0] == "CYPHER" && (parts[1] == "5" || parts[1] == "25") {
		return parts[1], true
	}
	return "", false
}

func isUnsecuredURL(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return strings.EqualFold(parsed.Scheme, "neo4j") || strings.EqualFold(parsed.Scheme, "bolt")
}

func parseDuration(value string) (dbtype.Duration, error) {
	match := regexp.MustCompile(`^P(-?\d+)M(-?\d+)DT(-?\d+)(?:\.(\d{1,9}))?S$`).FindStringSubmatch(value)
	if len(match) == 0 {
		return dbtype.Duration{}, fmt.Errorf("invalid duration %q", value)
	}
	months, _ := strconv.ParseInt(match[1], 10, 64)
	days, _ := strconv.ParseInt(match[2], 10, 64)
	seconds, _ := strconv.ParseInt(match[3], 10, 64)
	nanos := 0
	if match[4] != "" {
		nanos, _ = strconv.Atoi(match[4] + strings.Repeat("0", 9-len(match[4])))
	}
	return dbtype.Duration{Months: months, Days: days, Seconds: seconds, Nanos: nanos}, nil
}

func closeCatalogSession(ctx context.Context, session neo4jdriver.SessionWithContext, description string) {
	if err := session.Close(ctx); err != nil {
		klog.Warningf("failed to close Neo4j %s session: %v", description, err)
	}
}

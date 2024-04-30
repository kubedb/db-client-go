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

package mssql

import (
	"context"
	"fmt"
	"k8s.io/klog/v2"
	"strings"
	"xorm.io/xorm"
)

var (
	diskUsageSafetyFactor = 0.10 // Add 10%
	diskUsageDefaultMi    = 1024 // 1024 Mi
)

func (xc *XormClient) GetEstimateDatabaseDiskUsage(ctx context.Context, dbName string) (string, error) {
	session := xc.NewSession().Context(ctx)
	defer func(session *xorm.Session) {
		err := session.Close()
		if err != nil {
			klog.Error(err)
		}
	}(session)

	query := fmt.Sprintf(`USE %s; SELECT 
        (SELECT SUM(used_log_space_in_bytes) FROM sys.dm_db_log_space_usage) +
        (SELECT SUM(allocated_extent_page_count)*8*1024 FROM sys.dm_db_file_space_usage) AS total_size`, quoteName(dbName))

	var result struct {
		TotalSize int64 `xorm:"total_size"`
	}
	_, err := session.SQL(query).Get(&result)
	if err != nil {
		return "", fmt.Errorf("failed to execute query: %v", err)
	}

	totalDiskUsageInBytesWithExtra := result.TotalSize + int64(diskUsageSafetyFactor*float64(result.TotalSize))
	totalDiskUsageInMib := totalDiskUsageInBytesWithExtra / (1024 * 1024)
	if totalDiskUsageInMib < int64(diskUsageDefaultMi) {
		totalDiskUsageInMib = int64(diskUsageDefaultMi)
	}
	return fmt.Sprintf("%dMi", totalDiskUsageInMib), nil
}

func quoteName(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

func (xc *XormClient) FetchNonSystemDatabases(ctx context.Context) ([]string, error) {
	// Create a session from the mssqlClient
	session := xc.NewSession().Context(ctx)
	defer func(session *xorm.Session) {
		err := session.Close()
		if err != nil {
			klog.Error(err)
		}
	}(session)

	query := "SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb','kubedb_system')"
	rows, err := session.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	var databases []string
	for _, row := range rows {
		databases = append(databases, string(row["name"]))
	}
	return databases, nil
}

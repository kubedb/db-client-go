package mssql

import (
	"context"
	"fmt"
	"k8s.io/klog/v2"
	"strings"
	"time"
	"xorm.io/xorm"
)

var (
	diskUsageSafetyFactor = 0.10 // Add 10%
	diskUsageDefaultMi    = 1024 // 1024 Mi
)

func (xc *XormClient) GetEstimateDatabaseDiskUsage(dbName string, ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Create a session from the mssqlClient
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

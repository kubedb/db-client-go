// Copyright 2025 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dialects

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"xorm.io/xorm/core"
	"xorm.io/xorm/schemas"
)

func init() {
	RegisterDriver("gbase8s", &gbase8sDriver{})
	RegisterDialect(schemas.GBASE8S, func() Dialect {
		return &gbase8s{}
	})
}

var (
	gbase8sReservedWords = map[string]bool{
		"ACCESS":                    true,
		"ACCOUNT":                   true,
		"ACTIVATE":                  true,
		"ADD":                       true,
		"ADMIN":                     true,
		"ADVISE":                    true,
		"AFTER":                     true,
		"ALL":                       true,
		"ALL_ROWS":                  true,
		"ALLOCATE":                  true,
		"ALTER":                     true,
		"ANALYZE":                   true,
		"AND":                       true,
		"ANY":                       true,
		"ARCHIVE":                   true,
		"ARCHIVELOG":                true,
		"ARRAY":                     true,
		"AS":                        true,
		"ASC":                       true,
		"AT":                        true,
		"AUDIT":                     true,
		"AUTHENTICATED":             true,
		"AUTHORIZATION":             true,
		"AUTOEXTEND":                true,
		"AUTOMATIC":                 true,
		"BACKUP":                    true,
		"BECOME":                    true,
		"BEFORE":                    true,
		"BEGIN":                     true,
		"BETWEEN":                   true,
		"BFILE":                     true,
		"BITMAP":                    true,
		"BLOB":                      true,
		"BLOCK":                     true,
		"BODY":                      true,
		"BY":                        true,
		"CACHE":                     true,
		"CACHE_INSTANCES":           true,
		"CANCEL":                    true,
		"CASCADE":                   true,
		"CAST":                      true,
		"CFILE":                     true,
		"CHAINED":                   true,
		"CHANGE":                    true,
		"CHAR":                      true,
		"CHAR_CS":                   true,
		"CHARACTER":                 true,
		"CHECK":                     true,
		"CHECKPOINT":                true,
		"CHOOSE":                    true,
		"CHUNK":                     true,
		"CLEAR":                     true,
		"CLOB":                      true,
		"CLONE":                     true,
		"CLOSE":                     true,
		"CLOSE_CACHED_OPEN_CURSORS": true,
		"CLUSTER":                   true,
		"COALESCE":                  true,
		"COLUMN":                    true,
		"COLUMNS":                   true,
		"COMMENT":                   true,
		"COMMIT":                    true,
		"COMMITTED":                 true,
		"COMPATIBILITY":             true,
		"COMPILE":                   true,
		"COMPLETE":                  true,
		"COMPOSITE_LIMIT":           true,
		"COMPRESS":                  true,
		"COMPUTE":                   true,
		"CONNECT":                   true,
		"CONNECT_TIME":              true,
		"CONSTRAINT":                true,
		"CONSTRAINTS":               true,
		"CONTENTS":                  true,
		"CONTINUE":                  true,
		"CONTROLFILE":               true,
		"CONVERT":                   true,
		"COST":                      true,
		"CPU_PER_CALL":              true,
		"CPU_PER_SESSION":           true,
		"CREATE":                    true,
		"CURRENT":                   true,
		"CURRENT_SCHEMA":            true,
		"CURREN_USER":               true,
		"CURSOR":                    true,
		"CYCLE":                     true,
		"DANGLING":                  true,
		"DATABASE":                  true,
		"DATAFILE":                  true,
		"DATAFILES":                 true,
		"DATAOBJNO":                 true,
		"DATE":                      true,
		"DBA":                       true,
		"DBHIGH":                    true,
		"DBLOW":                     true,
		"DBMAC":                     true,
		"DEALLOCATE":                true,
		"DEBUG":                     true,
		"DEC":                       true,
		"DECIMAL":                   true,
		"DECLARE":                   true,
		"DEFAULT":                   true,
		"DEFERRABLE":                true,
		"DEFERRED":                  true,
		"DEGREE":                    true,
		"DELETE":                    true,
		"DEREF":                     true,
		"DESC":                      true,
		"DIRECTORY":                 true,
		"DISABLE":                   true,
		"DISCONNECT":                true,
		"DISMOUNT":                  true,
		"DISTINCT":                  true,
		"DISTRIBUTED":               true,
		"DML":                       true,
		"DOUBLE":                    true,
		"DROP":                      true,
		"DUMP":                      true,
		"EACH":                      true,
		"ELSE":                      true,
		"ENABLE":                    true,
		"END":                       true,
		"ENFORCE":                   true,
		"ENTRY":                     true,
		"ESCAPE":                    true,
		"EXCEPT":                    true,
		"EXCEPTIONS":                true,
		"EXCHANGE":                  true,
		"EXCLUDING":                 true,
		"EXCLUSIVE":                 true,
		"EXECUTE":                   true,
		"EXISTS":                    true,
		"EXPIRE":                    true,
		"EXPLAIN":                   true,
		"EXTENT":                    true,
		"EXTENTS":                   true,
		"EXTERNALLY":                true,
		"FAILED_LOGIN_ATTEMPTS":     true,
		"FALSE":                     true,
		"FAST":                      true,
		"FILE":                      true,
		"FIRST_ROWS":                true,
		"FLAGGER":                   true,
		"FLOAT":                     true,
		"FLOB":                      true,
		"FLUSH":                     true,
		"FOR":                       true,
		"FORCE":                     true,
		"FOREIGN":                   true,
		"FREELIST":                  true,
		"FREELISTS":                 true,
		"FROM":                      true,
		"FULL":                      true,
		"FUNCTION":                  true,
		"GLOBAL":                    true,
		"GLOBALLY":                  true,
		"GLOBAL_NAME":               true,
		"GRANT":                     true,
		"GROUP":                     true,
		"GROUPS":                    true,
		"HASH":                      true,
		"HASHKEYS":                  true,
		"HAVING":                    true,
		"HEADER":                    true,
		"HEAP":                      true,
		"IDENTIFIED":                true,
		"IDGENERATORS":              true,
		"IDLE_TIME":                 true,
		"IF":                        true,
		"IMMEDIATE":                 true,
		"IN":                        true,
		"INCLUDING":                 true,
		"INCREMENT":                 true,
		"INDEX":                     true,
		"INDEXED":                   true,
		"INDEXES":                   true,
		"INDICATOR":                 true,
		"IND_PARTITION":             true,
		"INITIAL":                   true,
		"INITIALLY":                 true,
		"INITRANS":                  true,
		"INSERT":                    true,
		"INSTANCE":                  true,
		"INSTANCES":                 true,
		"INSTEAD":                   true,
		"INT":                       true,
		"INTEGER":                   true,
		"INTERMEDIATE":              true,
		"INTERSECT":                 true,
		"INTO":                      true,
		"IS":                        true,
		"ISOLATION":                 true,
		"ISOLATION_LEVEL":           true,
		"KEEP":                      true,
		"KEY":                       true,
		"KILL":                      true,
		"LABEL":                     true,
		"LAYER":                     true,
		"LESS":                      true,
		"LEVEL":                     true,
		"LIBRARY":                   true,
		"LIKE":                      true,
		"LIMIT":                     true,
		"LINK":                      true,
		"LIST":                      true,
		"LOB":                       true,
		"LOCAL":                     true,
		"LOCK":                      true,
		"LOCKED":                    true,
		"LOG":                       true,
		"LOGFILE":                   true,
		"LOGGING":                   true,
		"LOGICAL_READS_PER_CALL":    true,
		"LOGICAL_READS_PER_SESSION": true,
		"LONG":                      true,
		"MANAGE":                    true,
		"MASTER":                    true,
		"MAX":                       true,
		"MAXARCHLOGS":               true,
		"MAXDATAFILES":              true,
		"MAXEXTENTS":                true,
		"MAXINSTANCES":              true,
		"MAXLOGFILES":               true,
		"MAXLOGHISTORY":             true,
		"MAXLOGMEMBERS":             true,
		"MAXSIZE":                   true,
		"MAXTRANS":                  true,
		"MAXVALUE":                  true,
		"MIN":                       true,
		"MEMBER":                    true,
		"MINIMUM":                   true,
		"MINEXTENTS":                true,
		"MINUS":                     true,
		"MINVALUE":                  true,
		"MLSLABEL":                  true,
		"MLS_LABEL_FORMAT":          true,
		"MODE":                      true,
		"MODIFY":                    true,
		"MOUNT":                     true,
		"MOVE":                      true,
		"MTS_DISPATCHERS":           true,
		"MULTISET":                  true,
		"NATIONAL":                  true,
		"NCHAR":                     true,
		"NCHAR_CS":                  true,
		"NCLOB":                     true,
		"NEEDED":                    true,
		"NESTED":                    true,
		"NETWORK":                   true,
		"NEW":                       true,
		"NEXT":                      true,
		"NOARCHIVELOG":              true,
		"NOAUDIT":                   true,
		"NOCACHE":                   true,
		"NOCOMPRESS":                true,
		"NOCYCLE":                   true,
		"NOFORCE":                   true,
		"NOLOGGING":                 true,
		"NOMAXVALUE":                true,
		"NOMINVALUE":                true,
		"NONE":                      true,
		"NOORDER":                   true,
		"NOOVERRIDE":                true,
		"NOPARALLEL":                true,
		"NOREVERSE":                 true,
		"NORMAL":                    true,
		"NOSORT":                    true,
		"NOT":                       true,
		"NOTHING":                   true,
		"NOWAIT":                    true,
		"NULL":                      true,
		"NUMBER":                    true,
		"NUMERIC":                   true,
		"NVARCHAR2":                 true,
		"OBJECT":                    true,
		"OBJNO":                     true,
		"OBJNO_REUSE":               true,
		"OF":                        true,
		"OFF":                       true,
		"OFFLINE":                   true,
		"OID":                       true,
		"OIDINDEX":                  true,
		"OLD":                       true,
		"ON":                        true,
		"ONLINE":                    true,
		"ONLY":                      true,
		"OPCODE":                    true,
		"OPEN":                      true,
		"OPTIMAL":                   true,
		"OPTIMIZER_GOAL":            true,
		"OPTION":                    true,
		"OR":                        true,
		"ORDER":                     true,
		"ORGANIZATION":              true,
		"OSLABEL":                   true,
		"OVERFLOW":                  true,
		"OWN":                       true,
		"PACKAGE":                   true,
		"PARALLEL":                  true,
		"PARTITION":                 true,
		"PASSWORD":                  true,
		"PASSWORD_GRACE_TIME":       true,
		"PASSWORD_LIFE_TIME":        true,
		"PASSWORD_LOCK_TIME":        true,
		"PASSWORD_REUSE_MAX":        true,
		"PASSWORD_REUSE_TIME":       true,
		"PASSWORD_VERIFY_FUNCTION":  true,
		"PCTFREE":                   true,
		"PCTINCREASE":               true,
		"PCTTHRESHOLD":              true,
		"PCTUSED":                   true,
		"PCTVERSION":                true,
		"PERCENT":                   true,
		"PERMANENT":                 true,
		"PLAN":                      true,
		"PLSQL_DEBUG":               true,
		"POST_TRANSACTION":          true,
		"PRECISION":                 true,
		"PRESERVE":                  true,
		"PRIMARY":                   true,
		"PRIOR":                     true,
		"PRIVATE":                   true,
		"PRIVATE_SGA":               true,
		"PRIVILEGE":                 true,
		"PRIVILEGES":                true,
		"PROCEDURE":                 true,
		"PROFILE":                   true,
		"PUBLIC":                    true,
		"PURGE":                     true,
		"QUEUE":                     true,
		"QUOTA":                     true,
		"RANGE":                     true,
		"RAW":                       true,
		"RBA":                       true,
		"READ":                      true,
		"READUP":                    true,
		"REAL":                      true,
		"REBUILD":                   true,
		"RECOVER":                   true,
		"RECOVERABLE":               true,
		"RECOVERY":                  true,
		"REF":                       true,
		"REFERENCES":                true,
		"REFERENCING":               true,
		"REFRESH":                   true,
		"RENAME":                    true,
		"REPLACE":                   true,
		"RESET":                     true,
		"RESETLOGS":                 true,
		"RESIZE":                    true,
		"RESOURCE":                  true,
		"RESTRICTED":                true,
		"RETURN":                    true,
		"RETURNING":                 true,
		"REUSE":                     true,
		"REVERSE":                   true,
		"REVOKE":                    true,
		"ROLE":                      true,
		"ROLES":                     true,
		"ROLLBACK":                  true,
		"ROW":                       true,
		"ROWID":                     true,
		"ROWNUM":                    true,
		"ROWS":                      true,
		"RULE":                      true,
		"SAMPLE":                    true,
		"SAVEPOINT":                 true,
		"SB4":                       true,
		"SCAN_INSTANCES":            true,
		"SCHEMA":                    true,
		"SCN":                       true,
		"SCOPE":                     true,
		"SD_ALL":                    true,
		"SD_INHIBIT":                true,
		"SD_SHOW":                   true,
		"SEGMENT":                   true,
		"SEG_BLOCK":                 true,
		"SEG_FILE":                  true,
		"SELECT":                    true,
		"SEQUENCE":                  true,
		"SERIALIZABLE":              true,
		"SESSION":                   true,
		"SESSION_CACHED_CURSORS":    true,
		"SESSIONS_PER_USER":         true,
		"SET":                       true,
		"SHARE":                     true,
		"SHARED":                    true,
		"SHARED_POOL":               true,
		"SHRINK":                    true,
		"SIZE":                      true,
		"SKIP":                      true,
		"SKIP_UNUSABLE_INDEXES":     true,
		"SMALLINT":                  true,
		"SNAPSHOT":                  true,
		"SOME":                      true,
		"SORT":                      true,
		"SPECIFICATION":             true,
		"SPLIT":                     true,
		"SQL_TRACE":                 true,
		"STANDBY":                   true,
		"START":                     true,
		"STATEMENT_ID":              true,
		"STATISTICS":                true,
		"STOP":                      true,
		"STORAGE":                   true,
		"STORE":                     true,
		"STRUCTURE":                 true,
		"SUCCESSFUL":                true,
		"SWITCH":                    true,
		"SYS_OP_ENFORCE_NOT_NULL$":  true,
		"SYS_OP_NTCIMG$":            true,
		"SYNONYM":                   true,
		"SYSDATE":                   true,
		"SYSDBA":                    true,
		"SYSOPER":                   true,
		"SYSTEM":                    true,
		"TABLE":                     true,
		"TABLES":                    true,
		"TABLESPACE":                true,
		"TABLESPACE_NO":             true,
		"TABNO":                     true,
		"TEMPORARY":                 true,
		"THAN":                      true,
		"THE":                       true,
		"THEN":                      true,
		"THREAD":                    true,
		"TIMESTAMP":                 true,
		"TIME":                      true,
		"TO":                        true,
		"TOPLEVEL":                  true,
		"TRACE":                     true,
		"TRACING":                   true,
		"TRANSACTION":               true,
		"TRANSITIONAL":              true,
		"TRIGGER":                   true,
		"TRIGGERS":                  true,
		"TRUE":                      true,
		"TRUNCATE":                  true,
		"TX":                        true,
		"TYPE":                      true,
		"UB2":                       true,
		"UBA":                       true,
		"UID":                       true,
		"UNARCHIVED":                true,
		"UNDO":                      true,
		"UNION":                     true,
		"UNIQUE":                    true,
		"UNLIMITED":                 true,
		"UNLOCK":                    true,
		"UNRECOVERABLE":             true,
		"UNTIL":                     true,
		"UNUSABLE":                  true,
		"UNUSED":                    true,
		"UPDATABLE":                 true,
		"UPDATE":                    true,
		"USAGE":                     true,
		"USE":                       true,
		"USER":                      true,
		"USING":                     true,
		"VALIDATE":                  true,
		"VALIDATION":                true,
		"VALUE":                     true,
		"VALUES":                    true,
		"VARCHAR":                   true,
		"VARCHAR2":                  true,
		"VARYING":                   true,
		"VIEW":                      true,
		"WHEN":                      true,
		"WHENEVER":                  true,
		"WHERE":                     true,
		"WITH":                      true,
		"WITHOUT":                   true,
		"WORK":                      true,
		"WRITE":                     true,
		"WRITEDOWN":                 true,
		"WRITEUP":                   true,
		"XID":                       true,
		"YEAR":                      true,
		"ZONE":                      true,
	}

	gbase8sQuoter = schemas.Quoter{
		Prefix:     '"',
		Suffix:     '"',
		IsReserved: schemas.AlwaysReserve,
	}
)

type gbase8s struct {
	Base
}

func (db *gbase8s) Init(uri *URI) error {
	db.quoter = gbase8sQuoter
	return db.Base.Init(db, uri)
}

func (db *gbase8s) Version(ctx context.Context, queryer core.Queryer) (*schemas.Version, error) {
	rows, err := queryer.QueryContext(ctx, "SELECT dbinfo('version', 'full') FROM systables WHERE tabid = 1")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var version string
	if !rows.Next() {
		if rows.Err() != nil {
			return nil, rows.Err()
		}
		return nil, errors.New("unknown version")
	}
	if err := rows.Scan(&version); err != nil {
		return nil, err
	}
	// Parse GBase 8s version string Example: "GBase Server Version 12.10.FC4G1TL"
	fields := strings.Fields(version)
	if len(fields) >= 4 && fields[0] == "GBase" && fields[1] == "Server" {
		version = fields[3]
	}
	return &schemas.Version{
		Number:  version,
		Edition: "GBase 8s",
	}, nil
}

func (db *gbase8s) Features() *DialectFeatures {
	return &DialectFeatures{
		AutoincrMode: IncrAutoincrMode,
	}
}

func (db *gbase8s) SQLType(c *schemas.Column) string {
	var res string
	switch t := c.SQLType.Name; t {
	case schemas.Serial:
		c.IsAutoIncrement = true
		res = "SERIAL"
	case schemas.BigSerial:
		c.IsAutoIncrement = true
		res = "BIGSERIAL"
	case schemas.TinyInt, schemas.UnsignedTinyInt, schemas.SmallInt, schemas.Bit, schemas.UnsignedBit:
		res = "SMALLINT"
	case schemas.UnsignedSmallInt, schemas.MediumInt, schemas.UnsignedMediumInt, schemas.Int, schemas.Integer:
		if c.IsAutoIncrement {
			return "SERIAL"
		}
		return "INT"
	case schemas.UnsignedInt, schemas.BigInt, schemas.UnsignedBigInt:
		if c.IsAutoIncrement {
			return "BIGSERIAL"
		}
		return "BIGINT"
	case schemas.Real:
		res = " SMALLFLOAT" // "REAL"
	case schemas.UnsignedFloat, schemas.Double, schemas.Float:
		res = "FLOAT" // "DOUBLE PRECISION"
	case schemas.Decimal, schemas.Numeric, schemas.Number:
		res = "NUMERIC" // DECIMAL
	case schemas.Money, schemas.SmallMoney:
		res = "Money"
	case schemas.Bool, schemas.Boolean:
		switch c.Default {
		case "true":
			c.Default = "1"
		case "false":
			c.Default = "0"
		}
		res = "NUMERIC(1,0)"
	case schemas.Char, schemas.NChar, schemas.Uuid:
		res = "CHAR"
	case schemas.Varchar, schemas.NVarchar, schemas.VARCHAR2, schemas.NVarchar:
		res = "VARCHAR"
	case schemas.Enum, schemas.Set:
		res = "VARCHAR(255)"
	case schemas.TinyText, schemas.MediumText, schemas.Text, schemas.LongText, schemas.XML, schemas.Array:
		res = "TEXT"
	case schemas.Clob:
		res = "CLOB"
	case schemas.TinyBlob, schemas.MediumBlob, schemas.LongBlob, schemas.Blob, schemas.VarBinary, schemas.Bytea:
		res = "BLOB"
	case schemas.Binary:
		res = "BYTE"
	case schemas.Json, schemas.Jsonb:
		res = "JSON"
	case schemas.Date, schemas.Year:
		res = "DATE"
	case schemas.DateTime, schemas.SmallDateTime, schemas.TimeStamp, schemas.TimeStampz, schemas.Time:
		if c.Length >= 1 && c.Length <= 5 {
			return fmt.Sprintf("DATETIME YEAR TO FRACTION(%d)", c.Length)
		} else {
			return "DATETIME YEAR TO  FRACTION(5)"
		}
	default:
		res = t
	}
	hasLen1 := c.Length > 0
	hasLen2 := c.Length2 > 0
	if res == "BIGINT" || res == "INT" || res == "SMALLINT" {
		// GBase 8s INT doesn't need length specification
	} else if hasLen2 {
		res += "(" + strconv.FormatInt(c.Length, 10) + "," + strconv.FormatInt(c.Length2, 10) + ")"
	} else if hasLen1 {
		res += "(" + strconv.FormatInt(c.Length, 10) + ")"
	}
	return res
}

func (db *gbase8s) ColumnTypeKind(t string) int {
	t = strings.Replace(t, "SQLT_", "", 1)
	switch strings.ToUpper(t) {
	case "DATETIME", "DATE", "TIMESTAMP":
		return schemas.TIME_TYPE
	case "CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "TEXT", "VARCHAR2", "NVARCHAR2", "LONG", "CLOB", "NCLOB":
		return schemas.TEXT_TYPE
	case "BLOB", "BYTE":
		return schemas.BLOB_TYPE
	case "BIGINT", "INTEGER", "SMALLINT", "INT", "FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC", "MONEY", "SMALLFLOAT", "NUMBER":
		return schemas.NUMERIC_TYPE
	case "SERIAL", "BIGSERIAL":
		return schemas.NUMERIC_TYPE
	case "BOOLEAN":
		return schemas.BOOL_TYPE
	default:
		return schemas.UNKNOW_TYPE
	}
}

func (db *gbase8s) IsReserved(name string) bool {
	_, ok := gbase8sReservedWords[strings.ToUpper(name)]
	return ok
}

func (db *gbase8s) SetQuotePolicy(quotePolicy QuotePolicy) {
	switch quotePolicy {
	case QuotePolicyNone:
		q := gbase8sQuoter
		q.IsReserved = schemas.AlwaysNoReserve
		db.quoter = q
	case QuotePolicyReserved:
		q := gbase8sQuoter
		q.IsReserved = db.IsReserved
		db.quoter = q
	case QuotePolicyAlways:
		fallthrough
	default:
		db.quoter = gbase8sQuoter
	}
}

func (db *gbase8s) AutoIncrStr() string {
	return "" // "SERIAL"
}

func (db *gbase8s) GetIndexes(queryer core.Queryer, ctx context.Context, tableName string) (map[string]*schemas.Index, error) {
	s := `select col.colname AS column_name, idx.idxtype AS uniqueness, idx.idxname AS index_name FROM sysindexes idx
		JOIN systables tab ON idx.tabid = tab.tabid
		JOIN (
			SELECT tabid, idxname, part1 AS colno, 1 AS seq FROM sysindexes WHERE part1 > 0
			UNION ALL
			SELECT tabid, idxname, part2, 2 FROM sysindexes WHERE part2 > 0
			UNION ALL
			SELECT tabid, idxname, part3, 3 FROM sysindexes WHERE part3 > 0
			UNION ALL
			SELECT tabid, idxname, part4, 4 FROM sysindexes WHERE part4 > 0
		) AS part ON part.idxname = idx.idxname AND part.tabid = idx.tabid
		JOIN syscolumns col ON col.tabid = idx.tabid AND col.colno = part.colno
		WHERE  tab.tabname =:1 ORDER BY idx.idxname, part.seq`
	s = compressStr(s)
	rows, err := queryer.QueryContext(ctx, s, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	proCols, err := db.primaryKeys(queryer, ctx, tableName)
	if err != nil {
		return nil, err
	}

	indexes := make(map[string]*schemas.Index)
	for rows.Next() {
		var indexType int
		var indexName, colName, uniqueness string
		err = rows.Scan(&colName, &uniqueness, &indexName)
		if err != nil {
			return nil, err
		}
		indexName = strings.Trim(indexName, `" `)
		if inSlice(colName, proCols) {
			continue
		}
		var isRegular bool
		if strings.HasPrefix(indexName, "IDX_"+tableName) || strings.HasPrefix(indexName, "UQE_"+tableName) {
			indexName = indexName[5+len(tableName):]
			isRegular = true
		}
		if uniqueness == "U" {
			indexType = schemas.UniqueType
		} else {
			indexType = schemas.IndexType
		}

		var index *schemas.Index
		var ok bool
		if index, ok = indexes[indexName]; !ok {
			index = new(schemas.Index)
			index.Type = indexType
			index.Name = indexName
			index.IsRegular = isRegular
			indexes[indexName] = index
		}
		index.AddColumn(colName)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return indexes, nil
}

func (db *gbase8s) IndexCheckSQL(tableName, idxName string) (string, []interface{}) {
	args := []interface{}{tableName, idxName}
	sql := `SELECT idx.idxname FROM sysindexes idx
			JOIN systables tab ON idx.tabid = tab.tabid
			JOIN syscolumns col ON col.tabid = tab.tabid
			WHERE tab.tabname = :1 and idx.idxname = :2 AND col.colno IN (
				idx.part1, idx.part2, idx.part3, idx.part4, idx.part5, idx.part6, idx.part7, idx.part8,
				idx.part9, idx.part10, idx.part11, idx.part12, idx.part13, idx.part14, idx.part15, idx.part16
			);`
	sql = compressStr(sql)
	return sql, args
}

func (db *gbase8s) DropIndexSQL(tableName string, index *schemas.Index) string {
	var name string
	if index.IsRegular {
		name = index.XName(tableName)
	} else {
		name = index.Name
	}
	return fmt.Sprintf("DROP INDEX %s", db.quoter.Quote(name))
}

func (db *gbase8s) GetTables(queryer core.Queryer, ctx context.Context) ([]*schemas.Table, error) {
	s := "SELECT tabname FROM systables WHERE tabid >= 100 AND tabtype = 'T'"
	rows, err := queryer.QueryContext(ctx, s)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]*schemas.Table, 0)
	for rows.Next() {
		table := schemas.NewEmptyTable()
		err = rows.Scan(&table.Name)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return tables, nil
}

func (db *gbase8s) IsTableExist(queryer core.Queryer, ctx context.Context, tableName string) (bool, error) {
	return db.HasRecords(queryer, ctx, `SELECT tabname FROM systables WHERE tabname = :1`, tableName)
}

func (db *gbase8s) CreateTableSQL(ctx context.Context, queryer core.Queryer, table *schemas.Table, tableName string) (string, bool, error) {
	sql := "CREATE TABLE "
	if tableName == "" {
		tableName = table.Name
	}
	quoter := db.Quoter()
	sql += quoter.Quote(tableName) + " ("
	pkList := table.PrimaryKeys
	for _, colName := range table.ColumnsSeq() {
		col := table.GetColumn(colName)
		s, _ := ColumnString(db, col, false, false)
		sql += s
		if len(col.Comment) > 0 {
			sql += fmt.Sprintf(" COMMENT '%s'", col.Comment)
		}
		sql = strings.TrimSpace(sql)
		sql += ", "
	}
	if len(pkList) > 0 {
		sql += "PRIMARY KEY ( "
		sql += quoter.Join(pkList, ",")
		sql += " ), "
	}
	sql = sql[:len(sql)-2] + ")"
	return sql, false, nil
}

func (db *gbase8s) DropTableSQL(tableName string) (string, bool) {
	return fmt.Sprintf("DROP TABLE %s", db.quoter.Quote(tableName)), false
}

func (db *gbase8s) GetColumns(queryer core.Queryer, ctx context.Context, tableName string) ([]string, map[string]*schemas.Column, error) {
	s := `SELECT c.colname, c.coltypename, c.collength, MOD(c.collength, 256) AS scale, (c.collength - MOD(c.collength,256)) / 256 AS precision, 
			df.default, cm.comments, cs.constrtype FROM syscolumnsext c
			left join  systables t on c.tabid = t.tabid 
			left join  syscolcomms cm on c.colno = cm.colno and  c.tabid = cm.tabid 
			left join  sysdefaultsexpr df on c.colno = df.colno and  c.tabid = df.tabid and df.type = 'T'
			left join  syscoldepend nl on c.colno = nl.colno and  c.tabid = nl.tabid 
			left join  sysconstraints cs on nl.constrid = cs.constrid and  c.tabid = cs.tabid 
			where t.tabname = :1`
	s = compressStr(s)
	rows, err := queryer.QueryContext(ctx, s, tableName)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	proCols, err := db.primaryKeys(queryer, ctx, tableName)
	if err != nil {
		return nil, nil, err
	}

	cols := make(map[string]*schemas.Column)
	colSeq := make([]string, 0)
	for rows.Next() {
		col := new(schemas.Column)
		col.Indexes = make(map[string]int)
		var colName, colDefault, nullable, dataType, comment *string
		var dataLen, precision, scale int64
		err = rows.Scan(&colName, &dataType, &dataLen, &scale, &precision, &colDefault, &comment, &nullable)
		if err != nil {
			return nil, nil, err
		}

		col.Name = strings.Trim(*colName, `" `)
		if colDefault != nil {
			col.Default = *colDefault
			col.DefaultIsEmpty = false
		} else {
			col.DefaultIsEmpty = true
		}
		if inSlice(col.Name, proCols) {
			col.IsPrimaryKey = true
		}

		if nullable != nil && *nullable == "N" {
			col.Nullable = false
		} else {
			col.Nullable = true
		}
		if comment != nil {
			col.Comment = *comment
		}

		var ignore bool
		var dt string
		var len1, len2 int64
		dts := strings.Split(*dataType, "(")
		dt = dts[0]
		if len(dts) > 1 {
			lens := strings.Split(dts[1][:len(dts[1])-1], ",")
			if len(lens) > 1 {
				len1, _ = strconv.ParseInt(lens[0], 10, 64)
				len2, _ = strconv.ParseInt(lens[1], 10, 64)
			} else {
				len1, _ = strconv.ParseInt(lens[0], 10, 64)
			}
		}
		if scale != 0 && precision != 0 {
			col.Length = precision
			col.Length2 = scale
		} else {
			col.Length = scale
		}
		if strings.Contains(dt, "SERIAL") {
			col.IsAutoIncrement = true
		}

		switch dt {
		case "SERIAL8":
			col.SQLType = schemas.SQLType{Name: schemas.BigSerial, DefaultLength: len1, DefaultLength2: len2}
		case "VARCHAR2":
			col.SQLType = schemas.SQLType{Name: schemas.Varchar, DefaultLength: len1, DefaultLength2: len2}
		case "NVARCHAR2":
			col.SQLType = schemas.SQLType{Name: schemas.NVarchar, DefaultLength: len1, DefaultLength2: len2}
		case "TIMESTAMP WITH TIME ZONE":
			col.SQLType = schemas.SQLType{Name: schemas.TimeStampz, DefaultLength: 0, DefaultLength2: 0}
		case "DATETIME YEAR TO SECOND":
			col.SQLType = schemas.SQLType{Name: schemas.TimeStamp, DefaultLength: 0, DefaultLength2: 0}
		case "LONG", "LONG RAW":
			col.SQLType = schemas.SQLType{Name: schemas.Text, DefaultLength: 0, DefaultLength2: 0}
		case "RAW":
			col.SQLType = schemas.SQLType{Name: schemas.Binary, DefaultLength: 0, DefaultLength2: 0}
		case "ROWID":
			col.SQLType = schemas.SQLType{Name: schemas.Varchar, DefaultLength: 18, DefaultLength2: 0}
		case "SMALLFLOAT":
			col.SQLType = schemas.SQLType{Name: schemas.Real, DefaultLength: 0, DefaultLength2: 0}
		case "BYTE":
			col.SQLType = schemas.SQLType{Name: schemas.Binary, DefaultLength: len1, DefaultLength2: len2}
		case "AQ$_SUBSCRIBERS":
			ignore = true
		default:
			if strings.Contains(dt, "DATETIME") && strings.Contains(dt, "FRACTION") {
				col.SQLType = schemas.SQLType{Name: schemas.TimeStamp, DefaultLength: 0, DefaultLength2: 0}
			} else {
				col.SQLType = schemas.SQLType{Name: strings.ToUpper(dt), DefaultLength: len1, DefaultLength2: len2}
			}
		}
		if ignore {
			continue
		}
		if _, ok := schemas.SqlTypes[col.SQLType.Name]; !ok {
			return nil, nil, fmt.Errorf("Unknown colType %v %v", *dataType, col.SQLType)
		}

		cols[col.Name] = col
		colSeq = append(colSeq, col.Name)
	}
	if rows.Err() != nil {
		return nil, nil, rows.Err()
	}
	return colSeq, cols, nil
}

func (db *gbase8s) ModifyColumnSQL(tableName string, col *schemas.Column) string {
	s, _ := ColumnString(db.dialect, col, false, true)
	modifyColumnSQL := fmt.Sprintf("ALTER TABLE %s MODIFY %s;", db.quoter.Quote(tableName), s)
	if col.Comment != "" {
		modifyColumnSQL += fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s'", db.quoter.Quote(tableName), db.quoter.Quote(col.Name), col.Comment)
	}
	return modifyColumnSQL
}

func (db *gbase8s) IsColumnExist(queryer core.Queryer, ctx context.Context, tableName, colName string) (bool, error) {
	args := []interface{}{tableName, colName}
	query := "SELECT colname FROM syscolumnsext c, systables t WHERE c.tabid = t.tabid and tabname = :1 AND colname = :2"
	return db.HasRecords(queryer, ctx, query, args...)
}

func (db *gbase8s) Filters() []Filter {
	return []Filter{}
}

type gbase8sDriver struct {
	baseDriver
}

func (g *gbase8sDriver) Features() *DriverFeatures {
	return &DriverFeatures{
		SupportReturnInsertedID: true,
	}
}

func (g *gbase8sDriver) GenScanResult(colType string) (interface{}, error) {
	colType = strings.Replace(colType, "SQLT_", "", 1)
	switch colType {
	case "CHAR", "NCHAR", "VARCHAR", "VARCHAR2", "NVARCHAR2", "AFC":
		var s sql.NullString
		return &s, nil
	case "TEXT", "CLOB", "NCLOB", "CLOB2", "JSON", "XMLTYPE":
		var s sql.NullString
		return &s, nil
	case "BYTE", "BLOB":
		var r sql.RawBytes
		return &r, nil
	case "SERIAL", "SERIAL8", "BIGSERIAL", "BIGINT", "INT8":
		var s sql.NullInt64
		return &s, nil
	case "INTEGER", "INT", "SMALLINT":
		var s sql.NullInt32
		return &s, nil
	case "FLOAT", "SMALLFLOAT", "REAL":
		var s sql.NullFloat64
		return &s, nil
	case "DECIMAL", "NUMERIC", "MONEY", "NUMBER", "BDOUBLE":
		var s sql.NullFloat64
		return &s, nil
	case "DATE", "DATETIME", "TIMESTAMP", "DATETIME YEAR TO SECOND", "DATETIME YEAR TO FRACTION":
		var s sql.NullTime
		return &s, nil
	case "INTERVAL":
		var s sql.NullString
		return &s, nil
	case "BOOLEAN":
		var s sql.NullBool
		return &s, nil
	default:
		// Check whether it is a variant of DATETIME YEAR TO FRACTION
		if strings.Contains(colType, "DATETIME") && strings.Contains(colType, "FRACTION") {
			var s sql.NullTime
			return &s, nil
		}
		var r sql.RawBytes
		return &r, nil
	}
}

// dataSourceName=user/password@ipv4:port/dbname
// gbase8s://user:password@ip:port/dbname?param2=1&param2=2
func (o *gbase8sDriver) Parse(driverName, dataSourceName string) (*URI, error) {
	db := &URI{DBType: schemas.GBASE8S}
	dsnPattern := regexp.MustCompile(
		`^(?P<user>.*):(?P<password>.*)@` + // user:password@
			`(?P<net>.*)` + // ip:port
			`\/(?P<dbname>.*)`) // dbname
	matches := dsnPattern.FindStringSubmatch(dataSourceName)
	names := dsnPattern.SubexpNames()
	for i, match := range matches {
		if names[i] == "dbname" {
			db.DBName = match
		}
	}
	if db.DBName == "" && len(matches) != 0 {
		return nil, errors.New("dbname is empty")
	}
	return db, nil
}

func (db *gbase8s) primaryKeys(queryer core.Queryer, ctx context.Context, tableName string) ([]string, error) {
	s := `SELECT col.colname AS column_name FROM systables t
		JOIN sysconstraints con ON con.tabid = t.tabid
		JOIN sysindexes idx ON con.idxname = idx.idxname
		JOIN syscolumns col ON col.tabid = t.tabid AND col.colno IN (
			idx.part1, idx.part2, idx.part3, idx.part4, idx.part5, idx.part6, idx.part7, idx.part8,
			idx.part9, idx.part10, idx.part11, idx.part12, idx.part13, idx.part14, idx.part15, idx.part16
		)
		WHERE t.tabname = :1 AND con.constrtype = 'P'`
	s = compressStr(s) // Compress whitespace in the query string
	rows, err := queryer.QueryContext(ctx, s, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	priCols := make([]string, 0)
	for rows.Next() {
		var priCol string
		err = rows.Scan(&priCol)
		if err != nil {
			return nil, err
		}
		priCols = append(priCols, priCol)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return priCols, nil
}

func inSlice(target string, list []string) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}

// Replace all consecutive whitespace characters with a single space using regex
func compressStr(s string) string {
	re := regexp.MustCompile(`\s+`)
	return strings.TrimSpace(re.ReplaceAllString(s, " "))
}

package ignite

import (
	"github.com/amsokol/ignite-go-client/binary/errors"
)

const (
	// StatementTypeAny is ANY = 0
	StatementTypeAny byte = 0

	// StatementTypeSelect is SELECT = 1
	StatementTypeSelect byte = 1

	// StatementTypeUpdate is UPDATE = 2
	StatementTypeUpdate byte = 2
)

// QuerySQLData input parameter for QuerySQL func
type QuerySQLData struct {
	// Name of a type or SQL table.
	Table string

	// SQL query string.
	Query string

	// Query arguments.
	QueryArgs []interface{}

	// Distributed joins.
	DistributedJoins bool

	// Local query.
	LocalQuery bool

	// Replicated only - Whether query contains only replicated tables or not.
	ReplicatedOnly bool

	// Cursor page size.
	PageSize int

	// Timeout(milliseconds) value should be non-negative. Zero value disables timeout.
	Timeout int64
}

// QuerySQLPage is query result page
type QuerySQLPage struct {
	// Key -> Values
	Rows map[interface{}]interface{}

	// Indicates whether more results are available to be fetched with QuerySQLCursorGetPage.
	// When true, query cursor is closed automatically.
	HasMore bool
}

// QuerySQLResult output from QuerySQL func
type QuerySQLResult struct {
	// Cursor id. Can be closed with ResourceClose func.
	ID int64

	// Query result first page
	QuerySQLPage
}

// QuerySQLFieldsData input parameter for QuerySQLFields func
type QuerySQLFieldsData struct {
	// Schema for the query; can be empty, in which case default PUBLIC schema will be used.
	Schema string

	// Query cursor page size.
	PageSize int

	// Max rows.
	MaxRows int

	// SQL query string.
	Query string

	// Query arguments.
	QueryArgs []interface{}

	// Statement type.
	// ANY = 0
	// SELECT = 1
	// UPDATE = 2
	StatementType byte

	// Distributed joins.
	DistributedJoins bool

	// Local query.
	LocalQuery bool

	// Replicated only - Whether query contains only replicated tables or not.
	ReplicatedOnly bool

	// Enforce join order.
	EnforceJoinOrder bool

	// Collocated - Whether your data is co-located or not.
	Collocated bool

	// Lazy query execution.
	LazyQuery bool

	// Timeout(milliseconds) value should be non-negative. Zero value disables timeout.
	Timeout int64

	// Include field names.
	IncludeFieldNames bool
}

// QuerySQLFieldsPage is query result page
type QuerySQLFieldsPage struct {
	// Values
	Rows [][]interface{}

	// Indicates whether more results are available to be fetched with QuerySQLFieldsCursorGetPage.
	// When true, query cursor is closed automatically.
	HasMore bool
}

// QuerySQLFieldsResult output from QuerySQLFields func
type QuerySQLFieldsResult struct {
	// Cursor id. Can be closed with ResourceClose func.
	ID int64

	// Field (column) count.
	FieldCount int

	// Needed only when IncludeFieldNames is true in the request.
	// Column names.
	Fields []string

	// Query result first page
	QuerySQLFieldsPage
}

// QueryScanData input parameter for QueryScan func
type QueryScanData struct {
	// Cursor page size.
	PageSize int

	// Number of partitions to query (negative to query entire cache).
	Partitions int

	// Local flag - whether this query should be executed on local node only.
	LocalQuery bool
}

// QueryScanPage is query result page
type QueryScanPage struct {
	// Key -> Values
	Rows map[interface{}]interface{}

	// Indicates whether more results are available to be fetched with QueryScanCursorGetPage.
	// When true, query cursor is closed automatically.
	HasMore bool
}

// QueryScanResult output from QueryScan func
type QueryScanResult struct {
	// Cursor id. Can be closed with ResourceClose func.
	ID int64

	// Query result first page
	QueryScanPage
}

func (c *client) QuerySQL(cache string, binary bool, data QuerySQLData) (QuerySQLResult, error) {
	// request and response
	req := NewRequestOperation(OpQuerySQL)
	res := NewResponseOperation(req.UID)

	r := QuerySQLResult{QuerySQLPage: QuerySQLPage{Rows: map[interface{}]interface{}{}}}
	var err error

	// set parameters
	if err = WriteInt(req, HashCode(cache)); err != nil {
		return r, errors.Wrapf(err, "failed to write cache name")
	}
	if err = WriteBool(req, binary); err != nil {
		return r, errors.Wrapf(err, "failed to write binary flag")
	}
	if err = WriteOString(req, data.Table); err != nil {
		return r, errors.Wrapf(err, "failed to write table name")
	}
	if err = WriteOString(req, data.Query); err != nil {
		return r, errors.Wrapf(err, "failed to write query")
	}

	var l int32
	if data.QueryArgs != nil {
		l = int32(len(data.QueryArgs))
	}
	// write args
	if err = WriteInt(req, l); err != nil {
		return r, errors.Wrapf(err, "failed to write query arg count")
	}
	if l > 0 {
		for i, v := range data.QueryArgs {
			if err = WriteObject(req, v); err != nil {
				return r, errors.Wrapf(err, "failed to write query arg with index %d", i)
			}
		}
	}

	if err = WriteBool(req, data.DistributedJoins); err != nil {
		return r, errors.Wrapf(err, "failed to write distributed joins flag")
	}
	if err = WriteBool(req, data.LocalQuery); err != nil {
		return r, errors.Wrapf(err, "failed to write local query flag")
	}
	if err = WriteBool(req, data.ReplicatedOnly); err != nil {
		return r, errors.Wrapf(err, "failed to write replicated only flag")
	}
	if err = WriteInt(req, int32(data.PageSize)); err != nil {
		return r, errors.Wrapf(err, "failed to write page size")
	}
	if err = WriteLong(req, data.Timeout); err != nil {
		return r, errors.Wrapf(err, "failed to write timeout")
	}

	// execute operation
	if err = c.Do(req, res); err != nil {
		return r, errors.Wrapf(err, "failed to execute OP_QUERY_SQL operation")
	}
	if err = res.CheckStatus(); err != nil {
		return r, err
	}

	// process result
	if r.ID, err = ReadLong(res); err != nil {
		return r, errors.Wrapf(err, "failed to read cursor ID")
	}
	count, err := ReadInt(res)
	if err != nil {
		return r, errors.Wrapf(err, "failed to read row count")
	}
	// read data
	for i := 0; i < int(count); i++ {
		key, err := ReadObject(res)
		if err != nil {
			return r, errors.Wrapf(err, "failed to read key with index %d", i)
		}
		value, err := ReadObject(res)
		if err != nil {
			return r, errors.Wrapf(err, "failed to read value with index %d", i)
		}
		r.Rows[key] = value
	}
	if r.HasMore, err = ReadBool(res); err != nil {
		return r, errors.Wrapf(err, "failed to read has more flag")
	}
	return r, nil
}

// QuerySQLCursorGetPage retrieves the next SQL query cursor page by cursor id from QuerySQL.
func (c *client) QuerySQLCursorGetPage(id int64) (QuerySQLPage, error) {
	// request and response
	req := NewRequestOperation(OpQuerySQLCursorGetPage)
	res := NewResponseOperation(req.UID)

	r := QuerySQLPage{Rows: map[interface{}]interface{}{}}
	var err error

	// set parameters
	if err = WriteLong(req, id); err != nil {
		return r, errors.Wrapf(err, "failed to write cursor id")
	}

	// execute operation
	if err = c.Do(req, res); err != nil {
		return r, errors.Wrapf(err, "failed to execute OP_QUERY_SQL_CURSOR_GET_PAGE operation")
	}
	if err = res.CheckStatus(); err != nil {
		return r, err
	}

	// process result
	count, err := ReadInt(res)
	if err != nil {
		return r, errors.Wrapf(err, "failed to read row count")
	}
	// read data
	for i := 0; i < int(count); i++ {
		key, err := ReadObject(res)
		if err != nil {
			return r, errors.Wrapf(err, "failed to read key with index %d", i)
		}
		value, err := ReadObject(res)
		if err != nil {
			return r, errors.Wrapf(err, "failed to read value with index %d", i)
		}
		r.Rows[key] = value
	}
	if r.HasMore, err = ReadBool(res); err != nil {
		return r, errors.Wrapf(err, "failed to read has more flag")
	}

	return r, nil
}

func (c *client) QuerySQLFieldsRaw(cache string, binary bool, data QuerySQLFieldsData) (*ResponseOperation, error) {
	// request and response
	req := NewRequestOperation(OpQuerySQLFields)
	res := NewResponseOperation(req.UID)

	var err error

	// set parameters
	if err := WriteInt(req, HashCode(cache)); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache name")
	}
	if err := WriteBool(req, binary); err != nil {
		return nil, errors.Wrapf(err, "failed to write binary flag")
	}
	if len(data.Schema) > 0 {
		if err := WriteOString(req, data.Schema); err != nil {
			return nil, errors.Wrapf(err, "failed to write schema for the query")
		}
	} else {
		if err := WriteNull(req); err != nil {
			return nil, errors.Wrapf(err, "failed to write nil for schema for the query")
		}
	}
	if err = WriteInt(req, int32(data.PageSize)); err != nil {
		return nil, errors.Wrapf(err, "failed to write page size")
	}
	if err = WriteInt(req, int32(data.MaxRows)); err != nil {
		return nil, errors.Wrapf(err, "failed to write max rows")
	}
	if err = WriteOString(req, data.Query); err != nil {
		return nil, errors.Wrapf(err, "failed to write query")
	}

	var l int32
	if data.QueryArgs != nil {
		l = int32(len(data.QueryArgs))
	}
	// write args
	if err = WriteInt(req, l); err != nil {
		return nil, errors.Wrapf(err, "failed to write query arg count")
	}
	if l > 0 {
		for i, v := range data.QueryArgs {
			if err = WriteObject(req, v); err != nil {
				return nil, errors.Wrapf(err, "failed to write query arg with index %d", i)
			}
		}
	}

	if err = WriteByte(req, data.StatementType); err != nil {
		return nil, errors.Wrapf(err, "failed to write statement type")
	}
	if err = WriteBool(req, data.DistributedJoins); err != nil {
		return nil, errors.Wrapf(err, "failed to write distributed joins flag")
	}
	if err = WriteBool(req, data.LocalQuery); err != nil {
		return nil, errors.Wrapf(err, "failed to write local query flag")
	}
	if err = WriteBool(req, data.ReplicatedOnly); err != nil {
		return nil, errors.Wrapf(err, "failed to write replicated only flag")
	}
	if err = WriteBool(req, data.EnforceJoinOrder); err != nil {
		return nil, errors.Wrapf(err, "failed to write enforce join order flag")
	}
	if err = WriteBool(req, data.Collocated); err != nil {
		return nil, errors.Wrapf(err, "failed to write collocated flag")
	}
	if err = WriteBool(req, data.LazyQuery); err != nil {
		return nil, errors.Wrapf(err, "failed to write lazy query flag")
	}
	if err = WriteLong(req, data.Timeout); err != nil {
		return nil, errors.Wrapf(err, "failed to write timeout")
	}
	if err = WriteBool(req, data.IncludeFieldNames); err != nil {
		return nil, errors.Wrapf(err, "failed to write include field names flag")
	}

	// execute operation
	if err = c.Do(req, res); err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_QUERY_SQL_FIELDS operation")
	}
	if err = res.CheckStatus(); err != nil {
		return nil, err
	}

	return res, nil
}

// QuerySQLFields performs SQL fields query.
func (c *client) QuerySQLFields(cache string, binary bool, data QuerySQLFieldsData) (QuerySQLFieldsResult, error) {
	var r QuerySQLFieldsResult

	res, err := c.QuerySQLFieldsRaw(cache, binary, data)
	if err != nil {
		return r, err
	}

	// read field names
	if r.ID, err = ReadLong(res); err != nil {
		return r, errors.Wrapf(err, "failed to read cursor ID")
	}
	fieldCount, err := ReadInt(res)
	if err != nil {
		return r, errors.Wrapf(err, "failed to read field count")
	}
	r.FieldCount = int(fieldCount)
	if data.IncludeFieldNames {
		r.Fields = make([]string, 0, fieldCount)
		for i := 0; i < r.FieldCount; i++ {
			var s string
			if s, err = ReadOString(res); err != nil {
				return r, errors.Wrapf(err, "failed to read field name with index %d", i)
			}
			r.Fields = append(r.Fields, s)
		}
	} else {
		r.Fields = []string{}
	}

	// read data
	rowCount, err := ReadInt(res)
	if err != nil {
		return r, errors.Wrapf(err, "failed to read row count")
	}
	r.Rows = make([][]interface{}, rowCount)
	for i := 0; i < int(rowCount); i++ {
		r.Rows[i] = make([]interface{}, r.FieldCount)
		for j := 0; j < r.FieldCount; j++ {
			r.Rows[i][j], err = ReadObject(res)
			if err != nil {
				return r, errors.Wrapf(err, "failed to read value (row with index %d, column with index %d", i, j)
			}
		}
	}
	if r.HasMore, err = ReadBool(res); err != nil {
		return r, errors.Wrapf(err, "failed to read has more flag")
	}

	return r, nil
}

func (c *client) QuerySQLFieldsCursorGetPageRaw(id int64) (*ResponseOperation, error) {
	// request and response
	req := NewRequestOperation(OpQuerySQLFieldsCursorGetPage)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := WriteLong(req, id); err != nil {
		return nil, errors.Wrapf(err, "failed to write cursor id")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_QUERY_SQL_FIELDS_CURSOR_GET_PAGE operation")
	}
	if err := res.CheckStatus(); err != nil {
		return nil, err
	}

	return res, nil
}

// QuerySQLFieldsCursorGetPage retrieves the next query result page by cursor id from QuerySQLFields.
func (c *client) QuerySQLFieldsCursorGetPage(id int64, fieldCount int) (QuerySQLFieldsPage, error) {
	var r QuerySQLFieldsPage

	res, err := c.QuerySQLFieldsCursorGetPageRaw(id)
	if err != nil {
		return r, err
	}

	// read data
	rowCount, err := ReadInt(res)
	if err != nil {
		return r, errors.Wrapf(err, "failed to read row count")
	}
	r.Rows = make([][]interface{}, rowCount)
	for i := 0; i < int(rowCount); i++ {
		r.Rows[i] = make([]interface{}, fieldCount)
		for j := 0; j < int(fieldCount); j++ {
			r.Rows[i][j], err = ReadObject(res)
			if err != nil {
				return r, errors.Wrapf(err, "failed to read value (row with index %d, column with index %d", i, j)
			}
		}
	}
	if r.HasMore, err = ReadBool(res); err != nil {
		return r, errors.Wrapf(err, "failed to read has more flag")
	}

	return r, nil
}

func (c *client) QueryScan(cache string, binary bool, data QueryScanData) (QueryScanResult, error) {
	// request and response
	req := NewRequestOperation(OpQueryScan)
	res := NewResponseOperation(req.UID)

	r := QueryScanResult{QueryScanPage: QueryScanPage{Rows: map[interface{}]interface{}{}}}
	var err error

	// set parameters
	if err = WriteInt(req, HashCode(cache)); err != nil {
		return r, errors.Wrapf(err, "failed to write cache name")
	}
	if err = WriteBool(req, binary); err != nil {
		return r, errors.Wrapf(err, "failed to write binary flag")
	}
	// filtering is not supported
	if err = WriteNull(req); err != nil {
		return r, errors.Wrapf(err, "failed to write null as filter object")
	}

	if err = WriteInt(req, int32(data.PageSize)); err != nil {
		return r, errors.Wrapf(err, "failed to write page size")
	}
	if err = WriteInt(req, int32(data.Partitions)); err != nil {
		return r, errors.Wrapf(err, "failed to write number of partitions to query")
	}
	if err = WriteBool(req, data.LocalQuery); err != nil {
		return r, errors.Wrapf(err, "failed to write local query flag")
	}

	// execute operation
	if err = c.Do(req, res); err != nil {
		return r, errors.Wrapf(err, "failed to execute OP_QUERY_SCAN operation")
	}
	if err = res.CheckStatus(); err != nil {
		return r, err
	}

	// process result
	if r.ID, err = ReadLong(res); err != nil {
		return r, errors.Wrapf(err, "failed to read cursor ID")
	}
	count, err := ReadInt(res)
	if err != nil {
		return r, errors.Wrapf(err, "failed to read row count")
	}
	// read data
	for i := 0; i < int(count); i++ {
		key, err := ReadObject(res)
		if err != nil {
			return r, errors.Wrapf(err, "failed to read key with index %d", i)
		}
		value, err := ReadObject(res)
		if err != nil {
			return r, errors.Wrapf(err, "failed to read value with index %d", i)
		}
		r.Rows[key] = value
	}
	if r.HasMore, err = ReadBool(res); err != nil {
		return r, errors.Wrapf(err, "failed to read has more flag")
	}
	return r, nil
}

// QueryScanCursorGetPage fetches the next SQL query cursor page by cursor id that is obtained from OP_QUERY_SCAN.
func (c *client) QueryScanCursorGetPage(id int64) (QueryScanPage, error) {
	// request and response
	req := NewRequestOperation(OpQueryScanCursorGetPage)
	res := NewResponseOperation(req.UID)

	r := QueryScanPage{Rows: map[interface{}]interface{}{}}
	var err error

	// set parameters
	if err = WriteLong(req, id); err != nil {
		return r, errors.Wrapf(err, "failed to write cursor id")
	}

	// execute operation
	if err = c.Do(req, res); err != nil {
		return r, errors.Wrapf(err, "failed to execute OP_QUERY_SCAN_CURSOR_GET_PAGE operation")
	}
	if err = res.CheckStatus(); err != nil {
		return r, err
	}

	// process result
	count, err := ReadInt(res)
	if err != nil {
		return r, errors.Wrapf(err, "failed to read row count")
	}
	// read data
	for i := 0; i < int(count); i++ {
		key, err := ReadObject(res)
		if err != nil {
			return r, errors.Wrapf(err, "failed to read key with index %d", i)
		}
		value, err := ReadObject(res)
		if err != nil {
			return r, errors.Wrapf(err, "failed to read value with index %d", i)
		}
		r.Rows[key] = value
	}
	if r.HasMore, err = ReadBool(res); err != nil {
		return r, errors.Wrapf(err, "failed to read has more flag")
	}

	return r, nil
}

// ResourceClose closes a resource, such as query cursor.
func (c *client) ResourceClose(id int64) error {
	// request and response
	req := NewRequestOperation(OpResourceClose)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := WriteLong(req, id); err != nil {
		return errors.Wrapf(err, "failed to write cursor id")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return errors.Wrapf(err, "failed to execute OP_RESOURCE_CLOSE operation")
	}

	return res.CheckStatus()
}

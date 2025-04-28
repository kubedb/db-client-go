package ignite

import (
	"io"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

// ResponseHandshake is struct handshake response
type ResponseHandshake struct {
	// Success flag
	Success bool
	// Server version major, minor, patch
	Major, Minor, Patch int
	// Error message
	Message string

	response
}

// ReadFrom is function to read request data from io.Reader.
// Returns read bytes.
func (r *ResponseHandshake) ReadFrom(rr io.Reader) (int64, error) {
	// read response
	n, err := r.response.ReadFrom(rr)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to read handshare response")
	}

	r.Success, err = ReadBool(r)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to read success flag")
	}

	if !r.Success {
		v, err := ReadShort(r)
		if err != nil {
			return 0, errors.Wrapf(err, "failed to read server version major")
		}
		r.Major = int(v)

		v, err = ReadShort(r)
		if err != nil {
			return 0, errors.Wrapf(err, "failed to read server version minor")
		}
		r.Minor = int(v)

		v, err = ReadShort(r)
		if err != nil {
			return 0, errors.Wrapf(err, "failed to read server version patch")
		}
		r.Patch = int(v)

		r.Message, err = ReadOString(r)
		if err != nil {
			return 0, errors.Wrapf(err, "failed to read error message")
		}
	}

	return n, nil
}

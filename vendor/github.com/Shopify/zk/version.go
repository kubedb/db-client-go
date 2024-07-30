package zk

import (
	"fmt"
	"strings"
)

// ParseVersion parses a version string into a Version struct.
func ParseVersion(vs string) Version {
	v, err := ParseVersionErr(vs)
	if err != nil {
		return Version{-1, -1, -1}
	}
	return v
}

// ParseVersionErr parses a version string into a Version struct; returns an error if the string is invalid.
func ParseVersionErr(vs string) (Version, error) {
	var major, minor, patch int
	var err error

	switch strings.Count(vs, ".") {
	case 2:
		_, err = fmt.Sscanf(vs, "%d.%d.%d", &major, &minor, &patch)
	case 1:
		_, err = fmt.Sscanf(vs, "%d.%d", &major, &minor)
	case 0:
		_, err = fmt.Sscanf(vs, "%d", &major)
	default:
		err = fmt.Errorf("too many dots")
	}

	if err != nil {
		return Version{}, fmt.Errorf("invalid version string: %v", err)
	}
	return Version{major, minor, patch}, nil
}

type Version struct {
	Major int
	Minor int
	Patch int
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

func (v Version) IsValid() bool {
	return v.Major != -1
}

func (v Version) LessThan(other Version) bool {
	if v.Major < other.Major {
		return true
	}
	if v.Major > other.Major {
		return false
	}
	if v.Minor < other.Minor {
		return true
	}
	if v.Minor > other.Minor {
		return false
	}
	return v.Patch < other.Patch
}

func (v Version) GreaterThan(other Version) bool {
	if v.Major > other.Major {
		return true
	}
	if v.Major < other.Major {
		return false
	}
	if v.Minor > other.Minor {
		return true
	}
	if v.Minor < other.Minor {
		return false
	}
	return v.Patch > other.Patch
}

func (v Version) Equal(other Version) bool {
	return v.Major == other.Major && v.Minor == other.Minor && v.Patch == other.Patch
}

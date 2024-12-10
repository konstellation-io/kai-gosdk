package prediction

import (
	"errors"
	"time"
)

var (
	ErrDateRangeFilterRequired = errors.New("creation date range is required")
)

type Filter struct {
	RequestID    string
	Workflow     string
	WorkflowType string
	Process      string
	Version      string
	CreationDate TimestampRange
}

type TimestampRange struct {
	StartDate time.Time
	EndDate   time.Time
}

func (f Filter) Validate() error {
	if f.CreationDate.StartDate.IsZero() {
		return ErrDateRangeFilterRequired
	}

	if f.CreationDate.EndDate.IsZero() {
		return ErrDateRangeFilterRequired
	}

	return nil
}

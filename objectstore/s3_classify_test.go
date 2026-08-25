package objectstore

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// apiErr builds an error shaped the way the SDK actually delivers one: a
// transport ResponseError wrapping a smithy APIError.
func apiErr(status int, code string) error {
	return fmt.Errorf("operation error S3: PutObject, %w", &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      &smithy.GenericAPIError{Code: code, Message: "..."},
		},
	})
}

func TestClassify(t *testing.T) {
	cases := []struct {
		name                             string
		err                              error
		notFound, precondition, conflict bool
	}{
		{"412 PreconditionFailed", apiErr(412, "PreconditionFailed"), false, true, false},
		{"409 ConditionalRequestConflict", apiErr(409, "ConditionalRequestConflict"), false, false, true},
		{"404 NoSuchKey", apiErr(404, "NoSuchKey"), true, false, false},
		// The one that matters: a missing BUCKET is also 404, but it must not
		// look like a missing object, or a typo'd bucket reads as an empty log.
		{"404 NoSuchBucket", apiErr(404, "NoSuchBucket"), false, false, false},
		{"typed NoSuchKey", &types.NoSuchKey{}, true, false, false},
		{"500 InternalError", apiErr(500, "InternalError"), false, false, false},
		{"503 SlowDown", apiErr(503, "SlowDown"), false, false, false},
		// A transport error with no HTTP response at all: the ambiguous case.
		// It must be classified as none of the three, so the caller treats the
		// outcome as unknown.
		{"bare transport error", errors.New("read: connection reset by peer"), false, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isNotFound(c.err); got != c.notFound {
				t.Errorf("isNotFound = %v, want %v", got, c.notFound)
			}
			if got := isPreconditionFailed(c.err); got != c.precondition {
				t.Errorf("isPreconditionFailed = %v, want %v", got, c.precondition)
			}
			if got := isConflict(c.err); got != c.conflict {
				t.Errorf("isConflict = %v, want %v", got, c.conflict)
			}
		})
	}
}

// TestConflictNotPrecondition pins the distinction the retry loop depends on:
// 409 and 412 must never map to the same sentinel.
func TestConflictNotPrecondition(t *testing.T) {
	c := apiErr(409, "ConditionalRequestConflict")
	if isPreconditionFailed(c) {
		t.Fatal("409 must not classify as a precondition failure: it would drive the unbounded re-plan loop")
	}
	wrapped := fmt.Errorf("%w: %s", ErrConflict, c)
	if !errors.Is(wrapped, ErrConflict) || errors.Is(wrapped, ErrPreconditionFailed) {
		t.Fatal("ErrConflict must be distinguishable from ErrPreconditionFailed")
	}
}

package json_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/isaquesb/go-event-bus"
	eventjson "github.com/isaquesb/go-event-bus/json"
)

// FuzzRegistryDecode feeds arbitrary bytes to Decode. A panic fails the run;
// so does an error that is not a *event.DecodeError, or a nil error that comes
// with a nil event.
func FuzzRegistryDecode(f *testing.F) {
	for _, pattern := range []string{"testdata/*.json", "testdata/golden/*.golden.json"} {
		files, err := filepath.Glob(pattern)
		if err != nil {
			f.Fatalf("glob %s: %v", pattern, err)
		}
		for _, file := range files {
			data, err := os.ReadFile(file)
			if err != nil {
				f.Fatalf("read %s: %v", file, err)
			}
			f.Add(data)
		}
	}

	for _, seed := range []string{
		``,
		`{}`,
		`null`,
		`[]`,
		`{"name":"x"}`,
		`{"name":"user.created","version":2,"payload":{}}`,
	} {
		f.Add([]byte(seed))
	}

	reg := eventjson.NewRegistry()
	reg.Register("user.created", func() event.Event { return &UserCreatedV1{} }, 1)
	reg.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)
	reg.RegisterUpcaster(&userCreatedV1ToV2Upcaster{})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, evt, err := reg.Decode(context.Background(), data)
		if err != nil {
			var de *event.DecodeError
			if !errors.As(err, &de) {
				t.Fatalf("error is not a *event.DecodeError: %T %v", err, err)
			}
			return
		}
		if evt == nil {
			t.Fatal("nil error with a nil event")
		}
	})
}

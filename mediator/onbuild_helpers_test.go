package mediator_test

import (
	"context"
	"reflect"
	"time"
)

type ctxT = context.Context

func reflectTypeOf(v any) reflect.Type { return reflect.TypeOf(v) }

func timeoutAfter() <-chan time.Time { return time.After(5 * time.Second) }

package otsql

import (
	"context"
	"database/sql/driver"

	"github.com/opentracing/opentracing-go"
)

type otRows struct {
	parent driver.Rows
	ctx    context.Context
	tracer opentracing.Tracer
}

func (r otRows) Columns() []string {
	return r.parent.Columns()
}

func (r otRows) Close() error {
	span := opentracing.SpanFromContext(r.ctx)
	span = r.tracer.StartSpan("sql:rows_close", opentracing.ChildOf(span.Context()))
	_ = opentracing.ContextWithSpan(r.ctx, span)
	defer span.Finish()

	return r.parent.Close()
}

func (r otRows) Next(dest []driver.Value) error {
	span := opentracing.SpanFromContext(r.ctx)

	span = r.tracer.StartSpan("sql:rows_next", opentracing.ChildOf(span.Context()))
	_ = opentracing.ContextWithSpan(r.ctx, span)
	defer span.Finish()

	return r.parent.Next(dest)
}

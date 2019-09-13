package otsql

import (
	"context"
	"database/sql/driver"
	"strconv"

	"github.com/opentracing/opentracing-go"
)

type conn interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.Pinger
	driver.Execer
	driver.ExecerContext
	driver.Queryer
	driver.QueryerContext
}

// otConn implements driver.Conn
type otConn struct {
	parent driver.Conn
	tracer opentracing.Tracer
}

func (c otConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.parent.Prepare(query)
	if err != nil {
		return nil, err
	}

	return wrapStmt(stmt, query), nil
}

func (c otConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if prepCtx, ok := c.parent.(driver.ConnPrepareContext); ok {
		return prepCtx.PrepareContext(ctx, query)
	}
	return c.parent.Prepare(query)
}

func (c otConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.TODO(), driver.TxOptions{})
}

func (c otConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if connBeginTx, ok := c.parent.(driver.ConnBeginTx); ok {
		return connBeginTx.BeginTx(ctx, opts)
	}
	return c.parent.Begin()
}

func (c otConn) Close() error {
	return c.parent.Close()
}

func (c otConn) Ping(ctx context.Context) error {
	if pinger, ok := c.parent.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

func (c otConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if exec, ok := c.parent.(driver.Execer); ok {
		return exec.Exec(query, args)
	}

	return nil, driver.ErrSkip
}

func (c otConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if execCtx, ok := c.parent.(driver.ExecerContext); ok {
		return execCtx.ExecContext(ctx, query, args)
	}

	return nil, driver.ErrSkip
}

func (c otConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if queryer, ok := c.parent.(driver.Queryer); ok {
		return queryer.Query(query, args)
	}

	return nil, driver.ErrSkip
}

func (c otConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if queryerCtx, ok := c.parent.(driver.QueryerContext); ok {
		span := opentracing.SpanFromContext(ctx)
		if span == nil {
			return queryerCtx.QueryContext(ctx, query, args)
		}

		span = c.tracer.StartSpan("sql:query", opentracing.ChildOf(span.Context()))
		ctx := opentracing.ContextWithSpan(ctx, span)
		defer span.Finish()

		span.SetTag("sql.query", query)
		for _, arg := range args {
			if arg.Name != "" {
				span.SetTag("sql.arg."+arg.Name, arg.Value)
			} else {
				span.SetTag("sql.arg."+strconv.Itoa(arg.Ordinal), arg.Value)
			}
		}

		rows, err := queryerCtx.QueryContext(ctx, query, args)
		if err != nil {
			return nil, err
		}

		return otRows{parent: rows, ctx: ctx, tracer: c.tracer}, nil
	}

	return nil, driver.ErrSkip
}

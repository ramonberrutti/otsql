package otsql

import (
	"context"
	"database/sql/driver"

	"github.com/opentracing/opentracing-go"
)

type otDriver struct {
	parent    driver.Driver
	connector driver.Connector
	tracer    opentracing.Tracer
}

// WrapConnector allows wrapping a database driver.Connector
func WrapConnector(dc driver.Connector, tracer opentracing.Tracer) driver.Connector {
	return &otDriver{
		parent:    dc.Driver(),
		connector: dc,
		tracer:    tracer,
	}
}

func (d otDriver) Connect(ctx context.Context) (driver.Conn, error) {
	c, err := d.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &otConn{parent: c, tracer: d.tracer}, nil
}

func (d otDriver) Driver() driver.Driver {
	return d.parent
}

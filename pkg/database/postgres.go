package database

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type PostgresDB struct {
	Host     string
	Port     string
	Database string
	Username string
	Password string
	Query    string
	conn     *sqlx.DB
}

func (p *PostgresDB) Init() error {
	var err error
	URI := fmt.Sprintf(
		"user=%s password=%s host=%s port=%s dbname=%s %s",
		p.Username,
		p.Password,
		p.Host,
		p.Port,
		p.Database,
		p.Query,
	)
	p.conn, err = sqlx.Connect("postgres", URI)
	if err != nil {
		return err
	}
	p.conn.SetMaxOpenConns(4)
	return nil
}

func (p *PostgresDB) Conn() *sqlx.DB {
	return p.conn
}

package db

import (
	"errors"

	"github.com/qiniu/log.v1"
	"gopkg.in/mgo.v2"
)

type Conn struct {
	session *mgo.Session
	db      *mgo.Database
}

func (conn *Conn) C(name string) *mgo.Collection {
	return conn.db.C(name)
}
func (conn *Conn) Close() {
	conn.session.Close()
}

type MongoDB struct {
	Address  string
	Database string
}

func (m *MongoDB) Init() error {
	if m.Address == "" {
		return errors.New("mongodb address should not be empty")
	}
	_, err := mgo.Dial(m.Address)
	if err != nil {
		return err
	}
	return nil
}

func (m *MongoDB) NewConn() *Conn {
	session, err := mgo.Dial(m.Address)
	if err != nil {
		log.Errorf("new connection error:%v", err)
	}
	db := session.DB(m.Database)
	return &Conn{session, db}
}

// Copyright 2013 Beego Authors
// Copyright 2014 The Macaron Authors
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package session

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	//_ "github.com/gocql/gocql"

	"github.com/go-macaron/session"
)

// CassandraStore represents a cassandra session store implementation.
type CassandraStore struct {
	Session *gocql.Session
	Cluster *gocql.ClusterConfig
	sid     string
	lock    sync.RWMutex
	data    map[interface{}]interface{}
}

// NewCassandraStore creates and returns a cassandra session store.
func NewCassandraStore(Session *gocql.Session, Cluster *gocql.ClusterConfig, sid string, kv map[interface{}]interface{}) *CassandraStore {
	return &CassandraStore{
		Session: Session,
		Cluster: Cluster,
		sid:     sid,
		data:    kv,
	}
}

// Set sets value to given key in session.
func (s *CassandraStore) Set(key, val interface{}) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.data[key] = val
	return nil
}

// Get gets value by given key in session.
func (s *CassandraStore) Get(key interface{}) interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.data[key]
}

// Delete delete a key from session.
func (s *CassandraStore) Delete(key interface{}) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.data, key)
	return nil
}

// ID returns current session ID.
func (s *CassandraStore) ID() string {
	return s.sid
}

// Release releases resource and save data to provider.
func (s *CassandraStore) Release() error {

	data, err := session.EncodeGob(s.data)

	if err != nil {
		return err
	}

	s.Cluster.SerialConsistency = gocql.Serial

	if err := s.Session.Query("UPDATE session SET data=? WHERE key=?", data, s.sid).Exec(); err != nil {

		return err
	}

	return nil
}

// Flush deletes all session data.
func (s *CassandraStore) Flush() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.data = make(map[interface{}]interface{})
	return nil
}

// CassandraProvider represents a cassandra session provider implementation.
type CassandraProvider struct {
	Session *gocql.Session
	Cluster *gocql.ClusterConfig
	expire  int64
}

// Init initializes cassandra session provider.
// connStr: username:password@protocol(address)/dbname?param=value
func (p *CassandraProvider) Init(expire int64, connStr string) (err error) {

	p.expire = expire

	// connect to the cluster
	configs := strings.Split(connStr, ":")

	cluster := gocql.NewCluster(configs[0])
	cluster.Keyspace = configs[1]
	cluster.Timeout = 200 * time.Second
	session, _ := cluster.CreateSession()
	//defer session.Close()

	p.Session = session
	p.Cluster = cluster

	return nil
}

// Read returns raw session store by session ID.
func (p *CassandraProvider) Read(sid string) (session.RawStore, error) {
	var data []byte
	var count int

	err := p.Session.Query("SELECT count(*) as count,data FROM session WHERE key=? limit 1", sid).Scan(&count, &data)

	if count == 0 {

		p.Session.Query("INSERT INTO session(key,data) VALUES(?,?) USING TTL ?", sid, "", p.expire).Exec()

	}
	if err != nil {
		return nil, err
	}

	var kv map[interface{}]interface{}
	if len(data) == 0 {
		kv = make(map[interface{}]interface{})
	} else {
		kv, err = session.DecodeGob(data)
		if err != nil {
			return nil, err
		}
	}

	return NewCassandraStore(p.Session, p.Cluster, sid, kv), nil
}

// Exist returns true if session with given ID exists.
func (p *CassandraProvider) Exist(sid string) bool {
	var data []byte

	err := p.Session.Query("SELECT data FROM session WHERE key=? limit 1", sid).Scan(&data)

	if err != nil && err != sql.ErrNoRows {

		panic("session/cassandra: error checking existence: " + err.Error())

	}
	return err != sql.ErrNoRows
}

// Destory deletes a session by session ID.
func (p *CassandraProvider) Destory(sid string) error {
	if err := p.Session.Query("DELETE FROM session WHERE key=?", sid).Exec(); err != nil {
		return err
	}
	return nil
}

// Regenerate regenerates a session store from old session ID to new one.
func (p *CassandraProvider) Regenerate(oldsid, sid string) (_ session.RawStore, err error) {
	if p.Exist(sid) {
		return nil, fmt.Errorf("new sid '%s' already exists", sid)
	}

	if !p.Exist(oldsid) {
		if err := p.Session.Query("INSERT INTO session(key,data) VALUES(?,?) USING TTL ?",
			oldsid, "", p.expire).Exec(); err != nil {
			return nil, err
		}
	}

	if err := p.Session.Query("UPDATE session SET key=? WHERE key=? USING TTL ?", sid, oldsid, p.expire).Exec(); err != nil {
		return nil, err
	}

	return p.Read(sid)
}

// Count counts and returns number of sessions.
func (p *CassandraProvider) Count() (total int) {
	if err := p.Session.Query("SELECT COUNT(*) AS NUM FROM session limit 1").Scan(&total); err != nil {
		panic("session/cassandra: error counting records: " + err.Error())
	}
	return total
}

// GC calls GC to clean expired sessions.
func (p *CassandraProvider) GC() {

	// setup ttl, so cassandra will auto GC
}

func init() {
	session.Register("cassandra", &CassandraProvider{})
}

// Copyright 2018 Dirk Lembke

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raftconsul

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/raft"
)

// ConsulStore is a Raft LogStore and StableStore implementation
type ConsulStore struct {
	kv     *api.KV
	prefix string
}

// Config contains the store configuration
type Config struct {
	Address string
	Timeout time.Duration
	Prefix  string

	HTTPBasicAuth *api.HttpBasicAuth
	TLSConfig     *api.TLSConfig
}

// NewConsulStore creates a new store with the given configuration
func NewConsulStore(config Config) (*ConsulStore, error) {

	c := api.DefaultConfig()

	if config.Address != "" {
		c.Address = config.Address
	}

	if config.Timeout != 0 {
		c.WaitTime = config.Timeout
	}

	if config.HTTPBasicAuth != nil {
		c.HttpAuth = config.HTTPBasicAuth
	}

	if config.TLSConfig != nil {
		c.TLSConfig = *config.TLSConfig
	}

	client, err := api.NewClient(c)
	if err != nil {
		return nil, err
	}

	config.Prefix = strings.Trim(config.Prefix, " \t")

	if config.Prefix == "" {
		config.Prefix = "raft"
	}

	if !strings.HasSuffix(config.Prefix, "/") {
		config.Prefix = config.Prefix + "/"
	}

	if strings.HasPrefix(config.Prefix, "/") {
		config.Prefix = config.Prefix[1:]
	}

	store := &ConsulStore{
		kv:     client.KV(),
		prefix: config.Prefix,
	}

	return store, nil
}

// Clear deletes all existing store entries
func (s *ConsulStore) Clear(prefix string) error {

	if prefix == "" {
		prefix = s.prefix
	}

	_, err := s.kv.DeleteTree(prefix, nil)

	return err
}

// Raft StableStore implementation
// StableStore is used to provide stable storage
// of key configurations to ensure safety.

// Set a key-value-pair into store
func (s *ConsulStore) Set(key []byte, v []byte) error {
	k := string(key)
	return s.set(k, v, "")
}

// Get returns the value for key, or an empty byte slice if key was not found.
func (s *ConsulStore) Get(key []byte) ([]byte, error) {
	k := string(key)
	return s.get(k, "")
}

// SetUint64 is like Set, but handles uint64 values
func (s *ConsulStore) SetUint64(key []byte, val uint64) error {
	return s.Set(key, []byte(strconv.FormatUint(val, 10)))
}

// GetUint64 returns the uint64 value for key, or 0 if key was not found.
func (s *ConsulStore) GetUint64(key []byte) (uint64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}

	if val == nil || len(val) == 0 {
		return 0, nil
	}

	return strconv.ParseUint(string(val), 10, 64)
}

// Raft LogStore implementation
// LogStore is used to provide an interface for storing
// and retrieving logs in a durable fashion.

// FirstIndex returns the first index written. 0 for no entries.
func (s *ConsulStore) FirstIndex() (uint64, error) {
	keys, _, err := s.kv.Keys(s.prefix+"logs/", "", nil)
	if err != nil {
		return 0, err
	}

	if len(keys) == 0 {
		return 0, nil
	}

	sort.Strings(keys)

	// return first valid index
	for _, k := range keys {
		index, err := getIndex(k)
		if err == nil {
			return index, nil
		}
	}

	return 0, nil
}

// LastIndex returns the last index written. 0 for no entries.
func (s *ConsulStore) LastIndex() (uint64, error) {
	keys, _, err := s.kv.Keys(s.prefix+"logs/", "", nil)
	if err != nil {
		return 0, err
	}

	if len(keys) == 0 {
		return 0, nil
	}

	sort.Strings(keys)

	// return last valid index
	for i := len(keys) - 1; i >= 0; i-- {
		index, err := getIndex(keys[i])
		if err == nil {
			return index, nil
		}
	}

	return 0, nil
}

// GetLog gets a raft log entry at a given index.
func (s *ConsulStore) GetLog(idx uint64, log *raft.Log) error {

	val, err := s.get(getKey(idx, "logs/"), "")

	if err != nil {
		return err
	}

	if val == nil {
		return fmt.Errorf("Log entry %d not found", idx)
	}

	return decode(val, log)
}

// StoreLog stores a raft log entry.
func (s *ConsulStore) StoreLog(log *raft.Log) error {
	key := getKey(log.Index, "logs/")
	val, err := encode(log)
	if err != nil {
		return err
	}

	return s.set(key, val, "")
}

// StoreLogs stores multiple raft log entries.
func (s *ConsulStore) StoreLogs(logs []*raft.Log) error {

	chunks := split(logs, 64)

	for _, chunk := range chunks {
		l := len(chunk)
		ops := make(api.KVTxnOps, l, l)

		for i, log := range chunk {

			if log == nil {
				continue
			}

			val, err := encode(log)
			if err != nil {
				return err
			}

			op := new(api.KVTxnOp)
			op.Verb = api.KVSet
			op.Key = getKey(log.Index, s.prefix+"logs/")
			op.Value = val

			ops[i] = op
		}

		_, _, _, err := s.kv.Txn(ops, nil)

		if err != nil {
			return err
		}
	}

	return nil
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (s *ConsulStore) DeleteRange(min, max uint64) error {

	ops := make(api.KVTxnOps, 0, 32)
	keys, _, err := s.kv.Keys(s.prefix+"logs/", "", nil)

	if err != nil {
		return err
	}

	for _, k := range keys {

		index, err := getIndex(k)

		if err != nil {
			continue
		}

		if index >= min && index <= max {
			op := new(api.KVTxnOp)
			op.Verb = api.KVDelete
			op.Key = k

			ops = append(ops, op)
		}
	}

	_, _, _, err = s.kv.Txn(ops, nil)

	return err
}

// private functions
func (s *ConsulStore) set(key string, v []byte, prefix string) error {

	if prefix == "" {
		prefix = s.prefix
	}

	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	if strings.HasPrefix(prefix, "/") {
		prefix = prefix[1:]
	}

	p := &api.KVPair{Key: prefix + key, Value: v}
	_, err := s.kv.Put(p, nil)
	return err
}

func (s *ConsulStore) get(key string, prefix string) ([]byte, error) {
	if prefix == "" {
		prefix = s.prefix
	}

	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	if strings.HasPrefix(prefix, "/") {
		prefix = prefix[1:]
	}

	pair, _, err := s.kv.Get(prefix+key, nil)
	if err != nil {
		return nil, err
	}

	if pair == nil {
		return []byte{}, nil
	}

	return pair.Value, nil
}

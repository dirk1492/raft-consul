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
	"reflect"
	"strconv"
	"strings"

	"github.com/hashicorp/consul/api"
)

var testData = []byte("alea iacta est")

var uint64TestValue = uint64(1234567890)
var uint64TestData = []byte("1234567890")

var byteKey = string([]byte{1, 2, 3, 4})

var testMessages = map[uint64][]byte{
	3464425: []byte("{\"Index\":3464425,\"Term\":36745184581,\"Type\":0,\"Data\":\"AQIDBAUGBwgJAA==\"}"),
	3464435: []byte("{\"Index\":3464428,\"Term\":36745184581,\"Type\":0,\"Data\":\"AQIDBAUGBwgJAA==\"}"),
	3464445: []byte("{\"Index\":3464445,\"Term\":36745184581,\"Type\":0,\"Data\":\"AQIDBAUGBwgJAA==\"}"),
	3464487: []byte("{\"Index\":3464487,\"Term\":36745184581,\"Type\":0,\"Data\":\"AQIDBAUGBwgJAA==\"}"),
	3464421: []byte("{\"Index\":3464421,\"Term\":36745184581,\"Type\":0,\"Data\":\"AQIDBAUGBwgJAA==\"}"),
}

var firstIndex = uint64(3464421)
var lastIndex = uint64(3464487)

func clear(address string) error {
	c := api.DefaultConfig()

	if address != "" {
		c.Address = address
	}

	client, err := api.NewClient(c)
	if err != nil {
		return err
	}

	kv := client.KV()

	_, err = kv.DeleteTree("", nil)

	return err
}

func setup(address string) error {

	err := clear(address)

	if err != nil {
		return err
	}

	err = put(address, "raft/dataset1", testData)
	if err != nil {
		return err
	}

	err = put(address, "raft/test/dataset1", testData)
	if err != nil {
		return err
	}

	err = put(address, "custom/dataset1", testData)
	if err != nil {
		return err
	}

	err = put(address, "custom/test/dataset1", testData)
	if err != nil {
		return err
	}

	err = put(address, "raft/uint64", uint64TestData)
	if err != nil {
		return err
	}

	err = put(address, "raft/byte_"+byteKey, uint64TestData)
	if err != nil {
		return err
	}

	err = put(address, "raft/test/uint64", uint64TestData)
	if err != nil {
		return err
	}

	err = put(address, "custom/uint64", uint64TestData)
	if err != nil {
		return err
	}

	err = put(address, "raft/abc", []byte("abc"))
	if err != nil {
		return err
	}

	err = put(address, "custom/test/uint64", uint64TestData)
	if err != nil {
		return err
	}

	for key, value := range testMessages {
		err = put(address, fmt.Sprintf("%v%020d", "raft/logs/", key), value)
		if err != nil {
			return err
		}
	}

	err = put(address, "raft/logs/error", uint64TestData)
	if err != nil {
		return err
	}

	return nil
}

func put(address string, key string, value []byte) error {
	return set(address, "", key, value)
}

func set(address string, prefix string, key string, value []byte) error {
	c := api.DefaultConfig()

	if address != "" {
		c.Address = address
	}

	client, err := api.NewClient(c)
	if err != nil {
		return err
	}

	kv := client.KV()

	if prefix != "" {
		prefix := strings.Trim(prefix, " \t")

		if !strings.HasSuffix(prefix, "/") {
			prefix = prefix + "/"
		}

		if strings.HasPrefix(prefix, "/") {
			prefix = prefix[1:]
		}

		key = prefix + key
	}

	p := &api.KVPair{Key: key, Value: value}
	_, err = kv.Put(p, nil)

	return err
}

func list(address string, prefix string) (Uint64Array, error) {
	c := api.DefaultConfig()

	if address != "" {
		c.Address = address
	}

	client, err := api.NewClient(c)
	if err != nil {
		return nil, err
	}

	kv := client.KV()

	pairs, _, err := kv.List(prefix, nil)

	rc := []uint64{}
	for _, p := range pairs {
		key := p.Key
		pos := strings.LastIndexByte(key, '/')

		if pos != -1 {
			key = key[pos+1:]
		}

		val, err := strconv.ParseUint(key, 10, 64)
		if err == nil {
			rc = append(rc, val)
		}
	}

	return rc, err
}

func check(address string, prefix string, key string, value []byte) error {

	c := api.DefaultConfig()

	if address != "" {
		c.Address = address
	}

	client, err := api.NewClient(c)
	if err != nil {
		return err
	}

	kv := client.KV()

	if prefix != "" {
		prefix := strings.Trim(prefix, " \t")

		if !strings.HasSuffix(prefix, "/") {
			prefix = prefix + "/"
		}

		if strings.HasPrefix(prefix, "/") {
			prefix = prefix[1:]
		}

		key = prefix + key
	} else {
		key = "raft/" + key
	}

	pair, _, err := kv.Get(key, nil)

	if err != nil {
		return err
	}

	if pair == nil {
		return fmt.Errorf("key '%v' not found", key)
	}

	if pair.Key == key && !reflect.DeepEqual(pair.Value, value) {
		return fmt.Errorf("expected '%v'=%v, got '%v'=%v", key, string(value), pair.Key, string(pair.Value))
	}

	return nil
}

func isEqual(v1 []byte, v2 []byte) bool {
	return reflect.DeepEqual(v1, v2)
}

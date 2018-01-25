raft-consul
===========

This repository provides the `raftconsul` package. The package exports the
`ConsulStore` which is an implementation of both a `LogStore` and `StableStore`.

It is meant to be used as a backend for the `raft` [package
here](https://github.com/hashicorp/raft).

This implementation uses [Consul](https://github.com/hashicorp/consul). Consul is a tool for service discovery and configuration. Consul is distributed, highly available, and extremely scalable.

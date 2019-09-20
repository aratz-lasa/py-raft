# raft_asyncio
[![Build Status](https://travis-ci.com/aratz-lasa/raft_asyncio.svg?token=14vGnmnCyxosg26uva6k&branch=master)](https://travis-ci.com/aratz-lasa/raft_asyncio) 
[![codecov](https://codecov.io/gh/aratz-lasa/raft/branch/master/graph/badge.svg?token=2lheZbjYK7)](https://codecov.io/gh/aratz-lasa/raft)
[![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Python **asyncio RAFT (Flexible Paxos)** consensus algorithm implementation.

## What is Raft?
Raft is a consensus algorithm that is designed to be easy to understand. It's equivalent to Paxos in fault-tolerance and performance.

## What is Flexible Paxos?
Flexible Paxos is the simple observation that it is not necessary to require all quorums in Paxos to intersect. It is sufficient to require that the quorum used by the leader election phase will overlap with the quorums used by previous replication phases.

## Examples
*TODO*

# References
- [RAFT web page](https://raft.github.io/)
- [Flexible Paxos web page](https://fpaxos.github.io/)

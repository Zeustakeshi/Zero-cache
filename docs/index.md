# ZeroCache Documentation

Welcome to the ZeroCache documentation.

## Overview

ZeroCache is a Redis-like in-memory cache engine for Python with **zero external dependencies**.
It provides familiar Redis data structures — strings, hashes, lists, sets, and sorted sets —
with async support, pipeline batching, pub/sub, and crash-safe persistence.

## Contents

- [Quickstart](quickstart.md) — Get up and running in 2 minutes
- [API Reference](api-reference.md) — Full method documentation
- [Architecture](architecture.md) — Internals explained
- [Contributing](contributing.md) — How to contribute

## Installation

```bash
pip install zero-cache
```

## Why ZeroCache?

| | ZeroCache | Redis + redis-py |
|--|-----------|-----------------|
| Dependencies | 0 | Redis server + redis-py |
| Setup | `pip install zero-cache` | Install Redis daemon |
| Latency | In-process (nanoseconds) | Network (microseconds+) |
| Use case | Single-process apps | Multi-process / distributed |
| Persistence | Optional file snapshot | Native RDB/AOF |

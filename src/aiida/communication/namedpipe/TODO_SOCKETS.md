# TODO: Refactor to Unix Domain Sockets

## Problem

Named pipes have issues with multiple concurrent writers:
- Multiple clients submitting tasks simultaneously → messages interleave and corrupt
- Multiple workers writing to broker broadcast pipe → same corruption issue
- Non-blocking I/O causes partial read issues

## Current Architecture (Named Pipes)

```
Clients  ──write──►  [Broker Task Pipe]       ──read──►  Broker  (PROBLEM: multiple writers)
Workers  ──write──►  [Broker Broadcast Pipe]  ──read──►  Broker  (PROBLEM: multiple writers)
Broker   ──write──►  [Worker Task Pipes]      ──read──►  Workers (OK: single writer)
Broker   ──write──►  [Worker Broadcast Pipes] ──read──►  Workers (OK: single writer)
```

## Proposed Architecture (Unix Domain Sockets)

```
                    ┌─────────────────┐
Clients  ──connect──►                 │
                    │  Broker Socket  │──► Broker handles each connection separately
Workers  ──connect──►                 │
                    └─────────────────┘
```

- Single listening socket on broker
- Each client/worker gets its own connection (no message interleaving)
- Bidirectional communication on each connection
- Use SOCK_STREAM or SOCK_SEQPACKET for message boundaries

## Implementation Plan

### 1. Create `SocketBrokerCommunicator`
- Listen on Unix domain socket
- Accept connections from clients and workers
- Use select/poll to handle multiple connections
- Each connection has its own receive buffer

### 2. Create `SocketWorkerCommunicator`
- Connect to broker socket on startup
- Maintain persistent connection
- Bidirectional: receive tasks, send replies/broadcasts

### 3. Create `SocketClientCommunicator`
- For task submission from `launch.py`
- Connect, send task, optionally wait for reply, disconnect

### 4. Message Protocol
- Length-prefixed messages (4-byte length + JSON payload)
- Or use SOCK_SEQPACKET for automatic message boundaries
- Message types: TASK, BROADCAST, RPC_REQUEST, RPC_RESPONSE

### 5. Files to Modify
- `src/aiida/communication/namedpipe/` → rename to `src/aiida/communication/socket/`
- `broker_communicator.py` → socket-based implementation
- `communicator.py` → socket-based implementation
- `broker.py` (PipeBroker) → update to use socket communicator

## Benefits
- No message corruption from concurrent writers
- Clean disconnect detection
- Simpler partial-read handling
- Bidirectional on single connection
- Better error handling

## Temporary Fix (Current)
For now, the named pipe implementation may work for single-client scenarios.
For production use with multiple concurrent submissions, socket refactoring is needed.

# Named Pipe Broker for AiiDA

This module implements a named pipe-based message broker for AiiDA, providing an alternative to RabbitMQ for local inter-process communication.

## Architecture

The named pipe broker consists of three main components:

1. **PipeCommunicator** - Implements the `kiwipy.Communicator` interface using named pipes
2. **PipeCoordinator** - Central process that manages task distribution and broadcast fanout
3. **PipeBroker** - Wrapper that integrates with AiiDA's broker system

## Features

- ✅ Full implementation of kiwipy Communicator interface
- ✅ RPC (Remote Procedure Call) support
- ✅ Task queue with persistence and crash recovery
- ✅ Broadcast/pub-sub messaging
- ✅ Event-driven I/O using selectors
- ✅ Worker discovery and management
- ✅ No external dependencies (no RabbitMQ needed)

## Usage

### Starting the Coordinator

The coordinator must be running before workers can connect:

```bash
# Start coordinator in background
verdi coordinator start

# Start coordinator in foreground (for debugging)
verdi coordinator start --foreground

# Check coordinator status
verdi coordinator status

# Stop coordinator
verdi coordinator stop

# Restart coordinator
verdi coordinator restart
```

### Using with AiiDA Profile

Configure your AiiDA profile to use the named pipe broker:

```python
from aiida.manage.configuration import get_config, Profile

config = get_config()
profile = config.get_profile()

# The broker will automatically use PipeBroker if configured
# or can be explicitly set in profile configuration
```

### Example: Worker with Task Subscriber

```python
from aiida.brokers.namedpipe import PipeCommunicator
from pathlib import Path

# Create communicator
communicator = PipeCommunicator(
    profile_name='my_profile',
    config_path=Path.home() / '.aiida' / 'profiles' / 'my_profile'
)

# Add task subscriber
def my_task_handler(communicator, task):
    print(f"Received task: {task}")
    # Process task
    return {"result": "success"}

communicator.add_task_subscriber(my_task_handler)

# Keep worker running
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    communicator.close()
```

### Example: Sending Tasks

```python
from aiida.brokers.namedpipe import PipeCommunicator

communicator = PipeCommunicator(
    profile_name='my_profile',
    config_path=Path.home() / '.aiida' / 'profiles' / 'my_profile'
)

# Send a task
future = communicator.task_send({'command': 'process', 'data': 123})

# Wait for result
result = future.result(timeout=10)
print(f"Task result: {result}")

communicator.close()
```

### Example: Broadcast Messages

```python
from aiida.brokers.namedpipe import PipeCommunicator

# Subscriber
communicator = PipeCommunicator(...)

def broadcast_handler(communicator, body, sender, subject, correlation_id):
    print(f"Broadcast from {sender}: {body}")

communicator.add_broadcast_subscriber(broadcast_handler)

# Publisher
communicator.broadcast_send(
    body={'message': 'Hello, workers!'},
    sender='main',
    subject='announcement'
)
```

### Example: RPC Calls

```python
from aiida.brokers.namedpipe import PipeCommunicator

# RPC Server
server_comm = PipeCommunicator(
    profile_name='my_profile',
    config_path=config_path,
    worker_id='rpc_server'
)

def rpc_handler(communicator, msg):
    print(f"RPC call: {msg}")
    return {"response": "processed"}

server_comm.add_rpc_subscriber(rpc_handler, identifier='rpc_server')

# RPC Client
client_comm = PipeCommunicator(...)
future = client_comm.rpc_send('rpc_server', {'request': 'data'})
result = future.result(timeout=5)
```

## File Locations

### Named Pipes
Located in `/tmp/aiida_{profile}_pipes/`:
- `coordinator_tasks` - Coordinator's task intake pipe
- `coordinator_broadcast` - Coordinator's broadcast intake pipe
- `task_{worker_id}` - Worker's task pipe
- `rpc_{worker_id}` - Worker's RPC pipe
- `broadcast_{worker_id}` - Worker's broadcast pipe
- `reply_{pid}_{uuid}` - Worker's reply pipe

### Discovery Files
Located in `~/.aiida/profiles/{profile}/pipes/`:
- `coordinator.json` - Coordinator registration
- `worker_{id}.json` - Worker registrations

### Task Queue
Located in `~/.aiida/profiles/{profile}/coordinator/`:
- `task_*.json` - Persisted task files

## Implementation Details

### Message Framing
Messages use length-prefixed framing:
```
[4 bytes: length (big-endian)] [message data (JSON)]
```

### Request-Response Pattern
1. Client creates unique reply pipe
2. Client sends message with correlation ID and reply pipe path
3. Client's selector monitors reply pipe
4. Server processes request and writes to reply pipe
5. Client's selector triggers, reads response, matches correlation ID
6. Future is resolved with result

### Task Distribution
- Coordinator maintains persistent queue on disk
- Tasks distributed to workers using round-robin selection
- Crash recovery: orphaned tasks reassigned on coordinator restart

### Broadcast Fanout
- Multi-unicast implementation
- Coordinator writes to each worker's broadcast pipe
- Non-blocking writes to prevent one slow worker from blocking others

## Limitations

- **Platform**: POSIX systems only (Linux, macOS)
- **Scope**: Local processes only (no network communication)
- **Scalability**: Designed for ~100 workers max
- **Named pipes**: Different semantics than network sockets

## Performance Considerations

- Very low latency for local communication
- No network overhead
- Efficient selector-based I/O
- File descriptor limits: ensure `ulimit -n` is sufficient (each worker needs ~4 FDs)

## Troubleshooting

### Coordinator not starting
- Check if coordinator is already running: `verdi coordinator status`
- Check log file: `~/.aiida/profiles/{profile}/coordinator/coordinator.log`
- Verify permissions on `/tmp/aiida_{profile}_pipes/`

### Workers not receiving tasks
- Verify coordinator is running
- Check worker is registered: look for worker JSON in pipes discovery directory
- Verify worker's task subscriber is properly added

### Stale pipes
Clean up manually if needed:
```bash
rm -rf /tmp/aiida_{profile}_pipes/
rm -rf ~/.aiida/profiles/{profile}/pipes/
```

### Permission errors
Ensure pipes directory has correct permissions:
```bash
chmod 700 /tmp/aiida_{profile}_pipes/
```

## Maintenance

The coordinator automatically performs maintenance every 10 seconds:
- Cleans up old completed tasks (>24 hours)
- Removes dead worker entries
- Requeues tasks from dead workers

Manual cleanup:
```python
from aiida.brokers.namedpipe import discovery

config_path = Path.home() / '.aiida' / 'profiles' / 'my_profile'
count = discovery.cleanup_dead_processes(config_path)
print(f"Cleaned up {count} dead processes")
```

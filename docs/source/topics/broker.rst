.. _topics:broker:

******
Broker
******

AiiDA uses RabbitMQ as a message broker to coordinate process execution between the daemon workers.
This section explains the queue architecture and how processes are routed through the system.

For a quick reference on managing queues, see :ref:`how-to:queues`.

.. _topics:broker:overview:

========
Overview
========

When you submit a process using :func:`~aiida.engine.launch.submit`, AiiDA:

1. Persists the process state to the database
2. Sends a message to RabbitMQ with the process ID
3. Returns control to your script immediately

A daemon worker then:

1. Receives the message from RabbitMQ
2. Loads the process from the database
3. Executes it to completion (or until it waits for a sub-process)

.. _topics:broker:queues:

===========
User Queues
===========

A **user queue** is a named group of RabbitMQ queues that you can use to organize your work.
For example, you might have:

- ``default`` - for general work
- ``hpc-gpu`` - for GPU cluster jobs with specific concurrency limits
- ``hpc-cpu`` - for CPU cluster jobs with different limits

Each user queue is configured with two concurrency limits:

- **root_workchain_prefetch**: Maximum concurrent root-level WorkChains per worker
- **calcjob_prefetch**: Maximum concurrent CalcJobs per worker

These limits are configured via the CLI:

.. code-block:: console

    $ verdi broker queue create hpc-gpu 50 100

Or via the Python API:

.. code-block:: python

    from aiida import get_profile
    from aiida.manage.configuration import get_config

    profile = get_profile()
    queue_config = profile.get_queue_config()
    queue_config['hpc-gpu'] = {
        'root_workchain_prefetch': 50,
        'calcjob_prefetch': 100,
    }
    profile.set_queue_config(queue_config)

    config = get_config()
    config.update_profile(profile)
    config.store()

.. _topics:broker:architecture:

=====================
Queue Architecture
=====================

Each user queue maps to **three** underlying RabbitMQ queues:

.. list-table::
   :header-rows: 1
   :widths: 25 25 50

   * - Queue Type
     - Prefetch Limit
     - Purpose
   * - ``root-workchain``
     - Configurable (default: 200)
     - Top-level WorkChains submitted via ``submit()``
   * - ``nested-workchain``
     - Unlimited
     - WorkChains submitted from within another WorkChain
   * - ``calcjob``
     - Configurable (default: UNLIMITED)
     - All CalcJob submissions

This three-queue design prevents **deadlocks** that can occur with nested workflows.

Deadlock Prevention
-------------------

Consider a WorkChain that submits child WorkChains, which in turn submit CalcJobs:

.. code-block:: text

    RootWorkChain
    ├── ChildWorkChain1
    │   ├── CalcJob1
    │   └── CalcJob2
    └── ChildWorkChain2
        ├── CalcJob3
        └── CalcJob4

If all processes shared a single queue with a prefetch limit of 4:

1. 4 RootWorkChains could fill the queue
2. They submit ChildWorkChains, but no slots are available
3. The RootWorkChains wait forever → **deadlock**

With separate queues:

1. Root WorkChains consume from the limited ``root-workchain`` queue
2. Nested WorkChains go to the unlimited ``nested-workchain`` queue
3. CalcJobs go to the ``calcjob`` queue (optionally limited)

This ensures that:

- Root WorkChains can always make progress by spawning children
- Nested WorkChains can always run (unlimited queue)
- CalcJobs are rate-limited to prevent overwhelming remote resources

.. _topics:broker:prefetch:

================
Prefetch Limits
================

Prefetch limits control how many messages a daemon worker will fetch from RabbitMQ at once.

.. important::

    Prefetch limits are **per daemon worker**, not global limits.

    With ``N`` workers and a prefetch of ``P``, the effective limit is ``N × P``.

For example, with 4 daemon workers and ``root_workchain_prefetch=50``:

- Each worker can run up to 50 root WorkChains concurrently
- The system can run up to 200 root WorkChains total (4 × 50)

Setting limits
--------------

.. code-block:: console

    # Set root WorkChain limit to 100 per worker
    $ verdi broker queue set default root-workchain 100

    # Set CalcJob limit to 50 per worker
    $ verdi broker queue set default calcjob 50

    # Set CalcJob to unlimited
    $ verdi broker queue set default calcjob UNLIMITED

Choosing limits
---------------

- **root_workchain_prefetch**: Set based on memory usage per WorkChain and available RAM
- **calcjob_prefetch**: Set based on how many concurrent remote jobs you want to manage

Use ``UNLIMITED`` to allow the worker to consume as many messages as available.
Use ``0`` to block all new processes of that type on the queue (useful for maintenance).

.. _topics:broker:routing:

=============
Queue Routing
=============

When a process is submitted, AiiDA determines which RabbitMQ queue to use based on:

1. **The user queue**: Specified via ``submit(..., queue='hpc-gpu')`` or inherited from the parent process
2. **The queue type**: Determined by the process type and submission context

Routing rules
-------------

.. list-table::
   :header-rows: 1
   :widths: 40 30 30

   * - Submission Context
     - Process Type
     - Queue Type
   * - Top-level ``submit()``
     - WorkChain
     - ``root-workchain``
   * - Top-level ``submit()``
     - CalcJob
     - ``calcjob``
   * - From within a WorkChain (``self.submit()``)
     - WorkChain
     - ``nested-workchain``
   * - From within a WorkChain (``self.submit()``)
     - CalcJob
     - ``calcjob``

Queue inheritance
-----------------

When a process submits child processes, the children inherit the parent's user queue.
This ensures that an entire workflow tree stays within the same user queue:

.. code-block:: python

    # Parent submitted to 'hpc-gpu' queue
    node = submit(ParentWorkChain, inputs, queue='hpc-gpu')

    # Inside ParentWorkChain:
    def run_child(self):
        # This child automatically goes to 'hpc-gpu' queue
        return ToContext(child=self.submit(ChildWorkChain, **inputs))

The queue name is stored on the process node and can be inspected:

.. code-block:: python

    from aiida.orm import ProcessNode

    node = load_node(pk)
    queue_name = node.base.attributes.get(ProcessNode.QUEUE_NAME_KEY)

.. _topics:broker:moving:

=================
Moving Processes
=================

You can move processes between queues while the daemon is running:

.. code-block:: console

    $ verdi process set-queue 123 456 --queue hpc-gpu

This command:

1. Consumes the existing task from RabbitMQ (atomically, before the daemon can process it)
2. Updates the queue attribute on the process node
3. Re-submits the task to the new queue

.. note::

    If a process task has already been consumed by a daemon worker, it cannot be moved.
    The command will warn you and skip those processes.

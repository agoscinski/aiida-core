.. _how-to:queues:

****************************
How to use process queues
****************************

AiiDA supports multiple process queues to organize and prioritize work submitted to the daemon.
Each queue can have its own concurrency limits for WorkChains and CalcJobs.

For a detailed explanation of how the queue system works internally, see :ref:`topics:broker`.

The default queue
=================

A queue named ``default`` is automatically created when you first access the queue configuration.
All processes are submitted to this queue unless you specify otherwise.

Creating additional queues
==========================

You can create additional queues to organize work for different resources or use cases.
The daemon must be stopped when creating queues:

.. code-block:: console

    $ verdi daemon stop
    $ verdi broker queue create hpc-gpu 50 100

This creates a queue named ``hpc-gpu`` with:

- Maximum 50 concurrent root WorkChains per worker
- Maximum 100 concurrent CalcJobs per worker

Use ``UNLIMITED`` for unlimited concurrency:

.. code-block:: console

    $ verdi broker queue create hpc-cpu 10 UNLIMITED

This creates a ``hpc-cpu`` queue with 10 root WorkChains but unlimited CalcJobs per worker.

Use ``0`` to block all new processes of that type (useful for maintenance mode).

Listing queues
==============

To see all configured queues and their limits:

.. code-block:: console

    $ verdi broker queue list
    User Queue      Root WorkChain Limit  CalcJob Limit
    ------------  ----------------------  ---------------
    default                          200  UNLIMITED
    hpc-gpu                           50  100
    hpc-cpu                           10  UNLIMITED

Modifying queue limits
======================

To change the limits of an existing queue:

.. code-block:: console

    $ verdi broker queue set hpc-gpu root-workchain 100
    $ verdi broker queue set hpc-gpu calcjob 50

Submitting to a queue
=====================

By default, processes are submitted to the ``default`` queue.
To submit to a specific queue, use the ``queue`` parameter:

.. code-block:: python

    from aiida.engine import submit
    from aiida.plugins import WorkflowFactory

    MyWorkChain = WorkflowFactory('my_plugin.my_workchain')

    # Submit to the default queue (no queue parameter needed)
    node = submit(MyWorkChain, **inputs)

    # Submit to a specific queue
    node = submit(MyWorkChain, inputs, queue='hpc-gpu')

Moving processes between queues
===============================

To move running processes to a different queue:

.. code-block:: console

    $ verdi process set-queue 123 456 789 --queue hpc-gpu

This consumes the existing tasks from RabbitMQ and re-submits them to the new queue.

.. note::

    The daemon can be running when moving processes. The command atomically
    consumes tasks before the daemon can process them.

Deleting a queue
================

To delete a queue (the ``default`` queue cannot be deleted):

.. code-block:: console

    $ verdi broker queue delete hpc-gpu --force

.. warning::

    If there are processes waiting in the queue, they will be orphaned.
    Use ``verdi process repair --queue <new_queue>`` to reassign them.

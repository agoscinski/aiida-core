# -*- coding: utf-8 -*-
###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
# pylint: disable=no-member,too-many-public-methods,no-self-use
"""Module to test AiiDA processes."""
import threading

import plumpy
from plumpy.utils import AttributesFrozendict
import pytest

from aiida import orm
from aiida.common.lang import override
from aiida.engine import ExitCode, ExitCodesNamespace, Process, run, run_get_node, run_get_pk
from aiida.engine.processes.ports import PortNamespace
from aiida.manage.caching import enable_caching
from aiida.plugins import CalculationFactory
from tests.utils import processes as test_processes


class NameSpacedProcess(Process):
    """Name spaced process."""

    _node_class = orm.WorkflowNode

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input('some.name.space.a', valid_type=orm.Int)


@pytest.mark.requires_rmq
class TestProcessNamespace:
    """Test process namespace"""

    @pytest.fixture(autouse=True)
    def init_profile(self, aiida_profile_clean):  # pylint: disable=unused-argument
        """Initialize the profile."""
        # pylint: disable=attribute-defined-outside-init
        assert Process.current() is None
        yield
        assert Process.current() is None

    def test_namespaced_process(self):
        """Test that inputs in nested namespaces are properly validated and the link labels
        are properly formatted by connecting the namespaces with underscores."""
        proc = NameSpacedProcess(inputs={'some': {'name': {'space': {'a': orm.Int(5)}}}})

        # Test that the namespaced inputs are AttributesFrozenDicts
        assert isinstance(proc.inputs, AttributesFrozendict)
        assert isinstance(proc.inputs.some, AttributesFrozendict)
        assert isinstance(proc.inputs.some.name, AttributesFrozendict)
        assert isinstance(proc.inputs.some.name.space, AttributesFrozendict)

        # Test that the input node is in the inputs of the process
        input_node = proc.inputs.some.name.space.a
        assert isinstance(input_node, orm.Int)
        assert input_node.value == 5

        # Check that the link of the process node has the correct link name
        assert 'some__name__space__a' in proc.node.base.links.get_incoming().all_link_labels()
        assert proc.node.base.links.get_incoming().get_node_by_label('some__name__space__a') == 5
        assert proc.node.inputs.some.name.space.a == 5
        assert proc.node.inputs['some']['name']['space']['a'] == 5


class ProcessStackTest(Process):
    """Test process stack."""

    _node_class = orm.WorkflowNode

    @override
    def run(self):
        pass

    @override
    def on_create(self):
        # pylint: disable=attribute-defined-outside-init
        super().on_create()
        self._thread_id = threading.current_thread().ident

    @override
    def on_stop(self):
        """The therad must match the one used in on_create because process
        stack is using thread local storage to keep track of who called who."""
        super().on_stop()
        assert self._thread_id is threading.current_thread().ident


@pytest.mark.requires_rmq
class TestProcess:
    """Test AiiDA process."""

    @pytest.fixture(autouse=True)
    def init_profile(self, aiida_profile_clean, aiida_localhost):  # pylint: disable=unused-argument
        """Initialize the profile."""
        # pylint: disable=attribute-defined-outside-init
        assert Process.current() is None
        self.computer = aiida_localhost
        yield
        assert Process.current() is None

    @staticmethod
    def test_process_stack():
        run(ProcessStackTest)

    def test_inputs(self):
        with pytest.raises(ValueError):
            run(test_processes.BadOutput)

    def test_spec_metadata_property(self):
        """`Process.spec_metadata` should return the metadata port namespace of its spec."""
        assert isinstance(Process.spec_metadata, PortNamespace)
        assert Process.spec_metadata == Process.spec().inputs['metadata']

    def test_input_link_creation(self):
        """Test input link creation."""
        dummy_inputs = ['a', 'b', 'c', 'd']

        inputs = {string: orm.Str(string) for string in dummy_inputs}
        inputs['metadata'] = {'store_provenance': True}
        process = test_processes.DummyProcess(inputs)

        for entry in process.node.base.links.get_incoming().all():
            assert entry.link_label in inputs
            assert entry.link_label == entry.node.value
            dummy_inputs.remove(entry.link_label)

        # Make sure there are no other inputs
        assert not dummy_inputs

    @staticmethod
    def test_none_input():
        """Check that if we pass no input the process runs fine."""
        run(test_processes.DummyProcess)

    def test_input_after_stored(self):
        """Verify that adding an input link after storing a `ProcessNode` will raise because it is illegal."""
        from aiida.common import LinkType
        process = test_processes.DummyProcess()

        with pytest.raises(ValueError):
            process.node.base.links.add_incoming(orm.Int(1), link_type=LinkType.INPUT_WORK, link_label='illegal_link')

    def test_seal(self):
        _, p_k = run_get_pk(test_processes.DummyProcess)
        assert orm.load_node(pk=p_k).is_sealed

    def test_description(self):
        """Testing setting a process description."""
        dummy_process = test_processes.DummyProcess(inputs={'metadata': {'description': "Rockin' process"}})
        assert dummy_process.node.description == "Rockin' process"

        with pytest.raises(ValueError):
            test_processes.DummyProcess(inputs={'metadata': {'description': 5}})

    def test_label(self):
        """Test setting a label."""
        dummy_process = test_processes.DummyProcess(inputs={'metadata': {'label': 'My label'}})
        assert dummy_process.node.label == 'My label'

        with pytest.raises(ValueError):
            test_processes.DummyProcess(inputs={'label': 5})

    def test_work_calc_finish(self):
        process = test_processes.DummyProcess()
        assert not process.node.is_finished_ok
        run(process)
        assert process.node.is_finished_ok

    @staticmethod
    def test_save_instance_state():
        """Test save instance's state."""
        proc = test_processes.DummyProcess()
        # Save the instance state
        bundle = plumpy.Bundle(proc)
        proc.close()
        bundle.unbundle()

    def test_exit_codes(self):
        """Test the properties to return various (sub) sets of existing exit codes."""
        ArithmeticAddCalculation = CalculationFactory('core.arithmetic.add')  # pylint: disable=invalid-name

        exit_codes = ArithmeticAddCalculation.exit_codes
        assert isinstance(exit_codes, ExitCodesNamespace)
        for _, value in exit_codes.items():
            assert isinstance(value, ExitCode)

        exit_statuses = ArithmeticAddCalculation.get_exit_statuses(['ERROR_NO_RETRIEVED_FOLDER'])
        assert isinstance(exit_statuses, list)
        for entry in exit_statuses:
            assert isinstance(entry, int)

        with pytest.raises(AttributeError):
            ArithmeticAddCalculation.get_exit_statuses(['NON_EXISTING_EXIT_CODE_LABEL'])

    def test_exit_codes_invalidate_cache(self):
        """
        Test that returning an exit code with 'invalidates_cache' set to ``True``
        indeed means that the ProcessNode will not be cached from.
        """
        # Sanity check that caching works when the exit code is not returned.
        with enable_caching():
            _, node1 = run_get_node(test_processes.InvalidateCaching, return_exit_code=orm.Bool(False))
            _, node2 = run_get_node(test_processes.InvalidateCaching, return_exit_code=orm.Bool(False))
            assert node1.base.extras.get('_aiida_hash') == node2.base.extras.get('_aiida_hash')
            assert '_aiida_cached_from' in node2.base.extras

        with enable_caching():
            _, node3 = run_get_node(test_processes.InvalidateCaching, return_exit_code=orm.Bool(True))
            _, node4 = run_get_node(test_processes.InvalidateCaching, return_exit_code=orm.Bool(True))
            assert node3.base.extras.get('_aiida_hash') == node4.base.extras.get('_aiida_hash')
            assert '_aiida_cached_from' not in node4.base.extras

    def test_valid_cache_hook(self):
        """
        Test that the is_valid_cache behavior can be specified from
        the method in the Process sub-class.
        """
        # Sanity check that caching works when the hook returns True.
        with enable_caching():
            _, node1 = run_get_node(test_processes.IsValidCacheHook)
            _, node2 = run_get_node(test_processes.IsValidCacheHook)
            assert node1.base.extras.get('_aiida_hash') == node2.base.extras.get('_aiida_hash')
            assert '_aiida_cached_from' in node2.base.extras

        with enable_caching():
            _, node3 = run_get_node(test_processes.IsValidCacheHook, not_valid_cache=orm.Bool(True))
            _, node4 = run_get_node(test_processes.IsValidCacheHook, not_valid_cache=orm.Bool(True))
            assert node3.base.extras.get('_aiida_hash') == node4.base.extras.get('_aiida_hash')
            assert '_aiida_cached_from' not in node4.base.extras

    def test_process_type_with_entry_point(self):
        """For a process with a registered entry point, the process_type will be its formatted entry point string."""
        from aiida.orm import InstalledCode

        code = InstalledCode(computer=self.computer, filepath_executable='/bin/true').store()
        parameters = orm.Dict(dict={})
        template = orm.Dict(dict={})
        options = {
            'resources': {
                'num_machines': 1,
                'tot_num_mpiprocs': 1
            },
            'max_wallclock_seconds': 1,
        }

        inputs = {
            'code': code,
            'parameters': parameters,
            'template': template,
            'metadata': {
                'options': options,
            }
        }

        entry_point = 'core.templatereplacer'
        process_class = CalculationFactory(entry_point)
        process = process_class(inputs=inputs)

        expected_process_type = f'aiida.calculations:{entry_point}'
        assert process.node.process_type == expected_process_type

        # Verify that process_class on the calculation node returns the original entry point class
        recovered_process = process.node.process_class
        assert recovered_process == process_class

    def test_process_type_without_entry_point(self):
        """
        For a process without a registered entry point, the process_type will fall back on the fully
        qualified class name
        """
        process = test_processes.DummyProcess()
        expected_process_type = f'{process.__class__.__module__}.{process.__class__.__name__}'
        assert process.node.process_type == expected_process_type

        # Verify that process_class on the calculation node returns the original entry point class
        recovered_process = process.node.process_class
        assert recovered_process == process.__class__

    def test_output_dictionary(self):
        """Verify that a dictionary can be passed as an output for a namespace."""

        class TestProcess1(Process):
            """Defining a new TestProcess class for testing."""

            _node_class = orm.WorkflowNode

            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input_namespace('namespace', valid_type=orm.Int, dynamic=True)
                spec.output_namespace('namespace', valid_type=orm.Int, dynamic=True)

            def run(self):
                self.out('namespace', self.inputs.namespace)

        results, node = run_get_node(TestProcess1, namespace={'alpha': orm.Int(1), 'beta': orm.Int(2)})

        assert node.is_finished_ok
        assert results['namespace']['alpha'] == orm.Int(1)
        assert results['namespace']['beta'] == orm.Int(2)

    def test_output_validation_error(self):
        """Test that a process is marked as failed if its output namespace validation fails."""

        class TestProcess1(Process):
            """Defining a new TestProcess class for testing."""

            _node_class = orm.WorkflowNode

            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input('add_outputs', valid_type=orm.Bool, default=lambda: orm.Bool(False))
                spec.output_namespace('integer.namespace', valid_type=orm.Int, dynamic=True)
                spec.output('required_string', valid_type=orm.Str, required=True)

            def run(self):
                if self.inputs.add_outputs:
                    self.out('required_string', orm.Str('testing').store())
                    self.out('integer.namespace.two', orm.Int(2).store())

        _, node = run_get_node(TestProcess1)

        # For default inputs, no outputs will be attached, causing the validation to fail at the end so an internal
        # exit status will be set, which is a negative integer
        assert node.is_finished
        assert not node.is_finished_ok
        assert node.exit_status == TestProcess1.exit_codes.ERROR_MISSING_OUTPUT.status
        assert node.exit_message == TestProcess1.exit_codes.ERROR_MISSING_OUTPUT.message

        # When settings `add_outputs` to True, the outputs should be added and validation should pass
        _, node = run_get_node(TestProcess1, add_outputs=orm.Bool(True))
        assert node.is_finished
        assert node.is_finished_ok
        assert node.exit_status == 0

    def test_exposed_outputs(self):
        """Test the ``Process.exposed_outputs`` method."""
        from aiida.common import AttributeDict
        from aiida.common.links import LinkType
        from aiida.engine.utils import instantiate_process
        from aiida.manage import get_manager

        runner = get_manager().get_runner()

        class ChildProcess(Process):
            """Dummy process with normal output and output namespace."""

            _node_class = orm.WorkflowNode

            @classmethod
            def define(cls, spec):
                super(ChildProcess, cls).define(spec)
                spec.input('input', valid_type=orm.Int)
                spec.output('output', valid_type=orm.Int)
                spec.output('name.space', valid_type=orm.Int)

        class ParentProcess(Process):
            """Dummy process that exposes the outputs of ``ChildProcess``."""

            _node_class = orm.WorkflowNode

            @classmethod
            def define(cls, spec):
                super(ParentProcess, cls).define(spec)
                spec.input('input', valid_type=orm.Int)
                spec.expose_outputs(ChildProcess)

        node_child = orm.WorkflowNode().store()
        node_output = orm.Int(1).store()
        node_output.base.links.add_incoming(node_child, link_label='output', link_type=LinkType.RETURN)
        node_name_space = orm.Int(1).store()
        node_name_space.base.links.add_incoming(node_child, link_label='name__space', link_type=LinkType.RETURN)

        process = instantiate_process(runner, ParentProcess, input=orm.Int(1))
        exposed_outputs = process.exposed_outputs(node_child, ChildProcess)

        expected = AttributeDict({
            'name': {
                'space': node_name_space,
            },
            'output': node_output,
        })
        assert exposed_outputs == expected

    def test_exposed_outputs_non_existing_namespace(self):
        """Test the ``Process.exposed_outputs`` method for non-existing namespace."""
        from aiida.common.links import LinkType
        from aiida.engine.utils import instantiate_process
        from aiida.manage import get_manager

        runner = get_manager().get_runner()

        class ChildProcess(Process):
            """Dummy process with normal output and output namespace."""

            _node_class = orm.WorkflowNode

            @classmethod
            def define(cls, spec):
                super(ChildProcess, cls).define(spec)
                spec.input('input', valid_type=orm.Int)
                spec.output('output', valid_type=orm.Int)
                spec.output('name.space', valid_type=orm.Int)

        class ParentProcess(Process):
            """Dummy process that exposes the outputs of ``ChildProcess``."""

            _node_class = orm.WorkflowNode

            @classmethod
            def define(cls, spec):
                super(ParentProcess, cls).define(spec)
                spec.input('input', valid_type=orm.Int)
                spec.expose_outputs(ChildProcess, namespace='child')

        node_child = orm.WorkflowNode().store()
        node_output = orm.Int(1).store()
        node_output.base.links.add_incoming(node_child, link_label='output', link_type=LinkType.RETURN)
        node_name_space = orm.Int(1).store()
        node_name_space.base.links.add_incoming(node_child, link_label='name__space', link_type=LinkType.RETURN)

        process = instantiate_process(runner, ParentProcess, input=orm.Int(1))

        # If the ``namespace`` does not exist, for example because it is slightly misspelled, a ``KeyError`` is raised
        with pytest.raises(KeyError):
            process.exposed_outputs(node_child, ChildProcess, namespace='cildh')


class TestValidateDynamicNamespaceProcess(Process):
    """Simple process with dynamic input namespace."""

    _node_class = orm.WorkflowNode

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.inputs.dynamic = True


@pytest.mark.usefixtures('clear_database_before_test')
def test_input_validation_storable_nodes():
    """Test that validation catches non-storable inputs even if nested in dictionary for dynamic namespace.

    Regression test for #5128.
    """
    with pytest.raises(ValueError):
        run(TestValidateDynamicNamespaceProcess, **{'namespace': {'a': 1}})
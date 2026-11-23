from typing import Annotated

from aiida import orm
from aiida.common.exceptions import GraphTypeCheckError
from aiida.engine import PortField, graph, task

Writable = Annotated[orm.StructureData, PortField(mutable=True)]


@task
def expand(structure: Writable) -> orm.StructureData:
    structure.set_cell([[5, 0, 0], [0, 5, 0], [0, 0, 5]])
    return structure


@graph
def prepare(structure: orm.StructureData) -> orm.StructureData:
    # No mutable=True on this boundary.
    return expand(structure=structure)


@graph
def report(structure: Writable) -> orm.StructureData:
    return prepare(structure=structure)


def run_case():
    try:
        report.build()
    except GraphTypeCheckError as error:
        assert orm.QueryBuilder().append(orm.Node).count() == 0
        return str(error)
    raise AssertionError('Expected the build to reject this binding')

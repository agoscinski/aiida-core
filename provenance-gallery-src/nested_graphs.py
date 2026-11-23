from typing import Annotated, TypedDict

from aiida import orm
from aiida.engine import PortField, graph, run_get_node, task

Writable = Annotated[orm.StructureData, PortField(mutable=True)]


@task
def expand(structure: Writable) -> orm.StructureData:
    structure.set_cell([[5, 0, 0], [0, 5, 0], [0, 0, 5]])
    return structure


@graph
def prepare(structure: Writable) -> orm.StructureData:
    return expand(structure=structure)


class Report(TypedDict):
    original: orm.StructureData
    expanded: orm.StructureData
    count: int


@graph
def report(structure: Writable) -> Report:
    expanded = prepare(structure=structure)
    return {'original': structure, 'expanded': expanded, 'count': 1}


def run_case():
    source = orm.StructureData(cell=[[1, 0, 0], [0, 1, 0], [0, 0, 1]])
    outputs, node = run_get_node(report, structure=source)
    assert outputs['original'].uuid == source.uuid
    assert outputs['expanded'].creator.uuid == node.called[0].uuid
    assert outputs['count'].creator.uuid == node.uuid
    assert len(node.called) == 1
    return node

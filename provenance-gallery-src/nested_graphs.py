import typing as t

from aiida import orm
from aiida.engine import PortField, graph, run_get_node, task

Writable = t.Annotated[orm.StructureData, PortField(mutable=True)]


@task
def expand(structure: Writable) -> orm.StructureData:
    structure.set_cell([[5, 0, 0], [0, 5, 0], [0, 0, 5]])
    return structure


@graph
def prepare(structure: Writable) -> orm.StructureData:
    return expand(structure=structure)


class Report(t.TypedDict):
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
    (nested,) = node.called
    assert isinstance(nested, orm.GraphNode)
    assert nested.inputs.structure.uuid == source.uuid
    assert nested.outputs.result.uuid == outputs['expanded'].uuid
    (calculation,) = nested.called
    assert outputs['expanded'].creator.uuid == calculation.uuid
    assert outputs['count'].creator.uuid == node.uuid
    return node

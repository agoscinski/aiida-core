import typing as t

from aiida import orm
from aiida.engine import PortField, graph, run_get_node, task

Writable = t.Annotated[orm.StructureData, PortField(mutable=True)]


class Pair(t.TypedDict):
    small: orm.StructureData
    large: orm.StructureData


@task
def resize(first: Writable, second: Writable) -> Pair:
    first.set_cell([[2, 0, 0], [0, 2, 0], [0, 0, 2]])
    second.set_cell([[3, 0, 0], [0, 3, 0], [0, 0, 3]])
    return {'small': first, 'large': second}


@graph
def size_pair(structure: Writable) -> Pair:
    resized = resize(first=structure, second=structure)
    return {'small': resized['small'], 'large': resized['large']}


def run_case():
    source = orm.StructureData(cell=[[1, 0, 0], [0, 1, 0], [0, 0, 1]])
    outputs, node = run_get_node(size_pair, structure=source)
    child = node.called[0]
    assert child.inputs.first.uuid == child.inputs.second.uuid == source.uuid
    assert len({source.uuid, outputs['small'].uuid, outputs['large'].uuid}) == 3
    return node

import typing as t

from aiida import orm
from aiida.engine import PortField, run_get_node, task

Writable = t.Annotated[orm.StructureData, PortField(mutable=True)]


@task
def expand(structure: Writable) -> orm.StructureData:
    structure.set_cell([[5, 0, 0], [0, 5, 0], [0, 0, 5]])
    return structure


def run_case():
    source = orm.StructureData(cell=[[1, 0, 0], [0, 1, 0], [0, 0, 1]])
    _, node = run_get_node(expand, structure=source)
    expanded = node.outputs.result
    assert source.uuid != expanded.uuid
    assert source.get_cell_volume() == 1
    assert round(expanded.get_cell_volume()) == 125
    return node

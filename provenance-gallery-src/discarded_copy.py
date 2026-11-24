import typing as t

from aiida import orm
from aiida.engine import PortField, run_get_node, task

Writable = t.Annotated[orm.StructureData, PortField(mutable=True)]


@task
def measure_after_expansion(structure: Writable) -> float:
    structure.set_cell([[4, 0, 0], [0, 4, 0], [0, 0, 4]])
    return structure.get_cell_volume()


def run_case():
    source = orm.StructureData(cell=[[1, 0, 0], [0, 1, 0], [0, 0, 1]])
    _, node = run_get_node(measure_after_expansion, structure=source)
    assert round(node.outputs.result.value) == 64
    assert orm.QueryBuilder().append(orm.StructureData).count() == 1
    return node

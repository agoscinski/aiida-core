from typing import Annotated

from aiida import orm
from aiida.engine import PortField, run_get_node, task

Writable = Annotated[orm.StructureData, PortField(mutable=True)]


@task
def measure_after_expansion(structure: Writable) -> float:
    structure.set_cell([[4, 0, 0], [0, 4, 0], [0, 0, 4]])
    return structure.get_cell_volume()


def run_case():
    source = orm.StructureData(cell=[[1, 0, 0], [0, 1, 0], [0, 0, 1]])
    volume, node = run_get_node(measure_after_expansion, structure=source)
    assert round(volume.value) == 64
    assert orm.QueryBuilder().append(orm.StructureData).count() == 1
    return node

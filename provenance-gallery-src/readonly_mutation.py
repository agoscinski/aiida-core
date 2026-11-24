from aiida import orm
from aiida.common.exceptions import ModificationNotAllowed
from aiida.engine import run_get_node, task


@task
def edit_read_only(structure: orm.StructureData) -> orm.StructureData:
    structure.set_cell([[5, 0, 0], [0, 5, 0], [0, 0, 5]])
    return structure


def run_case():
    source = orm.StructureData(cell=[[1, 0, 0], [0, 1, 0], [0, 0, 1]])
    try:
        run_get_node(edit_read_only, structure=source)
    except ModificationNotAllowed:
        (node,) = orm.QueryBuilder().append(orm.TaskNode).one()
        assert node.is_excepted
        assert source.get_cell_volume() == 1
        return node
    raise AssertionError('Expected mutation of the stored input to fail')

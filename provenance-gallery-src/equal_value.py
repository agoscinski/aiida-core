from typing import TypedDict

from aiida import orm
from aiida.engine import graph, run_get_node


class Pair(TypedDict):
    existing: int
    computed: int


@graph
def equal_but_distinct(value: int) -> Pair:
    return {'existing': value, 'computed': value + 0}


def run_case():
    source = orm.Int(7).store()
    outputs, node = run_get_node(equal_but_distinct, value=source)
    assert outputs['existing'].uuid == source.uuid
    assert outputs['computed'].uuid != source.uuid
    assert outputs['computed'].value == source.value
    assert outputs['existing'].creator is None
    assert outputs['computed'].creator.uuid == node.uuid
    return node

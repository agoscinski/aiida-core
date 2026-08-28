###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""`Data` sub class to represent an integer value."""

from __future__ import annotations

import numbers
import typing as t
from collections.abc import Sequence

from aiida.orm.fields import AttributeField, BaseField

from .base import to_aiida_type
from .numeric import NumericType

__all__ = ('Int',)


class Int(NumericType):
    """`Data` sub class to represent an integer value."""

    _type = int

    _attribute_fields: t.ClassVar[Sequence[BaseField]] = (AttributeField('value', int, 'The value of the integer'),)


@to_aiida_type.register(numbers.Integral)
def _(value):
    return Int(value)

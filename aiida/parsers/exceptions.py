# -*- coding: utf-8 -*-
from aiida.common.exceptions import ValidationError, ParsingError

__copyright__ = u"Copyright (c), This file is part of the AiiDA platform. For further information please visit http://www.aiida.net/.. All rights reserved."
__license__ = "MIT license, see LICENSE.txt file"
__version__ = "0.6.0.1"
__authors__ = "The AiiDA team."


class OutputParsingError(ParsingError):
    pass


class FailedJobError(ValidationError):
    pass        
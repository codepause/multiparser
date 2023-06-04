import abc
from typing import Any

from multiparser.core.single_driver import SingleDriverBase
from multiparser.core.request import Request


class Parser(abc.ABC):
    @abc.abstractmethod
    def __call__(self, single_driver: SingleDriverBase, request: Request, *args, **kwargs) -> Any:
        pass

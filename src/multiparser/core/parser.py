import abc

from multiparser.core.single_driver import SingleDriver
from multiparser.core.request import Request


class Parser(abc.ABC):
    @abc.abstractmethod
    def __call__(self, single_driver: SingleDriver, request: Request, *args, **kwargs):
        pass

from typing import Any


class Request:
    """
    Base class for Request.

    parent_idx (int): Inner request idx.
    args (tuple): passed on call to parser
    kwargs (dict): passed on call to parser

    Args:
        parser (Parser):

    Keyword Args:
        args (tuple): passed on call to parser
        kwargs (dict): passed on call to parser
        total_parts (int): total requests to fill RequestData.
        current_part (int): current idx of request.
    """

    def __init__(self, parser: 'Parser', args: tuple = None, kwargs: dict = None, total_parts: int = 1,
                 current_part: int = 0, **__):
        self.parent_idx = None  # Inner request idx.
        self.parser = parser
        self.args = args or tuple()
        self.kwargs = kwargs or dict()
        self.total_parts = total_parts
        self.current_part = current_part

    def is_last(self) -> bool:
        return self.total_parts == self.current_part + 1

    def __call__(self, single_driver: 'SingleDriverBase') -> Any:
        return self.parser(single_driver, self, *self.args, **self.kwargs)

    def __repr__(self) -> str:
        s = f'{self.__class__.__name__}[{self.args}, {self.kwargs}]'
        return s


class RequestData:
    """
    Container for data received.
    """

    def __init__(self):
        self.total_parts = 1
        self.parts_done = 0
        self.data = dict()

    def add_data(self, job_result: dict):
        req = job_result['request']
        data = job_result['data']
        if req.is_last():
            self.total_parts = req.total_parts
        self.data[req.current_part] = {'data': data, 'request': req}
        self.parts_done += 1

    def is_completed(self) -> bool:
        return self.total_parts >= self.parts_done

    def __repr__(self) -> str:
        s = f'{self.__class__.__name__}:[\n{self.data}\n]\n[{self.parts_done}/{self.total_parts} parts done]'
        return s

import threading

from multiparser.core.requests_handler import RequestsHandler
from multiparser.core.single_driver import SingleDriverBase


class MultiThreadDriver:
    def __init__(self, request_handler: 'RequestsHandler', n_workers=1,
                 driver_constructor: callable = SingleDriverBase):
        self._n_workers = n_workers
        self.request_handler = request_handler
        self.driver_constructor = driver_constructor

        self._workers = list()
        self._done = False
        self._working_thread = None
        self.lock = threading.Lock()

    def spawn_workers(self):
        # spawning single drivers
        for n_worker in range(self._n_workers):
            worker = self.driver_constructor(n_worker, self.request_handler, self.lock)
            worker.start()
            self._workers.append(worker)

    def kill_workers(self):
        for idx, worker in enumerate(self._workers):
            worker.stop()

    def start(self):
        print('MultiThreadParser started')
        self.spawn_workers()
        self._done = False

    def stop(self):
        print('MultiThreadParser stopped')
        self._done = True
        self.kill_workers()
        # self.join()

    def join(self):
        self._working_thread.join()

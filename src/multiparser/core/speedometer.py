import time


class Speedometer:
    """
    CLass to dynamically adjust request rates.
    Adjusts wait time to mean_speed < max_speed.

    max_speed (float): Max speed in req/sec
    window (int): calculate mean value.
    """

    def __init__(self, max_speed: float = float('inf')):
        self.max_speed = max_speed
        self.frequency = 1 / max_speed

        self._last_time_activated = 0

    def wait_required_time(self, lock: 'Lock'):
        with lock:
            delta = time.time() - self._last_time_activated
            if delta > self.frequency:
                pass
            else:
                time.sleep(self.frequency - delta)
            self._last_time_activated = time.time()

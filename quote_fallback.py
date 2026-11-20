"""One in-flight read per source; a slow fallback never owns quote intake."""
import threading
import time


class BackgroundRead:
    def __init__(self, load, wake, name, retry_seconds=1):
        self.load, self.wake, self.name = load, wake, name
        self.retry_seconds = retry_seconds
        self.lock = threading.Lock()
        self.requested = threading.Event()
        self.stopped = threading.Event()
        self.thread = None
        self.busy = False
        self.value = self.error = None
        self.completed = None

    def read(self, interval):
        """Return a completed observation, scheduling at most one new read."""
        with self.lock:
            delay = max(interval, self.retry_seconds) if self.error else interval
            if (not self.stopped.is_set() and not self.busy and
                    (self.completed is None or time.monotonic()-self.completed >= delay)):
                self.busy = True
                if self.thread is None:
                    self.thread = threading.Thread(target=self._loop, name=self.name, daemon=True)
                    try:self.thread.start()
                    except Exception:
                        self.thread=None;self.busy=False
                        raise
                self.requested.set()
            return self.value, self.error

    def _loop(self):
        while True:
            self.requested.wait()
            self.requested.clear()
            if self.stopped.is_set(): return
            value = error = None
            try: value = self.load()
            except Exception as exc: error = exc
            with self.lock:
                if self.stopped.is_set(): return
                self.value, self.error = value, error
                self.completed = time.monotonic()
                self.busy = False
            self.wake.set()

    def close(self):
        # An existing platform read finishes under its original timeout. Never
        # join it or publish its result into a replacement connection.
        self.stopped.set()
        self.requested.set()

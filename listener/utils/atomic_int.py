import threading


class AtomicInt:
    def __init__(self, initial=0):
        self.value = initial
        self.lock = threading.Lock()

    def set_value(self, value):
        with self.lock:
            self.value = value

    def get_value(self):
        return self.value

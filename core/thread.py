import threading


class StoppableThread(threading.Thread):
    """
    Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition. When the thread is signaled
    to stop, it also runs a custom stop function that might do some clean up.
    """

    def __init__(self, *args, **kwargs):
        stop_func = kwargs["stop"]
        del kwargs["stop"]
        super(StoppableThread, self).__init__(*args, **kwargs)
        self._stop_event = threading.Event()
        self.stop_func = stop_func

    def stop(self):
        self._stop_event.set()
        self.stop_func()

    def stopped(self):
        return self._stop_event.is_set()

import threading

class ThreadPauser:
    def __init__(self):
        self.condition = threading.Condition()
        self.result = None

    def sleep_until_resumed(self, timeout=30.0):
        """
        Wait until resumed or timeout.

        Args:
            timeout: Maximum seconds to wait (default 30s)

        Returns:
            True if resumed, False if timed out
        """
        with self.condition:
            return self.condition.wait(timeout=timeout)

    def resume_with_result(self, result):
        self.result = result
        with self.condition:
            self.condition.notify()

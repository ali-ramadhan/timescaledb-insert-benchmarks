import time

class Timer:
    def __init__(
        self,
        message="Timer",
        n=None,
        units="operations"
    ):
        self.message = message
        self.n = n
        self.units = units

    def __enter__(self):
        print(f"{self.message}: started.")
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end = time.perf_counter()
        self.interval = self.end - self.start

        if self.n and self.units:
            self.rate = self.n / self.interval
            print(f"{self.message}: {self.interval:.4f} seconds (n={self.n}, {self.rate:.2f} {self.units} per second).")
        else:
            print(f"{self.message}: {self.interval:.4f} seconds.")

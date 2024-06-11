from abc import ABC, abstractmethod


class History(ABC):
    def __init__(self, data):
        self.data = data
        self.current_idx = 0
        self.length = len(self.data)

    def __repr__(self):
        return self.data.__repr__()

    @abstractmethod
    def __next__(self):
        pass

    def reset(self):
        self.current_idx = 0

    def __iter__(self):
        self.reset()
        return self


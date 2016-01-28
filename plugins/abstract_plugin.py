from abc import ABCMeta, abstractmethod


class AbstractPlugin(metaclass=ABCMeta):
    @abstractmethod
    def get_data(self):
        pass

    @abstractmethod
    def parse_data(self):
        pass

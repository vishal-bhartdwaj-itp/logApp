from abc import abstractmethod


class BaseLogParser:

    def __init__(self):
        pass


    @abstractmethod
    def parse(self):
        pass
    


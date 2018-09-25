__author__ = 'Shahars'

class InMemoryPersister(object):
    def __init__(self):
        self.kernel = None

    def persist_kernel(self, kernel):
        self.kernel = kernel

    def get_kernel(self):
        return self.kernel
class KernelMetadata(object):
    def __init__(self, customer, kernel_hash):
        """
        @type customer: C{str}
        @type kernel_hash: C{str}
        """
        self.__customer = customer
        self.__kernel_hash = kernel_hash

    @property
    def customer(self):
        return self.__customer

    @property
    def hash(self):
        return self.__kernel_hash

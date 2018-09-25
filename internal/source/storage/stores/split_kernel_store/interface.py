from source.storage.stores.split_kernel_store.types import SplitKernelMetadata, SplitKernel, SubKernel


class SplitKernelStoreInterface(object):
    def store_split_kernel(self, split_kernel):
        """
        Stores the given split Kernel

        @param split_kernel: The split kernel to store
        @type split_kernel: L{SplitKernel}
        """
        raise NotImplementedError()

    def load_split_kernel(self, split_kernel_metadata):
        """
        Loads the split kernel
        @param split_kernel_metadata: The requested split kernel's metadata
        @type split_kernel_metadata: L{SplitKernelMetadata}
        @return: The loaded split kernel
        @rtype: L{SplitKernel}
        """
        raise NotImplementedError()

    def get_sub_kernels_ordinals_list(self, split_kernel_metadata):
        """
        Returns the list of available sub-kernel ordinals for the given split kernel

        @param split_kernel_metadata: The requested split kernel's metadata
        @type split_kernel_metadata: L{SplitKernelMetadata}

        @rtype: C{list}
        """
        raise NotImplementedError()

    def get_sub_kernels_list(self, customer, quest_id, query_id):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @rtype: C{list}
        """
        raise NotImplementedError()


class SubKernelStoreInterface(object):
    def load_sub_kernel_by_ordinal(self, split_kernel_metadata, sub_kernel_ordinal):
        """
        @param sub_kernel_ordinal: The ordinal for the requested sub-kernel
        @type sub_kernel_ordinal: C{int}
        @param split_kernel_metadata: The requested split kernel's metadata
        @type split_kernel_metadata: L{SplitKernelMetadata}
        @return: The Requested sub kernel
        @rtype: L{SubKernel}
        @raise LookupError: When the split kernel does not exist
        @raise KeyError: When there is a sub kernel for the given ordinal does not exist
        """
        raise NotImplementedError()

    def load_sub_kernel_by_ordinal_new(self, customer, quest_id, query_id, sub_kernel_ordinal):
        """
        @type customer: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @param sub_kernel_ordinal: The ordinal for the requested sub-kernel
        @type sub_kernel_ordinal: C{int}
        @return: The Requested sub kernel
        @rtype: L{SubKernel}
        @raise LookupError: When the split kernel does not exist
        @raise KeyError: When there is a sub kernel for the given ordinal does not exist
        """
        raise NotImplementedError()
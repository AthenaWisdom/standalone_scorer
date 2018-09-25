import functools
import json
import os

from source.storage.dataframe_storage_mixins.in_memory_mixin import InMemoryDataFrameStorageMixin
from source.storage.io_handlers.in_memory import InMemoryIOHandler
from source.storage.stores.split_kernel_store.interface import SplitKernelStoreInterface, SubKernelStoreInterface
from source.storage.stores.split_kernel_store.types import SplitKernelMetadata, SplitKernel, \
    SubKernel, SubKernelMetadata


class InMemorySplitKernelStore(InMemoryDataFrameStorageMixin, SplitKernelStoreInterface):
    def get_sub_kernels_list(self, customer, quest_id, query_id):
        split_kernel = self.load_split_kernel(SplitKernelMetadata(customer, query_id))
        return map(lambda s: s[0], split_kernel.df.select('sub_kernel').distinct().collect())

    def __init__(self, io_handler):
        """
        @type io_handler: InMemoryIOHandler
        """
        self.__io_handler = io_handler
        super(InMemorySplitKernelStore, self).__init__(self.__io_handler)

    def store_split_kernel(self, split_kernel):
        """
        Stores the given split Kernel

        @param split_kernel: The split kernel to store
        @type split_kernel: L{SplitKernel}
        """
        df = split_kernel.df
        sub_kernel_names = sorted(map(lambda x: x[0], df.select([df['sub_kernel']]).distinct().collect()))

        partial_output_path = functools.partial(os.path.join, 'sandbox-{}'.format(split_kernel.metadata.customer),
                                                'SplitKernels', split_kernel.metadata.id)
        sub_kernels_path = partial_output_path('sub_kernel_names.json')
        self._store_dataframe(df, partial_output_path('data'))
        self.__io_handler.save_raw_data(json.dumps(sub_kernel_names), sub_kernels_path)
        original_kernel_path = partial_output_path('original_kernel')
        self.__io_handler.save_raw_data(split_kernel.metadata.original_kernel_id, original_kernel_path)

    def load_split_kernel(self, metadata):
        """
        Loads the split kernel
        @param metadata: The requested split kernel's metadata
        @type metadata: L{SplitKernelMetadata}
        @return: The loaded split kernel
        @rtype: L{SplitKernel}
        @raise LookupError: When the split kernel does not exist
        """
        partial_input_path = functools.partial(os.path.join, 'sandbox-{}'.format(metadata.customer),
                                               'SplitKernels', metadata.id)
        sub_kernels_path = partial_input_path('sub_kernel_names.json')
        if not self.__io_handler.path_exists(sub_kernels_path):
            raise LookupError('Split Kernel "{}" does not exist '
                              'for customer "{}"'.format(metadata.id, metadata.customer))
        original_kernel_id = self.__io_handler.load_raw_data(partial_input_path('original_kernel'))
        new_metadata = SplitKernelMetadata(metadata.customer, metadata.id, original_kernel_id)
        return SplitKernel(new_metadata, self._load_dataframe(partial_input_path('data')))

    def get_sub_kernels_ordinals_list(self, split_kernel_metadata):
        partial_input_path = functools.partial(os.path.join, 'sandbox-{}'.format(split_kernel_metadata.customer),
                                               'SplitKernels', split_kernel_metadata.id)
        sub_kernels_path = partial_input_path('sub_kernel_names.json')
        return range(len(json.loads(self.__io_handler.load_raw_data(sub_kernels_path))))


class InMemorySubKernelStore(InMemoryDataFrameStorageMixin, SubKernelStoreInterface):
    def load_sub_kernel_by_ordinal_new(self, customer, quest_id, query_id, sub_kernel_ordinal):
        return self.load_sub_kernel_by_ordinal(SplitKernelMetadata(customer, query_id), sub_kernel_ordinal)

    def __init__(self, io_handler):
        """
        @type io_handler: L{InMemoryIOHandler}
        """
        self.__io_handler = io_handler
        super(InMemorySubKernelStore, self).__init__(self.__io_handler)

    def load_sub_kernel_by_ordinal(self, split_kernel_metadata, sub_kernel_ordinal):
        """
        @param sub_kernel_ordinal: The ordinal for the requested sub-kernel
        @type sub_kernel_ordinal: C{int}
        @param split_kernel_metadata: The requested split kernel's metadata
        @type split_kernel_metadata: L{SplitKernelMetadata}
        @return: The Requested sub kernel
        @rtype: L{SubKernel}
        @raise LookupError: When the split kernel does not exist
        @raise IndexError: When there is not split kernel for the given ordinal
        """
        partial_input_path = functools.partial(os.path.join, 'sandbox-{}'.format(split_kernel_metadata.customer),
                                               'SplitKernels', split_kernel_metadata.id)
        sub_kernels_path = partial_input_path('sub_kernel_names.json')
        if not self.__io_handler.path_exists(sub_kernels_path):
            raise LookupError('Split Kernel "{}" does not exist '
                              'for customer "{}"'.format(split_kernel_metadata.id, split_kernel_metadata.customer))
        sub_kernel_name = json.loads(self.__io_handler.load_raw_data(sub_kernels_path))[sub_kernel_ordinal]
        split_kernel_df = self._load_dataframe(partial_input_path('data'))
        sub_kernel_df = split_kernel_df.where('sub_kernel = "{}"'.format(sub_kernel_name)).toPandas()
        sub_kernel_metadata = SubKernelMetadata(split_kernel_metadata, sub_kernel_ordinal, *sub_kernel_name.split('='))
        return SubKernel(sub_kernel_metadata, sub_kernel_df)


class InMemoryWritableSubKernelStore(SubKernelStoreInterface):
    def __init__(self):
        self.__stored_sub_kernels = {}

    def load_sub_kernel_by_ordinal(self, split_kernel_metadata, sub_kernel_ordinal):
        raise NotImplementedError('This should not be used - remove this once the enw tests are running.')

    def load_sub_kernel_by_ordinal_new(self, customer, quest_id, query_id, sub_kernel_ordinal):
        key = str(customer) + '_' + str(quest_id) + '_' + str(query_id) + '_' + str(sub_kernel_ordinal)
        return SubKernel(self.__stored_sub_kernels[key]['meta'], self.__stored_sub_kernels[key]['df'])

    def store_sub_kernel(self, customer, quest_id, query_id, sub_kernel_ordinal, sub_kernel_df):
        key = str(customer) + '_' + str(quest_id) + '_' + str(query_id) + '_' + str(sub_kernel_ordinal)
        self.__stored_sub_kernels[key] = {
            'meta': SubKernelMetadata(SplitKernelMetadata(customer, query_id), sub_kernel_ordinal, 'dont-care-field', 'dont-care-value'),
            'df': sub_kernel_df
        }

import functools
import json
import logging
import os
from cStringIO import StringIO

import pandas as pd

from source.storage.io_handlers.interface import IOHandlerInterface
from source.storage.stores.split_kernel_store.interface import SplitKernelStoreInterface, SubKernelStoreInterface
from source.storage.stores.split_kernel_store.types import SplitKernelMetadata, SubKernel, SubKernelMetadata, \
    split_sub_kernel_into_properties
from source.utils.schema import FIELD_TYPE_NA_VALUES


class CSVSplitKernelStore(SplitKernelStoreInterface):
    def __init__(self, io_handler, hive_context):
        """
        @type io_handler: L{IOHandlerInterface}
        @type hive_context:C{HiveContext}
        """
        self.__io_handler = io_handler

    def get_sub_kernels_ordinals_list(self, split_kernel_metadata):
        sub_kernels_path = os.path.join('sandbox-{}'.format(split_kernel_metadata.customer),
                                        'SplitKernels', split_kernel_metadata.id, 'sub_kernel_names.json')
        return range(len(json.loads(self.__io_handler.load_raw_data(sub_kernels_path))))

    def get_sub_kernels_list(self, customer, quest_id, query_id):
        sub_kernels_path = os.path.join('sandbox-{}'.format(customer), 'Quests', quest_id,
                                        query_id, 'splitKernelStrategy.json')
        return json.loads(self.__io_handler.load_raw_data(sub_kernels_path))


class CSVSubKernelStore(SubKernelStoreInterface):
    def __init__(self, io_handler):
        """
        @type io_handler: L{IOHandlerInterface}
        """
        self.__io_handler = io_handler
        self.__logger = logging.getLogger('endor')

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

        part_name = 'part-r-{0:05d}'.format(sub_kernel_ordinal)
        data_folder_path = partial_input_path('data')
        data_input_path = filter(lambda x: part_name in x, self.__io_handler.list_dir(data_folder_path)).pop()
        sub_kernel_data = StringIO(self.__io_handler.load_raw_data(data_input_path))
        sub_kernel_df = pd.read_csv(sub_kernel_data, index_col=None, encoding='utf-8',
                                    na_values=FIELD_TYPE_NA_VALUES.values())
        sub_kernels_in_data = set(sub_kernel_df['sub_kernel'].values)
        if {sub_kernel_name} != sub_kernels_in_data:
            self.__logger.warning('Mismatch between sub kernels list ({}) '
                                  'and data ({})'.format({sub_kernel_name}, sub_kernels_in_data))
        sub_kernel_metadata = SubKernelMetadata(split_kernel_metadata, sub_kernel_ordinal,
                                                *split_sub_kernel_into_properties(sub_kernel_name))
        return SubKernel(sub_kernel_metadata, sub_kernel_df.drop('sub_kernel', 1))

    def load_sub_kernel_by_ordinal_new(self, customer, quest_id, query_id, sub_kernel_ordinal):
        partial_input_path = functools.partial(os.path.join, 'sandbox-{}'.format(customer), 'Quests',
                                               quest_id, query_id)
        sub_kernels_path = partial_input_path('splitKernelStrategy.json')
        if not self.__io_handler.path_exists(sub_kernels_path):
            raise LookupError('Split Kernel "{}" does not exist '
                              'for customer "{}"'.format(quest_id, customer))

        sub_kernel_name = json.loads(self.__io_handler.load_raw_data(sub_kernels_path))[sub_kernel_ordinal]

        part_name = 'part-{0:05d}'.format(sub_kernel_ordinal)
        data_folder_path = partial_input_path('splitKernel', 'data')
        data_input_path = filter(lambda x: part_name in x, self.__io_handler.list_dir(data_folder_path)).pop()
        sub_kernel_data = StringIO(self.__io_handler.load_raw_data(data_input_path))
        sub_kernel_df = pd.read_csv(sub_kernel_data, index_col=None, encoding='utf-8',
                                    na_values=FIELD_TYPE_NA_VALUES.values())
        sub_kernels_in_data = set(sub_kernel_df['sub_kernel'].values)
        if {sub_kernel_name} != sub_kernels_in_data:
            self.__logger.warning('Mismatch between sub kernels list ({}) '
                                  'and data ({})'.format({sub_kernel_name}, sub_kernels_in_data))
        sub_kernel_metadata = SubKernelMetadata(SplitKernelMetadata(customer, query_id), sub_kernel_ordinal,
                                                *split_sub_kernel_into_properties(sub_kernel_name))
        return SubKernel(sub_kernel_metadata, sub_kernel_df.drop('sub_kernel', 1))
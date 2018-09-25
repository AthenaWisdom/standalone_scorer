import pandas as pd
from pandas import DataFrame as PandasDataFrame


def split_sub_kernel_into_properties(sub_kernel_name):
    """
    @type sub_kernel_name: C{str}
    """
    if '===' in sub_kernel_name:
        return sub_kernel_name.split('===')
    return sub_kernel_name.split('=')


class SplitKernelMetadata(object):
    def __init__(self, customer, split_kernel_id, original_kernel_id=None):
        """
        @param original_kernel_id: The ID of the kernel from which this split kernel was generated.
            None allowed if unknown.
        @type original_kernel_id: C{str}
        @type customer: C{str}
        @type split_kernel_id: C{str}
        """
        self.__original_kernel_id = original_kernel_id
        self.__split_kernel_id = split_kernel_id
        self.__customer = customer

    @property
    def customer(self):
        return self.__customer

    @property
    def id(self):
        return self.__split_kernel_id

    @property
    def original_kernel_id(self):
        return self.__original_kernel_id


class SplitKernel(object):
    def __init__(self, metadata, split_kernel_df):
        """
        @type metadata: L{SplitKernelMetadata}
        @type split_kernel_df: L{SparkDataFrame}
        """
        self.__split_kernel_df = split_kernel_df
        self.__metadata = metadata
        self.__splitting_strategy = None

    @property
    def df(self):
        return self.__split_kernel_df

    @property
    def metadata(self):
        return self.__metadata

    @property
    def splitting_strategy(self):
        """
        @rtype: C{list}
        """
        if self.__splitting_strategy is None:
            strategy_pairs = map(lambda s: split_sub_kernel_into_properties(s[0]),
                                 self.df.select('sub_kernel').distinct().collect())
            self.__splitting_strategy = [{'key': x, 'value': y} for x, y in strategy_pairs]
        return self.__splitting_strategy

    @property
    def num_sub_kernels(self):
        return len(self.splitting_strategy)


class SubKernelMetadata(object):
    def __init__(self, split_kernel_metadata, part_number, field_name, field_value):
        """
        @type split_kernel_metadata: L{SplitKernelMetadata}
        @type part_number: C{int}
        @type field_name: C{str}
        @type field_value: C{str}
        """
        self.__field_value = field_value
        self.__field_name = field_name
        self.__part_number = part_number
        self.__split_kernel_metadata = split_kernel_metadata

    @property
    def field_name(self):
        return self.__field_name

    @property
    def field_value(self):
        return self.__field_value

    @property
    def part_number(self):
        return self.__part_number

    @property
    def split_kernel_metadata(self):
        return self.__split_kernel_metadata


class SubKernel(object):
    CONST_KERNEL_FIELDS = {"ID", "white", "ground", "black", "priority", "sub_kernel"}

    def __init__(self, metadata, sub_kernel_df):
        """
        @type metadata: L{SubKernelMetadata}
        @type sub_kernel_df: C{PandasDataFrame}
        """
        self.__sub_kernel_df = sub_kernel_df
        self.__metadata = metadata

    def __get_ids_by_field(self, field_name=None):

        df = self.__sub_kernel_df
        if field_name is not None:
            filtered_df = df[df[field_name] == 1]
        else:
            filtered_df = df
        return list(set(filtered_df['ID'].astype(float).values))

    @property
    def df(self):
        return self.__sub_kernel_df

    @property
    def metadata(self):
        return self.__metadata

    @property
    def whites(self):
        return self.__get_ids_by_field('white')

    @property
    def blacks(self):
        return self.__get_ids_by_field('black')

    @property
    def ground(self):
        return self.__get_ids_by_field('ground')

    @property
    def all_ids(self):
        return self.__get_ids_by_field()

    @property
    def external_data(self):
        external_cols = [col for col in self.__sub_kernel_df.columns if col not in self.CONST_KERNEL_FIELDS]
        if len(external_cols) > 0:
            return self.__sub_kernel_df[external_cols + ["ID"]]
        else:
            return pd.DataFrame()

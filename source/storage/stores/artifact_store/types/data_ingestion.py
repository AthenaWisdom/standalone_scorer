from datetime import datetime

from interface import ArtifactInterface, ExternalJobArtifactInterface
from source.storage.stores.artifact_store.types.common import DurationMetricArtifact

__all__ = [
    'InputCSVStatsArtifact',
    'SphereSpecificationsArtifact',
    'SphereGenerationConfigurationArtifact',
    'TopValuesPerColumn',
    'BottomValuesPerColumn',
    'SphereFrequentValuesPerColumnArtifact',
    'FootprintAnalyzingExternalJobArtifact',
    'ClustersFootprintHTMLArtifact',
    'ClustersExtractionExternalJobArtifact',
    'ClustersUnificationExternalJobArtifact',
    'ClustersUnificationPricingDurationMetricArtifact',
    'ClustersExtractionPricingDurationMetricArtifact',
]


# noinspection PyAbstractClass
class DataIngestionBaseArtifact(ArtifactInterface):
    operation = 'sphere_generation'


class InputCSVStatsArtifact(DataIngestionBaseArtifact):
    type = 'input_csv_stats'

    def __init__(self, customer, dataset, sphere_id, input_csv_name, min_time, max_time, num_users):
        """
        @type customer: C{str}
        @type dataset: C{str}
        @type sphere_id: C{str}
        @type input_csv_name: C{str}
        @type min_time: C{datetime}
        @type max_time: C{datetime}
        @type num_users: C{int}
        """
        super(InputCSVStatsArtifact, self).__init__(customer, sphere_id)
        self.__num_users = num_users
        self.__max_time = max_time
        self.__min_time = min_time
        self.__input_csv_name = input_csv_name
        self.__dataset = dataset
        self.__sphere_id = sphere_id

    def _to_dict(self):
        return {
            'num_users': self.__num_users,
            'max_time': self.__max_time,
            'min_time': self.__min_time,
            'input_csv_name': self.__input_csv_name,
            'dataset': self.__dataset,
            'sphere_id': self.__sphere_id,
        }


class SphereSpecificationsArtifact(DataIngestionBaseArtifact):
    type = 'sphere_specs'

    def __init__(self, customer, dataset, sphere_id, connecting_fields, input_csvs, reference_date):
        """
        @type customer: C{str}
        @type dataset: C{str}
        @type sphere_id: C{str}
        @type connecting_fields: C{list} of C{str}
        @type input_csvs: C{list} of C{str}
        @type reference_date: C{datetime}
        """
        super(SphereSpecificationsArtifact, self).__init__(customer, sphere_id)
        self.__reference_date = reference_date
        self.__input_csvs = input_csvs
        self.__connecting_fields = connecting_fields
        self.__dataset = dataset
        self.__sphere_id = sphere_id

    def _to_dict(self):
        return {
            'reference_date': self.__reference_date,
            'input_csvs': self.__input_csvs,
            'connecting_fields': self.__connecting_fields,
            'dataset': self.__dataset,
            'sphere_id': self.__sphere_id,
        }


class SphereGenerationConfigurationArtifact(DataIngestionBaseArtifact):
    type = 'sphere_generation_configuration'

    def __init__(self, customer, dataset, sphere_id, config):
        """
        @type customer: C{str}
        @type dataset: C{str}
        @type sphere_id: C{str}
        @type config: C{dict}
        """
        super(SphereGenerationConfigurationArtifact, self).__init__(customer, sphere_id)
        self.__config = config
        self.__sphere_id = sphere_id
        self.__dataset = dataset

    def _to_dict(self):
        return {
            'sphere_id': self.__sphere_id,
            'dataset': self.__dataset,
            'config': self.__config,
        }


class ValuesPerColumnArtifact(DataIngestionBaseArtifact):
    type = 'META_values_per_column'

    def __init__(self, customer, dataset, sphere_id, column_name, string_value=None,
                 integer_value=None, float_value=None, date_value=None, boolean_value=None):
        """
        @type customer: C{str}
        @type dataset: C{str}
        @type sphere_id: C{str}
        @type column_name: C{str}
        @type string_value: C{list} of C{str}
        @type integer_value: C{list} of C{int}
        @type float_value: C{list} of C{float}
        @type date_value: C{list} of (C{datetime} or C{date})
        @type boolean_value: C{list} of C{bool}
        """
        super(ValuesPerColumnArtifact, self).__init__(customer, sphere_id)
        self.__customer = customer
        self.__dataset = dataset
        self.__sphere_id = sphere_id
        self.__column_name = column_name
        self.__string_value = string_value
        self.__integer_value = integer_value
        self.__float_value = float_value
        self.__date_value = date_value
        self.__boolean_value = boolean_value

    def _to_dict(self):
        parent_dict = super(ValuesPerColumnArtifact, self)._to_dict()
        parent_dict.update({
            'sphere_id': self.__sphere_id,
            'boolean_value': self.__boolean_value,
            'dataset': self.__dataset,
            'date_value': self.__date_value,
            'float_value': self.__float_value,
            'integer_value': self.__integer_value,
            'column_name': self.__column_name,
            'string_value': self.__string_value
        })
        return parent_dict


class TopValuesPerColumn(ValuesPerColumnArtifact):
    type = 'top_values'


class BottomValuesPerColumn(ValuesPerColumnArtifact):
    type = 'bottom_values'


class SphereColumnStatsArtifact(DataIngestionBaseArtifact):
    type = 'sphere_column_stats'

    def __init__(self, customer, dataset, sphere_id, stat_name, string_value=None,
                 integer_value=None, float_value=None, date_value=None, boolean_value=None):
        """
        @type customer: C{str}
        @type dataset: C{str}
        @type sphere_id: C{str}
        @type stat_name: C{str}
        @type string_value: C{str}
        @type integer_value: C{int}
        @type float_value: C{float}
        @type date_value: C{datetime} or C{date}
        @type boolean_value: C{bool}
        """
        super(DataIngestionBaseArtifact, self).__init__(customer, sphere_id)
        self.__customer = customer
        self.__dataset = dataset
        self.__sphere_id = sphere_id
        self.__stat_name = stat_name
        self.__string_value = string_value
        self.__integer_value = integer_value
        self.__float_value = float_value
        self.__date_value = date_value
        self.__boolean_value = boolean_value

    def _to_dict(self):
        return {
            'sphere_id': self.__sphere_id,
            'boolean_value': self.__boolean_value,
            'dataset': self.__dataset,
            'date_value': self.__date_value,
            'float_value': self.__float_value,
            'integer_value': self.__integer_value,
            'stat_name': self.__stat_name,
            'string_value': self.__string_value
        }


class SphereFrequentValuesPerColumnArtifact(ValuesPerColumnArtifact):
    type = 'frequent_values_per_column'


class ClustersExtractionExternalJobArtifact(ExternalJobArtifactInterface, DataIngestionBaseArtifact):
    type = 'clusters_extraction_external_job'

    def __init__(self, customer, sphere_id, job_id, num_tasks):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @type job_id: C{str}
        @type num_tasks: C{int}
        @type stage: C{int}
        """
        super(ClustersExtractionExternalJobArtifact, self).__init__(customer, sphere_id, job_id, num_tasks)
        self.__sphere_id = sphere_id

    def _to_dict(self):
        return {
            'sphere_id': self.__sphere_id,
        }

    @property
    def sphere_id(self):
        return self.__sphere_id


class ClustersUnificationExternalJobArtifact(ExternalJobArtifactInterface, DataIngestionBaseArtifact):
    type = 'clusters_unification_external_job'

    def __init__(self, customer, sphere_id, job_id, num_tasks):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @type job_id: C{str}
        @type num_tasks: C{int}
        @type stage: C{int}
        """
        super(ClustersUnificationExternalJobArtifact, self).__init__(customer, sphere_id, job_id, num_tasks)
        self.__sphere_id = sphere_id

    def _to_dict(self):
        return {
            'sphere_id': self.__sphere_id,
        }

    @property
    def sphere_id(self):
        return self.__sphere_id


class ClustersUnificationPricingDurationMetricArtifact(DurationMetricArtifact):
    type = 'clusters_unification_pricing_duration_metric'


class ClustersUnificationSourceHashArtifact(DataIngestionBaseArtifact):
    type = 'clusters_unification_source_hash_artifact'

    def __init__(self, customer, sphere_id, file_hashes_map):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @type file_hashes_map: C{dict}
        """
        super(ClustersUnificationSourceHashArtifact, self).__init__(customer, sphere_id)
        self.__file_hashes_map = file_hashes_map
        self.__sphere_id = sphere_id

    def _to_dict(self):
        return {
            'sphere_id': self.__sphere_id,
            'file_hashes_map': self.__file_hashes_map,
        }

    @property
    def sphere_id(self):
        return self.__sphere_id


class ClustersExtractionPricingDurationMetricArtifact(DurationMetricArtifact):
    type = 'clusters_extraction_pricing_duration_metric'


class MergeSpheresConfigurationArtifact(ArtifactInterface):
    type = 'merge_spheres_configuration'
    operation = 'merge_spheres'

    def __init__(self, customer, sphere_id, input_spheres):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @type input_spheres: C{list} of C{dict}
        """
        super(MergeSpheresConfigurationArtifact, self).__init__(customer, sphere_id)
        self.__input_spheres = input_spheres
        self.__sphere_id = sphere_id

    def _to_dict(self):
        return {
            'input_spheres': self.__input_spheres,
            'sphere_id': self.__sphere_id
        }


class ClustersFootprintHTMLArtifact(DataIngestionBaseArtifact):
    type = 'clusters_footprint_html_artifact'

    def __init__(self, customer, sphere_id, html_url):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @type html_url: C{str}
        """
        super(ClustersFootprintHTMLArtifact, self).__init__(customer, sphere_id)
        self.__sphere_id = sphere_id
        self.__html_url = html_url

    def _to_dict(self):
        return {
            'sphere_id': self.__sphere_id,
            'html_url': self.__html_url,
        }


class ClustersFootprintAssertionArtifact(DataIngestionBaseArtifact):
    type = 'clusters_footprint_assertion_artifact'

    def __init__(self, customer, sphere_id, agg_dict, clusters_per_dataset_and_run_pair):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @type agg_dict: C{dict}
        @type clusters_per_dataset_and_run_pair: C{dict}
        """
        super(ClustersFootprintAssertionArtifact, self).__init__(customer, sphere_id)
        self.__sphere_id = sphere_id
        self.__agg_dict = agg_dict
        self.__clusters_per_dataset_and_run_pair = clusters_per_dataset_and_run_pair

    def _to_dict(self):
        return {
            'sphere_id': self.__sphere_id,
            'clusters_per_dataset_and_run_pair': self.__clusters_per_dataset_and_run_pair,
            'min_max_mean': self.__agg_dict,
        }


class FootprintAnalyzingExternalJobArtifact(ExternalJobArtifactInterface, DataIngestionBaseArtifact):
    type = 'footprint_analyzing_external_job'

    def __init__(self, customer, sphere_id, job_id, num_tasks):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @type job_id: C{str}
        @type num_tasks: C{int}
        @type stage: C{int}
        """
        super(FootprintAnalyzingExternalJobArtifact, self).__init__(customer, sphere_id, job_id, num_tasks)
        self.__sphere_id = sphere_id

    def _to_dict(self):
        return {
            'sphere_id': self.__sphere_id,
        }

    @property
    def sphere_id(self):
        return self.__sphere_id


class RunPairDiskUsageArtifact(DataIngestionBaseArtifact):
    type = 'run_pair_disk_usage_artifact'

    def __init__(self, customer, sphere_id, connecting_field, input_csv_name, disk_usage_percent):
        """
        @type input_csv_name: C{str}
        @type connecting_field: C{str}
        @type customer: C{str}
        @type sphere_id: C{str}
        @type disk_usage_percent: C{float}
        """
        super(RunPairDiskUsageArtifact, self).__init__(customer, sphere_id)
        self.__input_csv_name = input_csv_name
        self.__connecting_field = connecting_field
        self.__sphere_id = sphere_id
        self.__disk_usage_percent = disk_usage_percent

    def _to_dict(self):
        return {
            'sphere_id': self.__sphere_id,
            'disk_usage': self.__disk_usage_percent,
            'connecting_field': self.__connecting_field,
            'input_csv_name': self.__input_csv_name,
        }


class InputCsvSizeArtifact(DataIngestionBaseArtifact):
    type = 'input_csv_size_artifact'

    def __init__(self, customer, sphere_id, connecting_field, input_csv_name, input_csv_size, num_rows_in_input_csv):
        """
        @type input_csv_name: C{str}
        @type connecting_field: C{str}
        @type customer: C{str}
        @type sphere_id: C{str}
        @type input_csv_size: C{long}
        @type num_rows_in_input_csv: C{long}
        """
        super(InputCsvSizeArtifact, self).__init__(customer, sphere_id)
        self.__input_csv_name = input_csv_name
        self.__connecting_field = connecting_field
        self.__sphere_id = sphere_id
        self.__input_csv_size = input_csv_size
        self.__num_rows_in_input_csv = num_rows_in_input_csv

    def _to_dict(self):
        return {
            'sphere_id': self.__sphere_id,
            'input_csv_size': self.__input_csv_size,
            'num_rows_in_input_csv': self.__num_rows_in_input_csv,
            'connecting_field': self.__connecting_field,
            'input_csv_name': self.__input_csv_name,
        }

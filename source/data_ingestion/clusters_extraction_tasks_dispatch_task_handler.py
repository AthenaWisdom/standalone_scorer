import functools
import logging
import uuid

from functional import seq

from source.data_ingestion.clusters_extraction.task import ClustersExtractionTaskGenerator
from source.data_ingestion.clusters_extraction_tasks_dispatch_task import ClustersExtractionTasksDispatchTask
from source.data_ingestion.setup_maker.sphere_setup.connecting_fields.connecting_fields_thresholds_maker import \
    ConnectingFieldsThresholdsMaker
from source.data_ingestion.setup_maker.sphere_setup.params_maker import ParamsMaker
from source.jobnik_communicator.interface import JobnikSession
from source.storage.stores.artifact_store.interface import ArtifactStoreInterface
from source.storage.stores.artifact_store.types import ClustersExtractionExternalJobArtifact
from source.storage.stores.artifact_store.types.common import WarningArtifact
from source.storage.stores.engine_communication_store.interface import EngineCommunicationStoreInterface
from source.storage.stores.engine_communication_store.types import RunPair, InputCSV
from source.storage.stores.preview_store.interface import DataPreviewStoreInterface
from source.task_runner.handler_interface import TaskHandlerInterface
from source.utils.config import ConfigItem, ConfigParser
from source.utils.config_builder import build_sphere_generation_config

TIME_TYPES = {"datetime", "date"}
MAX_INPUT_CSV_SIZE_IN_BYTES = 50 * 1024 * 1024 * 1024


class ClustersExtractionTasksDispatchTaskHandler(TaskHandlerInterface):
    def __init__(self, engine_comm_store, data_preview_store, task_submitter, clusters_extractor_task_generator,
                 artifact_store):
        """
        @type engine_comm_store: L{EngineCommunicationStoreInterface}
        @type data_preview_store: L{DataPreviewStoreInterface}
        @type task_submitter: L{TaskSubmitterInterface}
        @type clusters_extractor_task_generator: L{ClustersExtractionTaskGenerator}
        @type artifact_store: L{ArtifactStoreInterface}
        """
        self.__connecting_fields_maker = ConnectingFieldsThresholdsMaker()
        self.__clusters_extractor_task_generator = clusters_extractor_task_generator
        self.__task_submitter = task_submitter
        self.__engine_comm_store = engine_comm_store
        self.__artifact_store = artifact_store
        self.__data_preview_store = data_preview_store
        self.__logger = logging.getLogger('endor')

    @staticmethod
    def get_task_type():
        return ClustersExtractionTasksDispatchTask

    # noinspection PyTypeChecker
    def generate_run_pairs(self, customer, sphere_id, all_input_csvs, uniques_dict, schema, ignored_fields,
                           main_time_column, main_id_column, setup_maker_config, input_csv_header):
        """
        @type sphere_id: C{str}
        @type customer: C{str}
        @type input_csv_header: C{list}
        @type all_input_csvs: C{list}
        @type uniques_dict: C{dict}
        @type schema: C{list}
        @type ignored_fields: C{list}
        @type main_time_column: C{str}
        @type main_id_column: C{str}
        @type setup_maker_config: L{ConfigItem}

        @rtype: C{list}
        """
        global_config = ConfigItem()
        global_config.main_timestamp_column = main_time_column
        global_config.main_id_field = main_id_column
        self.__connecting_fields_maker.report_result = lambda x, y: 0
        possible_connecting_fields = seq(schema).filter(lambda x: x['name'] != main_time_column)\
            .filter(lambda x: x['name'] not in ignored_fields).to_list()
        time_cols, non_time_cols = seq(possible_connecting_fields).partition(lambda x: x['data_type'] in TIME_TYPES)\
            .map(lambda x: x.map(lambda y: y['name']))
        time_cols = time_cols.flat_map(lambda x: ["%s_rounded_%d" % (x, rounding_minute)
                                                  for rounding_minute in setup_maker_config.rounding_minutes])

        get_input_csv_size = functools.partial(self.__engine_comm_store.get_input_csv_size, customer, sphere_id)
        input_csvs = filter(lambda x: get_input_csv_size(x) < MAX_INPUT_CSV_SIZE_IN_BYTES, all_input_csvs)

        if set(input_csvs) != set(all_input_csvs):
            warning_message = 'The following input CSVs are too big: {}'.format(set(all_input_csvs) - set(input_csvs))
            self.__artifact_store.store_artifact(WarningArtifact(customer, sphere_id, 1600, warning_message))

        connecting_field_from_generator = self.__connecting_fields_maker.run(input_csvs, uniques_dict, time_cols,
                                                                             non_time_cols, global_config,
                                                                             setup_maker_config)
        connecting_fields = map(lambda x: x.replace("_", ""),
                                seq(connecting_field_from_generator)
                                .filter(lambda x: x not in ignored_fields)
                                .sorted()
                                .to_list())

        connecting_fields_not_in_input_csv = set(connecting_fields) - set(input_csv_header)
        if len(connecting_fields_not_in_input_csv):
            raise RuntimeError("The following fields are chosen as connecting and "
                               "do not exist in input CSV: {} "
                               "(Maybe low cardinality)".format(list(connecting_fields_not_in_input_csv)))

        params_maker = ParamsMaker()
        params_gen_func = functools.partial(params_maker.run, global_config, setup_maker_config, input_csv_header)
        run_pairs = seq(input_csvs)\
            .map(lambda x: InputCSV(x, None))\
            .flat_map(lambda input_csv: seq(connecting_fields)
                      .map(lambda field: RunPair(customer, sphere_id, params_gen_func(input_csv.name, field),
                                                 input_csv))
                      )\
            .to_list()
        return run_pairs

    def __run(self, customer, sphere_id, schema, main_time_column, main_id_column, stats_source, setup_maker_config,
              ignored_fields, jobnik_session, feature_flags, finished_tasks_ids=None):
        """
        @type jobnik_session: L{JobnikSession}
        @type finished_tasks_ids: C{set}
        @type customer: C{str}
        @type sphere_id: C{str}
        @type schema: C{list}
        @type main_time_column: C{str}
        @type main_id_column: C{str}
        @type ignored_fields: C{list}
        @type stats_source: L{InputStats}
        @type setup_maker_config: L{ConfigItem}
        @type feature_flags: C{dict}

        @rtype: C{int}
        @return: The amount of run pairs
        """
        finished_tasks_ids = set() if finished_tasks_ids is None else finished_tasks_ids
        uniques_dict = self.__get_uniques_dict(customer, stats_source)
        input_csvs = sorted(self.__engine_comm_store.list_input_csvs(customer, sphere_id))

        if len(input_csvs) == 0:
            self.__artifact_store.store_artifact(WarningArtifact(customer, sphere_id, 1500,
                                                                 'No pairs could be generated - no input csvs found.'))
            return 0

        input_csv_header = self.__engine_comm_store.get_input_csv_header(customer, sphere_id, input_csvs[0])
        if "time" in input_csv_header:
            input_csv_header.remove("time")

        run_pairs = self.generate_run_pairs(customer, sphere_id, input_csvs, uniques_dict, schema, ignored_fields,
                                            main_time_column, main_id_column, setup_maker_config, input_csv_header)

        filtered_run_pairs = seq(enumerate(run_pairs)).filter(lambda x: x[0] not in finished_tasks_ids)\
            .map(lambda x: x[1]).to_list()
        for run_pair in filtered_run_pairs:
            self.__engine_comm_store.save_run_pair(customer, sphere_id, run_pair, False)

        job_id = uuid.uuid4().get_hex() if jobnik_session is None else jobnik_session.job_token['jobId']
        tasks = self.__clusters_extractor_task_generator.get_tasks(job_id, customer, sphere_id, filtered_run_pairs,
                                                                   jobnik_session, feature_flags)
        job_artifact = ClustersExtractionExternalJobArtifact(customer, sphere_id, job_id, len(tasks))
        self.__artifact_store.store_artifact(job_artifact)
        self.__task_submitter.submit_tasks(job_id, 'extract_clusters', tasks)
        return len(filtered_run_pairs)

    def __get_uniques_dict(self, customer, stats_source):
        uniques_dict = self.__data_preview_store.load_uniques_dict(customer, stats_source.id)
        return uniques_dict

    def handle_task(self, task):
        """
        @type task: L{ClustersExtractionTasksDispatchTask}
        """

        # before we know how many tasks there will be we assume there is only 1.
        for field in task.schema:
            if 'data_type' in field and 'type' not in field:
                field['type'] = field['data_type']
            elif 'data_type' not in field and 'type' in field:
                field['data_type'] = field['type']
        sphere_generation_config_dict = build_sphere_generation_config(task.basic_config, task.schema, task.fde_conf)
        sphere_generation_config = ConfigParser.from_dict(sphere_generation_config_dict)
        customer = sphere_generation_config['customer_id'] if 'customer_id' in sphere_generation_config else None
        sphere_id = sphere_generation_config['execution_id'] if 'execution_id' in sphere_generation_config else None
        self.__logger.progress('Run Pairs Generator started')
        num_of_run_pairs = self.__run(customer, sphere_id, task.schema,
                                      sphere_generation_config.global_config.main_timestamp_column,
                                      sphere_generation_config.global_config.main_id_field, task.input_stats,
                                      sphere_generation_config.setup_maker_config,
                                      sphere_generation_config.global_config.ignore_fields.to_builtin(),
                                      task.jobnik_session, task.feature_flags, finished_tasks_ids=set())

        # When sending progress indication, make sure to specify we have more tasks
        task.total_num_tasks = num_of_run_pairs + 1

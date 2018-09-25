def get_fde_changes_to_params_conf(param, fde_config, default_val):
    params_conf = default_val.copy()
    if param in fde_config.keys():
        for param_item, param_val in fde_config[param].iteritems():
            params_conf[param_item] = param_val
    return params_conf


def get_all_time_columns(schema, ignore_fields):
    all_time_cols = []
    for col_props in schema:
        if col_props['type'] == 'datetime':
            all_time_cols.append(col_props['name'])
    return list(set(all_time_cols) - set(ignore_fields))


def get_fields_to_hash(schema, main_id_field, ignore_fields):
    fields_to_hash = {main_id_field}
    for col_props in schema:
        if col_props['type'] == 'string':
            fields_to_hash.add(col_props['name'])
    return list(set(fields_to_hash) - set(ignore_fields))

def get_numeric_fields(schema, ignore_fields):
    numeric_fields = []
    for col_props in schema:
        if col_props['type'] in ['integer', 'float', 'double', 'long', 'decimal']:
            numeric_fields.append(col_props['name'])
    return list(set(numeric_fields)-set(ignore_fields))


def build_sphere_generation_config(basic_config, schema, fde_conf):
    """
    @type basic_config: C{dict}
    @type schema: C{list}
    @type fde_conf: C{dict}
    @return:
    @rtype: C{dict}
    """
    config = {
        'customer_id': basic_config["customer_id"],
        'dataset_id': basic_config['dataset_id'],
        'execution_id': basic_config['execution_id'],
        'clean_config': None,
        'fetcher_config': {
            'delimiter': ',',
            'have_header': True
        },
        'global_config': {
            'ignore_fields': basic_config["ignore_fields"],
            'reference_date': fde_conf.get('reference_date', None),
            'main_id_field': basic_config['legit_field_name'],
            'main_timestamp_column': basic_config['main_time_field'],
            'timestamp_format': basic_config['datetime_format'],
            'calculate_input_csv_stats': fde_conf.get('calculate_input_csv_stats', False),
            'all_time_cols': get_all_time_columns(schema, basic_config["ignore_fields"])},
        'setup_maker_config': {
            'rounding_minutes': fde_conf.get('rounding_minutes', [5, 60]),
            'input_csv_days': fde_conf.get('input_csv_days', [30, 60]),
            'fields_to_hash': fde_conf.get('fields_to_hash', get_fields_to_hash(schema,
                                                                                basic_config['legit_field_name'],
                                                                                basic_config["ignore_fields"])),
            'connecting_fields_config': {
                'max_time_connecting_fields': fde_conf.get('max_time_connecting_fields', 5),
                'fde_connecting_fields': fde_conf.get('connecting_fields', []),
                'non_time_conn_fields_unique_thresh': fde_conf.get('non_time_conn_fields_unique_thresh', 250),
                'max_non_time_connecting_fields': fde_conf.get('max_non_time_connecting_fields', 10),
                'time_to_check': fde_conf.get('informational_time_to_check', 1000),
                'threshold': fde_conf.get('informational_threshold', 0.2)
            },
            'extra_params_legit_fields': fde_conf.get('extra_params_legit_fields', []),
            'basic_params_config': get_fde_changes_to_params_conf("basic_params_config", fde_conf, BASIC_PARAMS_CONF),
            'non_time_fields_unique_thresh': fde_conf.get('non_time_fields_unique_thresh', 100)},
        'stats_config': {
            'numeric_fields': fde_conf.get('numeric_fields', get_numeric_fields(schema,
                                                                                basic_config["ignore_fields"])),
            'unique_threshold': fde_conf.get('unique_threshold_for_freq', 10000)
        }
    }
    return config

BASIC_PARAMS_CONF = {

      "ITER_DIM":"50",
      "ITER_FAC":"50",
      "numDates":"0",
      "numOutputIDs":"500",
      "flag_show_performance":"0",
      "AUC_calculation_strategy":"0",
      "numOfSplitSolutions":"1",
      "prms_maxMinutesPerMatrix1":"20",
      "prms_maxMinutesPerMatrix2":"20",
      "prms_maxMinutesPerMatrix3":"20",
      "prms_useMLinFinalPhase":"1",
      "prms_ML_th1":"4.25",
      "prms_ML_th2":"0.2",
      "prms_ML_MISC":"-1",
      "prms_randSeed":"0",
      "prms_regenerateExistingMatrices":"1",
      "prms_special_sectors":"[]",
      "prms_maximalMemoryInGB":"50",
      "prms_maximalFlops":"200000000000",
      "prms_matrices_sampling_threshold":"1",
      "prms_sampling_threshold":"1",
      "prms_flag_should_we_assess_timeAndMemory":"1",
      "prms_global_time_limit":"360000000",
      "prms_flag_useNewAssessmentFunction":"0",
      "prms_enough_matrices_percent_to_stop":"1",
      "prms_MultSparseMat_Windows_Compatibility_Mode":"0",
      "prms_num_of_CPUs":"0",
      "prms_runPhase2_4":"1",
      "prms_runPhase2_5":"1",
      "prms_runPhase2_6":"1",
      "prms_flag_read_input_file_through_dataset":"1",
      "prms_connectors_perc":"[0.25:0.1:0.75]",
      "prms_nodes_perc":"[0.1:0.1:0.8]",
      "prms_min_block_nodes":"5",
      "prms_num_runs_for_AUC_calculation":"6",
      "prms_should_we_print_ROC":"1"
   }
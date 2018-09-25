import copy
import datetime

from source.storage.stores.general_quest_data_store.types import QueryExecutionUnit
from source.utils.schema import STR_TO_PYTHON_CONVERTERS

__author__ = 'Shahars'


def bind_splitter_definition_to_data(enrichment_def, query_unit, customer_id):
    bound_enrichment = [__bind_source(x, query_unit, customer_id) for x in enrichment_def]
    return bound_enrichment


def bind_kernel_definition_to_data(kernel_definition_dict, query_unit, customer_id, is_past):
    try:
        if is_past:
            random_thresh = kernel_definition_dict["past_random_threshold"]
        else:
            random_thresh = kernel_definition_dict["present_random_threshold"]
    except KeyError:
        random_thresh = None

    bound_external_columns = [__bind_source(x, query_unit, customer_id) for x in
                              kernel_definition_dict['external_columns']]

    kernel_mode = kernel_definition_dict.get('mode', 'queryBuilder')
    default_validation_mode = 'cross-validation' if kernel_mode == 'openQuery' else 'use_past'

    if kernel_mode == 'sql':
        bound_whites, bound_universe, bound_ground = [None] * 3
    else:
        bound_whites = __bind_complete_selector(kernel_definition_dict['whites'], query_unit,
                                                customer_id, random_thresh)
        bound_universe = __bind_complete_selector(kernel_definition_dict['universe'], query_unit,
                                                  customer_id, None)
        bound_ground = __bind_complete_selector(kernel_definition_dict['ground'], query_unit,
                                                customer_id, random_thresh)
    try:
        raise_on_error = bool(int(kernel_definition_dict['raise_on_count_error']))
    except KeyError:
        raise_on_error = True
    return {
        'mode': kernel_mode,
        'validation_type': kernel_definition_dict.get('validation_type', default_validation_mode),
        'sql': kernel_definition_dict.get('sql', {}),
        'refernce_date': query_unit.timestamp,
        'raise_on_count_error': raise_on_error,
        "external_columns": bound_external_columns,
        "whites": bound_whites,
        "universe": bound_universe,
        # ground can be an empty selector.
        "ground": bound_ground
    }


def bind_single_population(population_config_dict, query_unit, customer_id):
    """
    @type population_config_dict: C{dict}
    @type query_unit: L{QueryExecutionUnit}
    @type customer_id: C{str}
    """
    return __bind_complete_selector(population_config_dict, query_unit, customer_id, None)


def __bind_source(source, query_unit, customer_id):
    bound_source = copy.deepcopy(source)
    bound_source['customer_id'] = customer_id
    if source['type'] in ('transactional', 'meta', 'internal', 'external'):
        try:
            batch_id = filter(lambda x: x.customer_id == customer_id and x.dataset_id == source['dataset_id'],
                              query_unit.batches).pop().batch_id
        except IndexError:
            raise ValueError('Batch data for dataset {} not found '
                             'in query execution unit'.format(source['dataset_id']))
        bound_source['batch_id'] = batch_id
    elif source['type'] == 'stats':
        bound_source['sphere_id'] = query_unit.stats_sphere_id
    # TODO (open query): add internal
    else:
        raise ValueError('unsupported source type: {}'.format(source['type']))
    return bound_source


def __bind_complete_selector(complete_selector, query_unit, customer_id, random_thresh):
        if len(complete_selector) == 0:
            return {}
        positives = []
        for x in complete_selector['positive']:
            positives.append({
                'source': __bind_source(x['source'], query_unit, customer_id),
                'apply_funcs': [__bind_function(func_def, x['source'], query_unit, random_thresh) for func_def in
                                x['apply_funcs']]
            })
        negatives = []
        for x in complete_selector['negative']:
            negatives.append({
                'source': __bind_source(x['source'], query_unit, customer_id),
                'apply_funcs': [__bind_function(func_def, x['source'], query_unit, random_thresh) for func_def in
                                x['apply_funcs']]
            })
        return {
            'positive': positives,
            'negative': negatives
        }



def __bind_function(source_user_selection_def, source, query_unit, random_threshold):
    # if source_user_selection_def['calc_only_active_users']:
    #     return source_user_selection_def

    copied = copy.deepcopy(source_user_selection_def)
    copied = __typify_literal(query_unit, copied, source)
    copied = __handle_backward_compatability(copied)
    try:
        seed = query_unit.seed
        copied = __handle_random_selection(copied, seed, random_threshold)
    except Exception:
        raise
    return copied


def __handle_backward_compatability(source_user_selection_def):
    copied = copy.deepcopy(source_user_selection_def)
    if "where_clause" not in copied:
        copied["where_clause"] = None
    return copied

def __handle_random_selection(func_def, seed, random_threshold):
    func_def.setdefault("agg_params", {"seed": seed})
    if func_def["aggregate_by"] == "random":
        func_def["agg_params"].update({"seed": seed})
        func_def["value_to_compare"] = float(random_threshold)
    return func_def

# current
def __typify_literal(query_unit, source_user_selection_def, source):
    if source_user_selection_def['calc_only_active_users']:
        return source_user_selection_def

    col_name = source_user_selection_def['column']
    val_str = source_user_selection_def['value_to_compare']
    copied = copy.deepcopy(source_user_selection_def)

    source_dataset_def = source['data_schema']
    column_type = next(
        x['type'] for x in source_dataset_def['schema'] if x['name'] == col_name)

    # TODO(shahar): make sure both cases pass for multiple types.
    if (copied['filter_by'] == 'isin') or (copied['filter_by'] == 'between'):
        copied['value_to_compare'] = [__typify_value_to_compare(query_unit, column_type, x.strip())
                                      for x in val_str.split(',')]
    else:
        copied['value_to_compare'] = __typify_value_to_compare(query_unit, column_type, val_str)
    return copied


def __typify_value_to_compare(query_unit, column_type, val_str):
    try:

        if column_type in ['date', 'datetime']:
            int_val = int(val_str)
            return query_unit.timestamp + datetime.timedelta(days=int_val)

    except ValueError:
        pass
    return STR_TO_PYTHON_CONVERTERS[column_type](val_str)

# def __new_typify_literal(func_def, source_def):
#     col_name = func_def['column']
#     val_str = func_def['value_to_compare']
#     source_dataset_def = source_def['data_schema']
#     column_type = next(
#         x['type'] for x in source_dataset_def['schema'] if x['name'] == col_name)
#
#     #TODO(shahar): make sure both cases pass for multiple types.
#     if func_def['filter_by'] == 'isin':
#         func_def['value_to_compare'] = [STR_TO_PYTHON_CONVERTERS[column_type](x.strip()) for x in val_str.split(',')]
#     else:
#         func_def['value_to_compare'] = STR_TO_PYTHON_CONVERTERS[column_type](val_str)
#     return func_def

# def __typify_literal(source_user_selection_def, source, query_unit):
#     if source_user_selection_def['calc_only_active_users']:
#         return source_user_selection_def
#
#     col_name = source_user_selection_def['column']
#     val_str = source_user_selection_def['value_to_compare']
#     copied = copy.deepcopy(source_user_selection_def)
#
#     source_dataset_def = source['data_schema']
#     column_type = next(
#         x['type'] for x in source_dataset_def['schema'] if x['name'] == col_name)
#
#     #TODO(shahar): make sure both cases pass for multiple types.
#     if copied['filter_by'] == 'isin':
#         copied['value_to_compare'] = [STR_TO_PYTHON_CONVERTERS[column_type](x.strip()) for x in val_str.split(',')]
#     else:
#         copied['value_to_compare'] = STR_TO_PYTHON_CONVERTERS[column_type](val_str)
#     return copied

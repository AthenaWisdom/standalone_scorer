import StringIO

from source.storage.stores.engine_communication_store.types import Params

class ParamsMaker(object):
    def run(self, global_sphere_config, setup_maker_config, fields, input_file_name, connecting_field):
        params_config = setup_maker_config.basic_params_config.to_builtin()
        format_str = "%f" * (len(fields) + 1)
        params_config['format'] = format_str
        params_config['input_fields_for_engine'] = [field.replace('_', '') for field in fields]
        all_legit_fields = setup_maker_config.extra_params_legit_fields.to_builtin() + \
                           [global_sphere_config.main_id_field]
        params_config['prms_legitFieldsForSuspects'] = "|".join([legit_field.replace('_', '') for
                                                                 legit_field in all_legit_fields])
        params_config['filename'] = input_file_name
        params_config['prms_connect_field'] = connecting_field.replace('_', '')
        res = json_to_txt(params_config)
        return Params(connecting_field, res)


def json_to_txt(obj_dict):
    params_txt = StringIO.StringIO()
    # Header line
    __write_param(params_txt, "parameter", "value")

    # Specification of fields in input.csv.
    format = obj_dict.pop("format")
    __write_param(params_txt, "\nformat", format + "\n")
    fields_for_engine = obj_dict.pop("input_fields_for_engine")
    fields_num = len(fields_for_engine)
    __write_param(params_txt, "fieldsNum", str(fields_num))
    for i in range(fields_num):
        __write_param(params_txt, "field" + str(i + 1), fields_for_engine[i])
    params_txt.write("\n")

    # All other parameters
    for param_name, param_value in sorted(obj_dict.iteritems()):
        if type(param_value) == bool:
            __write_param(params_txt, param_name, int(param_value))
        else:
            __write_param(params_txt, param_name, param_value)
    return params_txt.getvalue()


def __write_param(strm, param_name, param_value):
    """
    Writes a parameter to a text ouput stream.
    @param strm: The stream.
    @type strm: C{File Object}
    @param param_name: As implied.
    @type param_name: C{str}
    @param param_value: As implied.
    @type param_value: C{object}
    """
    strm.write(param_name + "\t" + str(param_value) + "\n")
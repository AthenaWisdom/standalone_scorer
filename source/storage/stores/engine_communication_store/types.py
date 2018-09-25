class InputCSV(object):
    def __init__(self, name, dataframe):
        """
        @type name: C{str}
        @type dataframe: C{DataFrame}
        """
        self.__dataframe = dataframe
        self.__name = name

    @property
    def name(self):
        return self.__name

    @property
    def df(self):
        return self.__dataframe


class Params(object):
    def __init__(self, connecting_field, data):
        """
        @type connecting_field: C{str}
        @type data: C{str}
        """
        self.__data = data
        self.__connecting_field = connecting_field

    @property
    def data(self):
        return self.__data

    @property
    def connecting_field(self):
        return self.__connecting_field


class RunPair(object):
    def __init__(self, customer, sphere_id, params, input_csv):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        @type params: L{Params}
        @type input_csv: L{InputCSV}
        """
        self.__sphere_id = sphere_id
        self.__customer = customer
        self.__input_csv = input_csv
        self.__params = params

    @property
    def input_csv(self):
        return self.__input_csv

    @property
    def params(self):
        return self.__params

    @property
    def sphere_id(self):
        return self.__sphere_id

    @property
    def customer(self):
        return self.__customer

    @property
    def name(self):
        return '{}_{}'.format(self.input_csv.name, self.params.connecting_field)

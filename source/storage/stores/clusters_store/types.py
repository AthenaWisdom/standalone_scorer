from pandas import DataFrame
from scipy.sparse import csr_matrix
from numpy import ndarray


class ClustersMetaData(object):
    def __init__(self, customer, sphere_id):
        """
        @type customer: C{str}
        @type sphere_id: C{str}
        """
        self.__sphere_id = sphere_id
        self.__customer = customer

    @property
    def customer(self):
        return self.__customer

    @property
    def id(self):
        return self.__sphere_id

    def __str__(self):
        return "ClustersMetaData({}, {})".format(self.__customer, self.__sphere_id)


class Clusters(object):
    def __init__(self, metadata, clusters_properties, population_to_clusters_matrix, population_ids):
        """
        @type metadata: L{ClustersMetaData}
        @type clusters_properties: C{DataFrame}
        @type population_to_clusters_matrix: C{csr_matrix}
        @type population_ids: C{ndarray}
        """
        self.__population_ids = population_ids
        self.__population_to_clusters_matrix = population_to_clusters_matrix
        self.__clusters_properties = clusters_properties
        self.__metadata = metadata

    @property
    def metadata(self):
        return self.__metadata

    @property
    def population_to_clusters_matrix(self):
        return self.__population_to_clusters_matrix

    @property
    def clusters_properties(self):
        return self.__clusters_properties

    @property
    def population_ids(self):
        return self.__population_ids

# -*- coding: utf-8 -*-
"""
Created on Sun Feb 08 18:19:06 2015

@author: Admin
"""
import os

import numpy as np
import pandas as pd

########################################################################################################################################
#
# Reading mat file methods
#
########################################################################################################################################

"""
Returns a DataFrame containing properties of a subset of the clusters, which apply to the listed conditions.
"""


def get_cluster_prop_subset_by_cond(clusters_props_df, req_list):

    if len(req_list) == 0:
        print "no cond"
        return clusters_props_df

    all_selectors = __get_selectors(clusters_props_df, req_list)
    combine_selectors = lambda selector1, selector2: (selector1 & selector2)
    selectors = reduce(combine_selectors, all_selectors)
    extracted = clusters_props_df[selectors]

    return extracted


def __create_cond(df, req_dict):

    col_name = req_dict["property"]
    val = req_dict["value"]
    operator = req_dict["operator"]
    t = type(df[col_name].values[0])
    return operator(df[col_name], t(val))


def __get_selectors(df, req_list):
    return [__create_cond(df, req_dict) for req_dict in req_list]
    

def get_people_dict_from_cluster(cluster_dict):

    pop_to_clust_dict = {}
    for cluster, pop in cluster_dict.iteritems():
        for p in pop:

            pop_to_clust_dict[p] = pop_to_clust_dict.get(p, [])
            pop_to_clust_dict[p].append(cluster)

    return pop_to_clust_dict


"""
expecting to receive already flattened ndarray (not singletones)
"""


def get_clust_to_pop_dict(expanded_clusters_data, bag_indices):
    clust_to_pop_dict = {}
    for clust_ind in bag_indices:
        clust_ind_data = expanded_clusters_data[clust_ind]
        clust_to_pop_dict[clust_ind] = np.concatenate((clust_ind_data[0], clust_ind_data[1]), axis=1)
    return clust_to_pop_dict


def get_new_GT_W_lists_by_whites_partition(OrigWhites, partitions_num):
    new_whites_list = [None]*partitions_num
    new_GT_list = [None]*partitions_num
    for i in range(partitions_num):
        copyWhites = OrigWhites.copy()
        np.random.shuffle(copyWhites)
        partition_pnt = np.ceil(len(copyWhites)/2.0)
        new_whites_list[i] = copyWhites[:partition_pnt]
        new_GT_list[i] = copyWhites[partition_pnt:]
    return new_whites_list, new_GT_list


def modify_one_cluster(x,new_whites):
    W = np.intersect1d(x[0], new_whites, True)
    G = np.union1d(x[1], np.setdiff1d(x[0], new_whites))
    B = x[2]
    return np.array([W, G, B])
    

def flatten_mat_file(clusters_data):
    return np.array([[x.flatten() for x in clst[0]] for clst in clusters_data])
    

"""
Receives a dictionary specifying property names and their mat structure.
Calculates random distribution of whites for each cluster.
Returns dataframe containing clusters properties.
"""


def _get_input_data(input_path, data_name):

    try:
        data_df = pd.read_csv(os.path.join(input_path, data_name), header=None)
    except IOError:
        raise IOError("File %s doesn't exist in %s path" % (data_name, input_path))
    except ValueError:
        raise ValueError("File %s has bad formatting, check if empty" % data_name)
    if len(data_df.columns) == 2:

        data_df.columns = ["ID", "priority"]
    elif len(data_df.columns) == 1:
        data_df.columns = ["ID"]
    else:
        raise ValueError("File %s has no columns" % data_name)


    return data_df["ID"].values


def get_ground_truth(data_path, name):
    return _get_input_data(data_path, name)


def get_universe(data_path, name):
    return _get_input_data(data_path, name)


def get_whites(data_path, name):
    return _get_input_data(data_path, name)
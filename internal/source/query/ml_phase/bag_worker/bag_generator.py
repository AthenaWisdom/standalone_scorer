import logging

from bag import Bag

EXTERNAL_DATA_NAME = "external_data.csv"


class BagGenerator(object):

    def __init__(self):
        self.__logger = logging.getLogger('endor')

    def create_new_bag(self, task, is_past_mode, universe, whites, all_props_df, pop_clust_map, bag_pop, external_data):

        success = True

        self.__logger.info("finished loading data from mat file")
        self.__logger.debug('loaded {0} clusters'.format(len(all_props_df.index.values)))
        # bag_name = config.curr_bag.name

        self.__logger.info("start generating bag")
        bag = Bag(self.__logger, "curr", all_props_df, pop_clust_map, bag_pop, whites, universe, external_data)

        self.__logger.info("finished generating %s bag" % "curr")

        universe_in_bag = len(bag.universe_in_bag)
        whites_in_bag = len(bag.whites_in_bag)
        running_mode_error = "past" if is_past_mode else "present"
        if universe_in_bag == 0 or whites_in_bag == 0:

            error_msg = "clusters in %s mode contains %d universe in clusters, and %d whites" \
                        % (running_mode_error, universe_in_bag, whites_in_bag)
            if task.sub_kernel_id == 0 and not is_past_mode:
                self.__logger.error(error_msg + ' In query 0.')
                raise ValueError(error_msg + ' In query 0.')
            else:
                self.__logger.warning(error_msg)
            success = False

        return bag, success

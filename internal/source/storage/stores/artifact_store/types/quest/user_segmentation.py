import re

from source.storage.stores.artifact_store.types.quest.base_artifact import UserSegmentationBaseArtifact

__all__ = [
    'UserSegmentationResults'
]


USER_PROFILE_NAME_REGEX = re.compile('(?P<dataset>\w+?)_(UserProfileOutput_((?P<activity>activityCount)|((?P<agg>\w+?)_(?P<agg_col>\w+))))')
FIELD_NAME_REGEX = re.compile('(?P<dataset>\w+?)___(?P<col>\w+)')


def get_ds_agg_field_from_match(match):
    if match.get('activity') is not None:
        return match['dataset'], 'activity', None
    if match.get('agg') is not None:
        return match['dataset'], match['agg'], match['agg_col']
    if match.get('col') is not None:
        return match['dataset'], None, match['col']


class UserSegmentationResults(UserSegmentationBaseArtifact):
    type = 'profiling_summary'
    # TODO(izik): exec_id to summary_id/seg_id

    def __init__(self, customer, user_segmentation_id, quest_id, significance_results):
        super(UserSegmentationResults, self).__init__(customer, user_segmentation_id)
        self.__significance_results = significance_results
        self.__quest_id = quest_id
        self.__user_segmentation_id = user_segmentation_id
        self.__customer = customer

    def _to_dict(self):
        return {
            'customer': self.__customer,
            'user_segmentation_id': self.__user_segmentation_id,
            'quest_id': self.__quest_id,
            'significance_results': self.__significance_results
        }

    @property
    def quest_id(self):
        return self.__quest_id

    @property
    def user_segmentation_id(self):
        return self.__user_segmentation_id

    @property
    def customer(self):
        return self.__customer

    @property
    def significance_results(self):
        return self.__significance_results


class PieFriendlyArtifact(UserSegmentationBaseArtifact):
    type = 'pie_friendly_artifact'

    def __init__(self, customer, user_segmentation_id, quest_id, group_name, field, segments):
        """
        @type customer: C{str}
        @type user_segmentation_id: C{str}
        @type quest_id: C{str}
        @type group_name: C{str}
        @type field: C{str}
        @type segments: C{list}
        """
        super(PieFriendlyArtifact, self).__init__(customer, user_segmentation_id)
        self.__segments = segments
        self.__field = field
        if 'UserProfileOutput' in field:
            match = USER_PROFILE_NAME_REGEX.match(field)
        else:
            match = FIELD_NAME_REGEX.match(field)

        if match is None:
            self.__dataset = None
            self.__agg = None
            self.__col = field
        else:
            self.__dataset, self.__agg, self.__col = get_ds_agg_field_from_match(match.groupdict())

        self.__group_name = group_name
        self.__quest_id = quest_id
        self.__user_segmentation_id = user_segmentation_id
        self.__customer = customer

    def _to_dict(self):
        return {
            'customer': self.__customer,
            'user_segmentation_id': self.__user_segmentation_id,
            'quest_id': self.__quest_id,
            'group_name': self.__group_name,
            'segments': self.__segments,
            'field': self.__field,
            'dataset': self.__dataset,
            'aggregation': self.__agg,
            'column': self.__col,
        }

    @property
    def segments(self):
        return self.__segments

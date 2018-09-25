import json
import os

__author__ = 'izik'


class TaskFetcher(object):
    def fetch_tasks(self):
        raise NotImplementedError()

class MetaFilesTaskFetcher(TaskFetcher):
    def __init__(self, meta_folder):
        self.meta_folder = meta_folder

    def fetch_tasks(self):
        triplets_uri = os.path.join(self.meta_folder, 'merger_data.json')
        with open(triplets_uri) as f:
            owned_triplet = json.load(f)

        variant_dict = owned_triplet['variant']
        return [(owned_triplet['scorer_id'], owned_triplet['merger_model'], variant_dict)]
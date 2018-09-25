import os
from source.utils.config import ConfigParser

__author__ = 'Shahars'


class ConfigGetter(object):
    def get_config(self):
        raise NotImplementedError()


class UriConfigGetter(ConfigGetter):
    def __init__(self, meta_folder):
        self.meta_folder = meta_folder

    def get_config(self):
        config_path = os.path.join(self.meta_folder, 'ml_conf.json')
        config = ConfigParser.from_file(config_path)
        return config

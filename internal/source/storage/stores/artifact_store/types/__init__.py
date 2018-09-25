from interface import ArtifactInterface
from quest import *
from common import *
from data_gateway import *
from data_ingestion import *


def all_subclasses(cls):
    return cls.__subclasses__() + [g for s in cls.__subclasses__() for g in all_subclasses(s)]


# noinspection PyTypeChecker
TYPES_TO_CLASSES = {artifact_class.type: artifact_class for artifact_class in all_subclasses(ArtifactInterface)}

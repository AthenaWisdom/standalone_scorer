"""
Utils function.
"""

import logging


def quiet_py4j():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.INFO)

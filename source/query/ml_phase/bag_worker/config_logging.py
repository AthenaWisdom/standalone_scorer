# -*- coding: utf-8 -*-
"""
Created on Wed Feb 11 09:34:06 2015

@author: Admin
"""
import logging
import sys

def add_logging_handler(formatter, handler, name):
    
    logger = logging.getLogger(name)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def config_logging(logging_dict, logging_format):
    name = logging_dict["name"]
    logger = logging.getLogger(name)
    
    formatter = logging.Formatter(logging_format)
    
    add_logging_handler(formatter, logging.StreamHandler(), name)
    logger.setLevel(logging_dict["logging_level"])
   
    add_logging_handler(formatter, logging.FileHandler(logging_dict["name"]), name)
    return logger


def get_logging_data(logger_data):
    logging_levels_dict = {"INFO": logging.INFO,
                           "DEBUG": logging.DEBUG,
                           "WARNING": logging.WARNING,
                           "ERROR": logging.ERROR,
                           "CRITICAL": logging.CRITICAL
                           }
    logger_path = logger_data.get("path")
    logging_level = logger_data.get("level")
    if logging_level not in logging_levels_dict.keys():
        sys.stdout.write("logging level %s doesn't exist, choosing default INFO\n" % logging_level)
        logging_level = "INFO"

    logging_dict = {"name": logger_path,
                    "logging_level": logging_level}
    return logging_dict


def get_logger(logger_data, logging_format):
    logging_dict = get_logging_data(logger_data)
    return config_logging(logging_dict, logging_format)
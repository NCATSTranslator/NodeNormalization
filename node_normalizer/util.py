import logging
import json
import os
import yaml
from collections import namedtuple
import copy
from logging.config import dictConfig
from logging.handlers import RotatingFileHandler
from fastapi.logger import logger as fastapi_logger

# Some constants.
BIOLINK_NAMED_THING = "biolink:NamedThing"

def get_numerical_curie_suffix(curie):
    """
    If a CURIE has a numerical suffix, return it as an integer. Otherwise return None.
    :param curie: A CURIE.
    :return: An integer if the CURIE suffix is castable to int, otherwise None.
    """
    curie_parts = curie.split(":", 1)
    if len(curie_parts) > 0:
        # Try to cast the CURIE suffix to an integer. If we get a ValueError, don't worry about it.
        try:
            return int(curie_parts[1])
        except ValueError:
            pass
    return None

# loggers = {}
class LoggingUtil(object):
    """ Logging utility controlling format and setting initial logging level """
    @staticmethod
    def init_logging(log_file_path=None, log_file_level=None):
        # If log_file_path is set, we use that. Otherwise, we use the LOG_LEVEL environmental variable.
        if not log_file_level:
            log_file_level = os.getenv("LOG_LEVEL", "INFO")

        dictConfig({
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {"default": {"format": "%(asctime)s | %(levelname)s | %(module)s:%(funcName)s | %(message)s"}},
            "handlers": {
                "console": {"level": log_file_level, "class": "logging.StreamHandler", "formatter": "default"},
            },
            "loggers": {
                "node-norm": {"handlers": ["console"], "level": log_file_level},
            },
        })
        # add gunicorn handlers and configure fastapi loggers
        logger = logging.getLogger("node-norm")
        gunicorn_error_logger = logging.getLogger("gunicorn.error")
        gunicorn_logger = logging.getLogger("gunicorn")
        uvicorn_access_logger = logging.getLogger("uvicorn.access")
        uvicorn_access_logger.handlers = gunicorn_error_logger.handlers
        fastapi_logger.handlers = gunicorn_error_logger.handlers
        fastapi_logger.setLevel(gunicorn_logger.level)
        logger.handlers += gunicorn_logger.handlers
        # if there was a file path passed in use it
        if log_file_path is not None:
            # create a rotating file handler, 100mb max per file with a max number of 10 files
            file_handler = RotatingFileHandler(filename=log_file_path + 'nn.log', maxBytes=1000000, backupCount=10)

            # set the formatter
            formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(module)s:%(funcName)s | %(message)s")
            file_handler.setFormatter(formatter)

            # set the log level
            file_handler.setLevel(log_file_level)

            # add the handler to the logger
            logger.addHandler(file_handler)

        # return to the caller
        return logger


class Munge(object):
    @staticmethod
    def gene(gene):
        return gene.split("/")[-1:][0] if gene.startswith("http://") else gene
    

class Text:
    """ Utilities for processing text. """

    @staticmethod
    def get_curie(text):
        # Assume it's a string
        return text.upper().split(':', 1)[0] if ':' in text else None

    @staticmethod
    def un_curie(text):
        return ':'.join(text.split(':', 1)[1:]) if ':' in text else text
        
    @staticmethod
    def short(obj, limit=80):
        text = str(obj) if obj else None
        return (text[:min(len(text), limit)] + ('...' if len(text) > limit else '')) if text else None

    @staticmethod
    def path_last(text):
        return text.split('/')[-1:][0] if '/' in text else text

    @staticmethod
    def obo_to_curie(text):
        return ':'.join(text.split('/')[-1].split('_'))

    @staticmethod
    def opt_to_curie(text):
        if text is None:
            return None
        if text.startswith('http://purl.obolibrary.org') or text.startswith('http://www.orpha.net'):
            return ':'.join(text.split('/')[-1].split('_'))
        if text.startswith('http://linkedlifedata.com/resource/umls'):
            return f'UMLS:{text.split("/")[-1]}'
        if text.startswith('http://identifiers.org/'):
            p = text.split("/")
            return f'{p[-2].upper()}:{p[-1]}'
        return text

    @staticmethod
    def curie_to_obo(text):
        x = text.split(':')
        return f'<http://purl.obolibrary.org/obo/{x[0]}_{x[1]}>'

    @staticmethod
    def snakify(text):
        decomma = '_'.join(text.split(','))
        dedash = '_'.join(decomma.split('-'))
        resu = '_'.join(dedash.split())
        return resu

    @staticmethod
    def upper_curie(text):
        if ':' not in text:
            return text
        p = text.split(':', 1)
        return f'{p[0].upper()}:{p[1]}'


class Resource:
    @staticmethod
    def get_resource_path(resource_name):
        """ Given a string resolve it to a module relative file path unless it is already an absolute path. """
        resource_path = resource_name
        if not resource_path.startswith(os.sep):
            resource_path = os.path.join(os.path.dirname(__file__), resource_path)
        return resource_path

    @staticmethod
    def load_json(path):
        result = None
        with open(path, 'r') as stream:
            result = json.loads(stream.read())
        return result

    @staticmethod
    def load_yaml(path):
        result = None
        with open(path, 'r') as stream:
            result = yaml.load(stream.read())
        return result

    @staticmethod
    def get_resource_obj(resource_name, format='json'):
        result = None
        path = Resource.get_resource_path(resource_name)
        if os.path.exists(path):
            m = {
                'json': Resource.load_json,
                'yaml': Resource.load_yaml
            }
            if format in m:
                result = m[format](path)
        return result

    @staticmethod
    # Modified from:
    # Copyright Ferry Boender, released under the MIT license.
    def deepupdate(target, src, overwrite_keys=[]):
        """Deep update target dict with src
        For each k,v in src: if k doesn't exist in target, it is deep copied from
        src to target. Otherwise, if v is a list, target[k] is extended with
        src[k]. If v is a set, target[k] is updated with v, If v is a dict,
        recursively deep-update it.

        Updated to deal with yaml structure: if you have a list of yaml dicts,
        want to merge them by "name"

        If there are particular keys you want to overwrite instead of merge, send in overwrite_keys
        """
        if type(src) == dict:
            for k, v in src.items():
                if k in overwrite_keys:
                    target[k] = copy.deepcopy(v)
                elif type(v) == list:
                    if k not in target:
                        target[k] = copy.deepcopy(v)
                    elif type(v[0]) == dict:
                        Resource.deepupdate(target[k], v, overwrite_keys)
                    else:
                        target[k].extend(v)
                elif type(v) == dict:
                    if k not in target:
                        target[k] = copy.deepcopy(v)
                    else:
                        Resource.deepupdate(target[k], v, overwrite_keys)
                elif type(v) == set:
                    if k not in target:
                        target[k] = v.copy()
                    else:
                        target[k].update(v.copy())
                else:
                    target[k] = copy.copy(v)
        else:
            # src is a list of dicts, target is a list of dicts, want to merge by name (yikes)
            src_elements = {x['name']: x for x in src}
            target_elements = {x['name']: x for x in target}
            for name in src_elements:
                if name in target_elements:
                    Resource.deepupdate(target_elements[name], src_elements[name], overwrite_keys)
                else:
                    target.append(src_elements[name])


class DataStructure:
    @staticmethod
    def to_named_tuple(type_name, d):
        return namedtuple(type_name, d.keys())(**d)


def uniquify_list(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]
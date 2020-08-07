"""Global marshal/unmarshal methods
Temporary replacement for type-polymorphic dump method.
so that model classes are not changed."""
import time
import os
import logging
from datetime import datetime, timedelta
from multiprocessing import Process, Queue
from queue import Empty as EmptyQueueException
import tornado.ioloop
import tornado.web
from prometheus_client import Gauge, generate_latest, REGISTRY
from prometheus_api_client import PrometheusConnect, Metric
from configuration import Configuration
import schedule
import sys
import pickle
import uuid
import itertools
import model as model_prophet
import json
import hashlib


logger = logging.getLogger(__name__)

def dumps_model(model):    
    if isinstance(model, model_prophet.MetricPredictor):
        logger.debug("Marshal {module}.{cls} object {oiu}".format(module=model_prophet.__name__, cls=model_prophet.MetricPredictor.__name__, oiu=id(model)))
        try:
            data = model.dumps()
            return data
        except Exceptions as e:
            raise e
    else:
        raise NotImplementedError("Unknown model type cannot be marshalled")


def loads_model(cls, data):
    if issubclass(cls, model_prophet.MetricPredictor):
        try:
            obj = model_prophet.MetricPredictor.loads(data)
            logger.debug("Unmarshal as {module}.{cls} object {oiu}".format(module=model_prophet.__name__, cls=model_prophet.MetricPredictor.__name__, oiu=id(obj)))
            return obj
        except Exception as e:
            raise e
    else:
        raise NotImplementedError("Unknown model type cannot be unmarshalled")


def dump_model_list(l):
    logger.info("Marshalling model list")
    x = dict(zip(itertools.starmap(uuid.uuid4, itertools.repeat([])), l))
    x_txt = dict()

    for (key, (metric, predictor_model)) in x.items():
        obj = predictor_model
        cls_name = None
        if isinstance(obj, model_prophet.MetricPredictor):
            cls_name = "prophet"
        else:
            raise NotImplementedError("Unknown model type cannot be identified")

        data = dumps_model(obj)
        x_txt[str(key)] = {
            "name": "{0}.p".format(key.hex),
            "class": cls_name,
            "size": len(data),
            "md5": hashlib.md5(data).hexdigest(),
            "metric": {
                "metric_name": metric.metric_name,
                "label_config": metric.label_config,
            },            
        }
        # saving marshaled data instead of the object
        x[key] = data
    
    try:
        with open("predictor_model_list.json", "w") as f:
            data = json.dumps(list(x_txt.values()))
            f.write(data)
    except Exception as e:
        raise e

    for (key, value) in x.items():
        fname = x_txt[str(key)]["name"]
        data = value
        try:
            with open(fname, "wb") as f:
                f.write(data)
        except Exception as e:
            raise e

"""
returns: List[(MetricInfo, Predictor)]
where MetricInfo is dict with metric_name and label_config attributes from Prometheus Metric object
"""
def load_model_list():
    x = list()
    x_list = list()
    try:
        with open("predictor_model_list.json", "r") as f:
            data = f.read()
            x_list = json.loads(data)
    except Exception as e:
        raise e
    
    for value in x_list:
        fname = value["name"]
        fsize = value["size"]
        cls_name = value["class"]
        md5 = value["md5"]
        metric_name = value["metric"]["metric_name"]
        label_config = value["metric"]["label_config"]
        cls = None

        try:
            with open(fname, "rb") as f:
                data = f.read(fsize)
        except Exception as e:
            raise e

        if hashlib.md5(data).hexdigest() != md5:
            raise Exception("checksum does not match")

        if cls_name == "prophet":
            cls = model_prophet.MetricPredictor
        else:
            raise NotImplementedError("Model class cannot be mapped to serializer")

        model = loads_model(cls, data)
        metric = {"metric_name": metric_name, "label_config": label_config}
        if model is None:
            raise Exception
        x.append((metric, model))

    logger.info("successfully unmarshalled model list")
    return x

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
import redis


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


def dump_model_list(l, r=None):
    if r is None:
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        r = redis.Redis(connection_pool=pool)
    
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
            "name": "{0}".format(key.hex),
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
    
    pipe = r.pipeline()
    for (key, value) in x_txt.items():
        pipe.set('manifest:{key}'.format(key=key), json.dumps(value))
    # pipe.execute()

    # pipe = r.pipeline()
    for (key, value) in x.items():
        h = x_txt[str(key)]["name"]
        pipe.set('model:{key}'.format(key=h), value)
    pipe.execute()
    
    for (key, value) in x_txt.items():
        r.publish('manifest:updates', key)        
    

"""
returns: List[(MetricInfo, Predictor)]
where MetricInfo is dict with metric_name and label_config attributes from Prometheus Metric object
"""
def load_model_list(r=None):
    
    if r is None:
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        r = redis.Redis(connection_pool=pool)
        
    x = list()
    x_list = list()
    
    pipe = r.pipeline()
    for key in r.scan_iter("manifest:*"):
        pipe.get(key)
    res = pipe.execute()    
    
    x_list = list(map(json.loads, res))
    
    for value in x_list:
        h = value["name"]
        fsize = value["size"]
        cls_name = value["class"]
        md5 = value["md5"]
        metric_name = value["metric"]["metric_name"]
        label_config = value["metric"]["label_config"]
        cls = None

        data = r.get('model:{key}'.format(key=h))

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

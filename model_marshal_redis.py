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
import re
from packaging import version
from label_hash import ts_hash


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
    now = datetime.now()
    timestamp = time.mktime(now.timetuple())
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
            "timestamp": timestamp,
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
def load_model_list(keys=None, r=None, hash_include=None):
    """[summary]

    Args:
        keys ([type], optional): [description]. Defaults to None.
        r ([type], optional): [description]. Defaults to None.

    Returns:
        [type]: [description]
    """
    if r is None:
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        r = redis.Redis(connection_pool=pool)
        
    x = list()
    
    # pipe = r.pipeline()
    if keys is None:
        keys = r.scan_iter("manifest:*")
        pattern = re.compile("manifest:(.*)")
        keys = list(map(lambda b_key: pattern.search(b_key.decode("utf-8")).group(1).strip(), keys))
        
    for key in keys:
        # original_key=key.decode("utf-8")
        # pattern = re.compile("manifest:(.*)")
        # ms = pattern.search(key)
        # item_key = ms.group(1).strip()
        manifest, model = load_model(key, r, hash_include=hash_include)
        if model is None:
            contine
        metric_name = manifest["metric"]["metric_name"]
        label_config = manifest["metric"]["label_config"]
        timestamp =  datetime.fromtimestamp(manifest.get("timestamp", 0))
        metric = {"metric_name": metric_name, "label_config": label_config, "timestamp": timestamp}
        x.append((key, metric, model))
    return x
        

def load_model(key, r, hash_include=None):
    pipe = r.pipeline()
    pipe.get("manifest:{key}".format(key=key))
    res = pipe.execute()    
    x_list = list(map(json.loads, res))
    # x_list.sort(key=lambda manifest: datetime.fromtimestamp(manifest.get("timestamp", 0)))
    manifest = x_list[0]
    # v =  version.parse(manifest.get("version", "0.0.0"))
    label_hash = ts_hash(metric_name=manifest["metric"]["metric_name"], label_config=manifest["metric"]["label_config"])
    if hash_include is not None and not (label_hash in hash_include):
        logger.debug("Skip loading model {h} ({metric_name}), label hash:{lh}".format(h = manifest["name"], metric_name=manifest["metric"]["metric_name"], lh=label_hash))
        return manifest, None
    h = manifest["name"]
    fsize = manifest["size"]
    cls_name = manifest["class"]
    md5 = manifest["md5"]    
    cls = None

    data = r.get('model:{key}'.format(key=h))

    if hashlib.md5(data).hexdigest() != md5:
        raise Exception("checksum does not match")

    if cls_name == "prophet":
        cls = model_prophet.MetricPredictor
    else:
        raise NotImplementedError("Model class cannot be mapped to serializer")

    model = loads_model(cls, data)
    if model is None:
        raise Exception
    
    logger.debug("Loaded model {h} ({metric_name}), label hash:{lh}, metric:{metric}".format(h = manifest["name"], metric_name=manifest["metric"]["metric_name"], lh=label_hash, metric=manifest["metric"]))
    
    return manifest, model
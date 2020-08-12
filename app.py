"""Metric Predictor/Anomaly detector
Overall mode:
# 1. Load/Update trained predictor Models
# 2. Discover/Update/Obsolete time series (unique combination of name and metric label/values dictionary).
# 3. Create new/Delete obsolete Gauges to cover all TS. (Gauge can be linked to multiple TS)
# 4. Load/Update/Trunkate value series for each discovered TS. Assign predictor Model for each Value Series (based on TS matching)
# 5. Update predictions of all models
# 6. Update Gauge values with relevant TS and available predictions
"""
import time
import os
import sys
import logging
import uuid
import re
from datetime import (datetime, timedelta)
from flask import (Flask, make_response)
from prometheus_client import (Gauge, generate_latest, CollectorRegistry)
from prometheus_api_client import (PrometheusConnect, Metric, MetricsList)
from configuration import Configuration
from sched import scheduler
from threading import (RLock, Semaphore, Lock, Thread)
import redis
from label_hash import ts_hash
import itertools

# Replace this for FS serialiser
# from model_marshal import (dump_model_list, load_model_list)
from model_marshal_redis import (dump_model_list, load_model_list)

logger = logging.getLogger(__name__)
ts_generation = itertools.count()
values_generation = itertools.count()
db_gauges = dict()  # Map[UUID, {"collector", ["valuesKey"]}]
db_ts = dict()  # Map[Hash[MetricInfo], {"labels", "generation"}]
db_values = dict()  # Map[Hash[MetricInfo], {"metric", "tsKey", "modelKey"}]
db_models = dict()  # Map[UUID, {"predictor", "labels"}]
scheduler_thread = None
registry = CollectorRegistry()
s = scheduler(time.time, time.sleep)
pc = PrometheusConnect(url=Configuration.prometheus_url, headers=Configuration.prom_connect_headers, disable_ssl=True,)
pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
r = redis.Redis(connection_pool=pool)
p_watches = r.pubsub()
app = Flask(__name__)


def get_metric_size(obj, seen=None):
    # From https://goshippo.com/blog/measure-real-size-any-python-object/
    # Recursively finds size of objects
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_metric_size(v, seen) for v in obj.values()])
        size += sum([get_metric_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_metric_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_metric_size(i, seen) for i in obj])
    return size

def update_models(local_predictor_model_list=None):
    """[Loads Predictor modes]
        When each model is loaded a new unique UUID is assigned to it.
        
        Model record:
        index (uuid):
        {
            "predictor" (MetricPredictor):
            "labels" (dict): label_config ++ {"__name__": metric_name}
        }
    Raises:
        e: [description]
    """
    try:
        if local_predictor_model_list is None:
            local_predictor_model_list = load_model_list(hash_include=list(db_ts.keys()))
        for (key, model, predictor) in local_predictor_model_list:
            # h = ts_hash(metric_name=model["metric_name"], label_config=model["label_config"])
            labels = {}
            labels.update(model["label_config"])
            labels.update({"__name__": model["metric_name"]})
            record_dict = {
                "predictor": predictor, 
                "labels": labels,
                "timestamp": model["timestamp"],
            }
            db_models.update({key: record_dict})
    except Exception as e:
        raise e


def update_tss():
    """Updates db_ts Store. Discover new TS and remove obsolete TS. 
    TS data are not stored in this record. Values record is used to store TS data.
    
    index (hash):
    {
        "labels" (dict): 
        "generation" (int):
    }
    metric (dict): Single record of a list returned by get_metric_range_data()
    generation (int): last update cycle when metric existed
    Raises:
        e: [description]
    """
    logger.info("Updating TS")
    now = datetime.now()
    generation = next(ts_generation)
    try:
        for metric in Configuration.metrics_list:
            current_start_time =  now - Configuration.current_data_window_size
            metric_init = pc.get_metric_range_data(metric_name=metric, start_time=current_start_time, end_time=now)
            
            hash_metric_list = list(map(lambda metric: (ts_hash(all_labels=metric["metric"]), {"labels": metric["metric"], "generation": generation}), metric_init))
            logger.info("new TS: {tss}".format(tss=dict(hash_metric_list)))
            db_ts.update(hash_metric_list)
            logger.info("TS stats: {tss}".format(tss=db_ts))
    except Exception as e:
        raise e
    


def update_values(models_include=None):
    """Update db_values for every TS.
    If Values record exists then updates its metric. If Values record does not exist then its created
    When Values record is created its predictor Model selected. Value record is associated with its TS.
    
    index (hash):
    {
        "metric" (Metric): first item of return value of MetricsList(get_metric_range_data())
        "ts" (tsKey): key of db_ts
        "model" (modelKey): key of db_models
    }

    Raises:
        Exception: [description]
        Exception: [description]
        Exception: [description]
        e: [description]
    """
    logger.info("Updating Values")
    now = datetime.now()
    generation = next(values_generation)
    for (h, ts) in db_ts.items():
        logger.debug("Updating [TS:{h}], labels:{labels}".format(h=h, labels=ts["labels"]))
        if h in db_values.keys():
            # TS is already tracked by a Values record in db_values
            current_start_time =  now - Configuration.current_data_window_size
            record = db_values[h]
            metric = record["metric"]
            metric_data = pc.get_metric_range_data(metric_name=metric.metric_name, label_config=metric.label_config, start_time=current_start_time, end_time=now)
            metrics = MetricsList(metric_data)
            if len(metrics) != 1:
                raise Exception("There can be only one")
            new_metric = metrics[0] + metric
            
            trunk_metric = Metric(new_metric, current_start_time) # This throws some exception really fast but this would have solved the problem.
            db_values[h]["metric"] = trunk_metric
            db_values[h]["generation"] = generation
            logger.debug("Update and truncate [Metric:{h}] horizon:{current_start_time} metric_name:{metric_name}, label_config:{label_config}".format(h=h, metric_name=metric.metric_name, label_config=metric.label_config, current_start_time=current_start_time))
        else:    
            current_start_time =  now - Configuration.current_data_window_size
            metric_name = ts["labels"]["__name__"]
            labels = dict()
            labels.update(ts["labels"])
            del labels["__name__"]
            
            items = db_models.items()
            if not models_include is None:
                items = filter(lambda item: item[0] in models_include, items)
                   
            models = list(filter(lambda model: ts_hash(all_labels=model[1]["labels"]) == h, items))
            if len(models) == 0:
                logger.warning("No models matching labels for [Metric:{h}] metric_name:{metric_name}, label_config:{label_config}".format(h=h, metric_name=metric_name, label_config=labels))
                continue
            
            metric_data = pc.get_metric_range_data(metric_name=metric_name, label_config=labels, start_time=current_start_time, end_time=now)
            metrics = MetricsList(metric_data)
            if len(metrics) != 1:
                raise Exception("There can be only one")
                        
            # pick the most recent model
            models.sort(key=lambda model: model[1].get("timestamp", datetime.fromtimestamp(0)), reverse=True)
            predictor = models[0][0]
            # predictor.build_prediction_df()
            record = {
                "metric": metrics[0],
                "ts": h,
                "model": predictor,
                "generation": generation
            }
            db_values.update({h: record})
            logger.debug("Add [Metric:{h}] horizon:{current_start_time} metric_name:{metric_name}, label_config:{label_config}".format(h=h, metric_name=metric_name, label_config=labels, current_start_time=current_start_time))


def update_values_models(keys=None):
    """[Update predictions of all models]

    Raises:
        Exception: [description]
    """
    logger.info("Updating [Value] models")
    items = db_values.items()
    if not keys is None:
        items = filter(lambda item: item[0] in keys, items)
    for (h, record) in items:
            # find all models with hash(labels) same as valuesKey
            models = list(filter(lambda model: ts_hash(all_labels=model[1]["labels"]) == h, db_models.items()))
            # pick the most recent model
            models.sort(key=lambda model: model[1].get("timestamp", datetime.fromtimestamp(0)), reverse=True)
            if len(models) == 0:
                raise Exception("There must be at least one predictor")
            predictor = models[0][1]["predictor"]
            if record["model"] != models[0][0]:
                logger.debug("Updating [Value:{h}] model to [Model:{mid}]".format(h=h, mid=models[0][0]))
                predictor.build_prediction_df()
                db_values[h]["model"] = models[0][0]


def update_gauges(registry):
    """Create Gauges for new TimeSeries. Delete Gauges for obsolete or non-existing TS
        Assocoates Gauge record with Values record.
        Method attempts to create a Gauge for each TS. If Gauge exists only associates the gauge record with the value
        
        Exception during gauge creation exception indicates that the Gauge has been created already. So method attempts to filter
        all gauges with current metric name. There must be only one other gauge registered with same metric name.
        
        Gauge record:
        index (uuid):
        {
                "collector" (Gauge): Gauge collector object registered in Registry. It contains all labels.
                "values" (set[valueKey]): list of db_values Keys that Gauge reports
        }

    Args:
        registry (registry.CollectorRegistry): Prometheus registry used to register Gauge collectors
        
    Raises:
        Exception: When more than one or none gauges were found during the search (see method description)
    """
    logger.info("Updating [Gauge] dictionary")
    for (h, ts) in db_ts.items():
        labels = dict()
        labels.update(ts["labels"])
        del labels["__name__"]
        label_list = list(labels.keys())
        label_list.append("value_type")
        try:
            gauge = Gauge(name=ts["labels"]["__name__"], documentation="Forecasted value", labelnames=label_list, registry=registry)            
            uid = str(uuid.uuid4())            
            record = {
                "collector": gauge,
                "values": set([h]),
            }
            db_gauges.update({uid: record})
            logger.debug("New gauge {mname} [Gauge:{gid}], link [TS:{tsid}]".format(mname=ts["labels"]["__name__"], gid=uid, tsid=db_gauges[uid]["values"]))
        except ValueError as e:
            # looking for already created gauge by metric name
            gauges = list(filter(lambda gauge: gauge[1]["collector"].describe()[0].name == ts["labels"]["__name__"], db_gauges.items()))
            if len(gauges) != 1:
                raise Exception("There can be only one")
            uid, record = gauges[0]                    
            record["values"].add(h)
            db_gauges.update({uid: record})
            logger.debug("Update gauge {mname} [Gauge:{gid}], link [TS:{tsid}]".format(mname=ts["labels"]["__name__"], gid=uid, tsid=db_gauges[uid]["values"]))


def update_gauge_values():
    """Updates values of all gauges
        Iterate through all gauges and sets ["yhat", "yhat_upper", "yhat_lower", "size", "original_value"]
    """
    logger.info("Updating [Gauge] values")
    now = datetime.now()
    for (h, gauge_record) in db_gauges.items():
        gauge = gauge_record["collector"]
        logger.debug("Set values in gauge [Gauge:{gid}], link [TS:{tsid}]".format(gid=h, tsid=gauge_record["values"]))
        for value_key in gauge_record["values"]:
            if not value_key in db_values.keys():
                logger.warning("[Value:{value_key}] does not exist for Gauge [Gauge:{gid}], link [TS:{tsid}]".format(value_key=value_key, gid=h, tsid=gauge_record["values"]))
                continue
            values = db_values[value_key]
            metric = values["metric"]
            predictor = db_models[values["model"]]["predictor"]
            prediction = predictor.predict_value(now)
            logger.debug("Set values in gauge [Gauge:{gid}], link [TS:{tsid}], [Metric:{labels}]. Data:{data}".format(gid=h, tsid=value_key, labels=db_ts[values["ts"]]["labels"], data=prediction.to_dict()))
            for column_name in list(prediction.columns):
                gauge.labels(
                    **metric.label_config,
                    value_type=column_name,
                ).set(prediction[column_name][0])

            uncertainty_range = prediction["yhat_upper"][0] - prediction["yhat_lower"][0]
            current_value = metric.metric_values.loc[metric.metric_values.ds.idxmax(), "y"]

            anomaly = 0
            weighted_anomaly = 0
            if ( current_value > prediction["yhat_upper"][0] ):                
                weighted_anomaly = ( current_value - prediction["yhat_upper"][0] ) / uncertainty_range
                anomaly = 1
            elif ( current_value < prediction["yhat_lower"][0] ):
                weighted_anomaly = ( current_value - prediction["yhat_lower"][0] ) / uncertainty_range
                anomaly = 1

            gauge.labels(
                **metric.label_config, value_type="weighted_anomaly"
            ).set(weighted_anomaly)
            
            gauge.labels(
                **metric.label_config, value_type="anomaly"
            ).set(anomaly)
            
            size = get_metric_size(metric)
            gauge.labels(
                **metric.label_config, value_type="size"
            ).set(get_metric_size(metric))
            
            size = get_metric_size(metric)
            gauge.labels(
                **metric.label_config, value_type="ts_generation"
            ).set(db_ts[values["ts"]]["generation"])
            
            size = get_metric_size(metric)
            gauge.labels(
                **metric.label_config, value_type="values_generation"
            ).set(values["generation"])
            
            gauge.labels(
                **metric.label_config, value_type="original_value"
            ).set(current_value)
            logger.debug("Set values in gauge [Gauge:{gid}], link [TS:{tsid}], [Metric:{labels}]. anomaly={anomaly}, size={size}, original_value={original_value}".format(gid=h, tsid=value_key, labels=db_ts[values["ts"]]["labels"], anomaly=anomaly, size=size, original_value=current_value))


def update_model_predictions(keys=None):
    logger.info("Update predictions")
    items = db_models.items()
    if not keys is None:
        items = filter(lambda item: item[0] in keys, items)
    for (h, model_record) in items:
        predictor = model_record["predictor"]
        ts_h = ts_hash(all_labels=model_record["labels"])
        logger.debug("Update prediction in [Model:{mid}], [Hash:{h}], labels:{labels}".format(mid=h, h=ts_h, labels=model_record["labels"]))
        predictor.build_prediction_df()


def update_gauges_proc():
    logger.info("Update gauges proc")
    update_gauges(registry)
    s.enter(300, 1, update_gauges_proc, [])


def update_values_proc():
    logger.info("Update values proc")
    update_values()
    s.enter(15, 1, update_values_proc, [])


def update_gauge_values_proc():
    logger.info("Update gauge values proc")
    update_gauge_values()
    s.enter(15, 1, update_gauge_values_proc, [])


def update_model_predictions_proc():
    logger.info("Update model predictions proc")
    update_model_predictions()
    s.enter(600, 1, update_model_predictions_proc, [])
    
def update_models_proc():
    logger.info("Update model proc")
    update_models()
    update_values_models()
    # s.enter(300, 1, update_models_proc, [])


def update_tss_proc():
    logger.info("Update TS proc")
    update_tss()
    s.enter(300, 1, update_tss_proc, [])

    
def init_proc():
    """[Initiate all dictionaries and start all timers]
    """    
    logger.info("Initial proc")
    update_tss_proc()        
    update_models_proc()
    update_model_predictions_proc()
    update_values_proc()
    update_gauges_proc()
    update_gauge_values_proc()
    logger.info("Initial proc complete")


def watch_db_proc():
    """[DB watching proc]
    The routine infinitely monitors manifest key updates in Redis. Once a key is updated ("set") it (re-)loads the model
    """
    p_watches.psubscribe('__keyspace@0__:manifest:*')
    while True:
        for m in p_watches.listen():
            if m["type"] == "pmessage" and m["data"].decode("utf-8") == "set":                
                original_pattern = m["channel"].decode("utf-8")
                pattern = re.compile("__keyspace@0__:manifest:(.*)")
                ms = pattern.search(original_pattern)
                key = ms.group(1).strip()
                logger.info("Updated Redis [Manifest:{key}]".format(key=key))
                local_predictor_model_list = load_model_list([key], hash_include=list(db_ts.keys()))
                s.enter(0, 1, update_models, [], kwargs={"local_predictor_model_list":local_predictor_model_list})
                s.enter(0, 2, update_model_predictions, [[key]])
                s.enter(0, 3, update_values, [[key]])                
                s.enter(0, 4, update_values_models, [[key]])
                s.enter(0, 5, update_gauge_values_proc, [])


@app.route('/metrics', endpoint="metrics-canonical")
@app.route('/', endpoint="metrics-root")
def root():
    metrics = generate_latest(registry).decode("utf-8")
    response = make_response(metrics, 200)
    response.mimetype = "text/plain"
    return response


def before_first_request_proc():
    s.enter(0, 1, init_proc, [])
    scheduler_thread = Thread(target=lambda: s.run(blocking=True), name="Scheduler").start()
    redis_watcher_thread = Thread(target=watch_db_proc, name="DBWatcher").start()


if __name__ == '__main__':
    logger.info("Application starting now")
    before_first_request_proc()
    app.run()
    
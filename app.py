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
from datetime import (datetime, timedelta)
from flask import (Flask, make_response)
from prometheus_client import (Gauge, generate_latest, CollectorRegistry)
from prometheus_api_client import (PrometheusConnect, Metric, MetricsList)
from configuration import Configuration
from sched import scheduler
from threading import (RLock, Semaphore, Lock, Thread)

# Replace this for FS serialiser
# from model_marshal import (dump_model_list, load_model_list)
from model_marshal_redis import (dump_model_list, load_model_list)


db_gauges = dict()  # Map[UUID, {"collector", ["valuesKey"]}]
db_ts = dict()  # Map[Hash[MetricInfo], {"labels", "generation"}]
db_values = dict()  # Map[Hash[MetricInfo], {"metric", "tsKey", "modelKey"}]
db_models = dict()  # Map[UUID, {"predictor", "labels"}]
scheduler_thread = None
registry = CollectorRegistry()
s = scheduler(time.time, time.sleep)
pc = PrometheusConnect(url=Configuration.prometheus_url, headers=Configuration.prom_connect_headers, disable_ssl=True,)
app = Flask(__name__)


def ts_hash(all_labels, metric_name=None, label_config=None):
    """[Compute unique hash for TS]
    Args:
        all_labels ([dict]): [combined label_config with __name__ label]
        metric_name ([string], optional): [label name. If provided added as __name__ label]. Defaults to None.
        label_config ([dict], optional): [label_config without __name__]. Defaults to None.

    Returns:
        [int]: [unique TS hash]
    """
    hash_dict = {}
    hash_dict.update(all_labels)
    if metric_name is not None:
        hash_dict.update({"__name__": metric_name})
    if label_config is not None:
        hash_dict.update(label_config)
    return hash(frozenset(hash_dict.items()))


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


def update_models():
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
        local_predictor_model_list = load_model_list()
        for (model, predictor) in local_predictor_model_list:
            # h = ts_hash(metric_name=model["metric_name"], label_config=model["label_config"])
            labels = {}
            labels.update(model["label_config"])
            labels.update({"__name__": model["metric_name"]})
            record_dict = {
                "predictor": predictor, 
                "labels": labels                 
            }
            db_models.update({str(uuid.uuid4()): record_dict})
    except Exception as e:
        raise e


def update_tss():
    """Updates TimeSerie Store. Discover new TS and remove obsolete TS. 
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
    now = datetime.now()
    try:
        for metric in Configuration.metrics_list:
            current_start_time =  now - Configuration.current_data_window_size
            metric_init = pc.get_metric_range_data(metric_name=metric, start_time=current_start_time, end_time=now)
            
            hash_metric_list = list(map(lambda metric: (ts_hash(all_labels=metric["metric"]), {"labels": metric["metric"], "generation": 0}), metric_init))
            db_ts.update(hash_metric_list)
    except Exception as e:
        raise e


def update_values():
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
    now = datetime.now()
    # try:
    for (h, ts) in db_ts.items():
        if h in db_values.keys():
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
        else:    
            current_start_time =  now - Configuration.current_data_window_size
            metric_name = ts["labels"]["__name__"]
            labels = dict()
            labels.update(ts["labels"])
            del labels["__name__"]
            metric_data = pc.get_metric_range_data(metric_name=metric_name, label_config=labels, start_time=current_start_time, end_time=now)
            metrics = MetricsList(metric_data)
            if len(metrics) != 1:
                raise Exception("There can be only one")
            
            models = list(filter(lambda model: ts_hash(all_labels=model[1]["labels"]) == h, db_models.items()))
            if len(models) != 1:
                    raise Exception("There can be only one")
            
            record = {
                "metric": metrics[0],
                "ts": h,
                "model": models[0][0]
            }
            db_values.update({h: record})
    # except Exception as e:
    #     raise e


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
            (gauge)
        except ValueError as e:
            # looking for already created gauge by metric name
            gauges = list(filter(lambda gauge: gauge[1]["collector"].describe()[0].name == ts["labels"]["__name__"], db_gauges.items()))
            if len(gauges) != 1:
                raise Exception("There can be only one")
            uid, record = gauges[0]                    
            record["values"].add(h)
            db_gauges.update({uid: record})


def update_gauge_values():
    """Updates values of all gauges
        Iterate through all gauges and sets ["yhat", "yhat_upper", "yhat_lower", "size", "original_value"]
    """
    now = datetime.now()
    for (h, gauge_record) in db_gauges.items():
        gauge = gauge_record["collector"]
        for value_key in gauge_record["values"]:
            values = db_values[value_key]
            metric = values["metric"]
            predictor = db_models[values["model"]]["predictor"]
            # predictor.build_prediction_df()
            prediction = predictor.predict_value(now)
            for column_name in list(prediction.columns):
                gauge.labels(
                    **metric.label_config,
                    value_type=column_name,
                ).set(prediction[column_name][0])

            uncertainty_range = prediction["yhat_upper"][0] - prediction["yhat_lower"][0]
            current_value = metric.metric_values.loc[metric.metric_values.ds.idxmax(), "y"]

            anomaly = 0
            if ( current_value > prediction["yhat_upper"][0] ):
                anomaly = ( current_value - prediction["yhat_upper"][0] ) / uncertainty_range
            elif ( current_value < prediction["yhat_lower"][0] ):
                anomaly = ( current_value - prediction["yhat_lower"][0] ) / uncertainty_range

            gauge.labels(
                **metric.label_config, value_type="anomaly"
            ).set(anomaly)
            
            gauge.labels(
                **metric.label_config, value_type="size"
            ).set(get_metric_size(metric))
            
            gauge.labels(
                **metric.label_config, value_type="original_value"
            ).set(current_value)


def update_model_predictions():
    for (h, model_record) in db_models.items():
        predictor = model_record["predictor"]
        predictor.build_prediction_df()


def update_gauges_proc():
    update_gauges(registry)
    s.enter(300, 1, update_gauges_proc, [])


def update_values_proc():
    update_values()
    s.enter(15, 1, update_values_proc, [])


def update_gauge_values_proc():
    update_gauge_values()
    s.enter(60, 1, update_gauge_values_proc, [])


def update_model_predictions_proc():
    update_model_predictions()
    s.enter(300, 1, update_model_predictions_proc, [])
    
def update_models_proc():
    update_models()
    # s.enter(300, 1, update_models_proc, [])


def update_tss_proc():
    update_tss()
    s.enter(300, 1, update_tss_proc, [])

    
def init_proc():
    update_models_proc()
    update_tss_proc()
    update_gauges_proc()    
    update_values_proc()
    update_model_predictions_proc()
    update_gauge_values_proc()


@app.route('/')
def root():
    metrics = generate_latest(registry).decode("utf-8")
    response = make_response(metrics, 200)
    response.mimetype = "text/plain"
    return response

@app.before_first_request
def before_first_request_proc():
    s.enter(0, 1, init_proc, [])
    scheduler_thread = Thread(target=lambda: s.run(blocking=True), name="Scheduler").start()

if __name__ == '__main__':
    before_first_request_proc()
    app.run()

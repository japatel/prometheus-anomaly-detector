"""docstring for packages."""
import time
import os
import logging
from datetime import datetime, timedelta
from multiprocessing import Process, Queue
from queue import Empty as EmptyQueueException
import tornado.ioloop
import tornado.web
from prometheus_client import Gauge, generate_latest, REGISTRY
from prometheus_api_client import PrometheusConnect, Metric, MetricsList
from configuration import Configuration
import schedule
import sys
import pickle
import itertools

# Replace this for FS serialiser
# from model_marshal import dump_model_list, load_model_list
from model_marshal_redis import dump_model_list, load_model_list


_LOGGER = logging.getLogger(__name__)

METRICS_LIST = Configuration.metrics_list
_LOGGER.info("Metric List: %s", METRICS_LIST)

PREDICTOR_MODEL_LIST = list()

pc = PrometheusConnect(
    url=Configuration.prometheus_url,
    headers=Configuration.prom_connect_headers,
    disable_ssl=True,
)

_LOGGER.info("Metric List size: %s", len(METRICS_LIST))
for metric in METRICS_LIST:
    # Initialize a predictor for all metrics first
    _LOGGER.info("Metric List read: %s", metric)
    current_start_time =  datetime.now() - Configuration.current_data_window_size
    metric_init = pc.get_metric_range_data(metric_name=metric, start_time=current_start_time, end_time=datetime.now())
    
    metric_list = map(lambda metric: Metric(metric, Configuration.rolling_training_window_size), metric_init)
    PREDICTOR_MODEL_LIST.extend(zip(metric_list, itertools.starmap(Configuration.model_module.MetricPredictor, itertools.repeat([]))))


def train_model():
    """Train the machine learning model.
    Traning interval rounds up to day starts (00h:00m:00s.00)
    """
    for (metric_to_predict, predictor_model) in PREDICTOR_MODEL_LIST:
        today = datetime(*datetime.now().timetuple()[:3])
        data_start_time = today - Configuration.rolling_training_window_size
        data_end_time = today

        _LOGGER.info(
            "Training MatricName = %s, label_config = %s, start_time = %s, end_time = %s",
            metric_to_predict.metric_name,
            metric_to_predict.label_config,
            data_start_time,
            data_end_time
        )

        # Download new metric data from prometheus
        new_metric_data = pc.get_metric_range_data(
            metric_name=metric_to_predict.metric_name,
            label_config=metric_to_predict.label_config,
            start_time=data_start_time,
            end_time=data_end_time,
        )[0]

        _LOGGER.info("Train after getting new data")

        # Train the new model
        start_time = datetime.now()
        predictor_model.train(new_metric_data, data_start_time)
        # predictor_model.build_prediction_df(Configuration.retraining_interval_minutes)
        _LOGGER.info(
            "Total Training time taken = %s, for metric: %s %s",
            str(datetime.now() - start_time),
            metric_to_predict.metric_name,
            metric_to_predict.label_config,
        )

        # _LOGGER.info("Predictor Model size: %s", predictor_model_size)


def build_predictions(data_queue=None):
    for (metric_to_predict, predictor_model) in PREDICTOR_MODEL_LIST:
        predictor_model.build_prediction_df(Configuration.retraining_interval_minutes)
    data_queue.put(PREDICTOR_MODEL_LIST)


if __name__ == "__main__":
    train_model()
    dump_model_list(PREDICTOR_MODEL_LIST)


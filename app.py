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
from prometheus_api_client import PrometheusConnect, Metric
from configuration import Configuration
import schedule
import sys

# Set up logging
_LOGGER = logging.getLogger(__name__)

METRICS_LIST = Configuration.metrics_list

# list of ModelPredictor Objects shared between processes
PREDICTOR_MODEL_LIST = list()

pc = PrometheusConnect(
    url=Configuration.prometheus_url,
    headers=Configuration.prom_connect_headers,
    disable_ssl=True,
)

_LOGGER.info("Metric List size: %s", len(METRICS_LIST))
for metric in METRICS_LIST:
    # Initialize a predictor for all metrics first
    current_start_time =  datetime.now() - Configuration.current_data_window_size
    metric_init = pc.get_metric_range_data(metric_name=metric, start_time=current_start_time, end_time=datetime.now())

    for unique_metric in metric_init:
        PREDICTOR_MODEL_LIST.append(
            Configuration.model_module.MetricPredictor(
                unique_metric,
                rolling_data_window_size=Configuration.rolling_training_window_size,
            )
        )

# A gauge set for the predicted values
GAUGE_DICT = dict()
_LOGGER.info("Predictor model List size: %s", len(PREDICTOR_MODEL_LIST))
for predictor in PREDICTOR_MODEL_LIST:
    unique_metric = predictor.metric
    label_list = list(unique_metric.label_config.keys())
    label_list.append("value_type")
    if unique_metric.metric_name not in GAUGE_DICT:
        GAUGE_DICT[unique_metric.metric_name] = Gauge(
            unique_metric.metric_name + "_" + predictor.model_name,
            predictor.model_description,
            label_list,
        )

_LOGGER.info("Gauge disct size: %s", len(GAUGE_DICT))
class MainHandler(tornado.web.RequestHandler):
    """Tornado web request handler."""

    def initialize(self, data_queue):
        """Check if new predicted values are available in the queue before the get request."""
        try:
            model_list = data_queue.get_nowait()
            self.settings["model_list"] = model_list
        except EmptyQueueException:
            pass

    async def get(self):
        """Fetch and publish metric values asynchronously."""
        # update metric value on every request and publish the metric
        for predictor_model in self.settings["model_list"]:
            # get the current metric value so that it can be compared with the
            # predicted values
            current_start_time =  datetime.now() - Configuration.current_data_window_size
            current_end_time = datetime.now()

            weekago_start_time = (datetime.now() - timedelta(days=7)) - Configuration.current_data_window_size
            weekago_end_time = (datetime.now() - timedelta(days=7))

            twoweeksago_start_time = (datetime.now() - timedelta(days=14)) - Configuration.current_data_window_size
            twoweeksago_end_time = (datetime.now() - timedelta(days=14))

            trust_prediction = 0
            anomaly = 1

            _LOGGER.info(
                "MatricName = %s, label_config = %s, start_time = %s, end_time = %s",
                predictor_model.metric.metric_name,
                predictor_model.metric.label_config,
                current_start_time,
                current_end_time
            )

            prediction_data_size = 0
            metric_name = predictor_model.metric.metric_name
            prediction = predictor_model.predict_value(datetime.now())

            if "size" in prediction:
               prediction_data_size = prediction['size']

            weekago_metric_data = pc.get_metric_range_data(
                metric_name=predictor_model.metric.metric_name,
                label_config=predictor_model.metric.label_config,
                start_time=weekago_start_time,
                end_time=weekago_end_time,
            )

            if weekago_metric_data and hasattr(weekago_metric_data, "__len__"):
                weekago_metric_value = Metric(weekago_metric_data[0])
                if (
                        weekago_metric_value.metric_values.loc[weekago_metric_value.metric_values.ds.idxmax(), "y"] < prediction["yhat_upper"][0]
                ) and (
                        weekago_metric_value.metric_values.loc[weekago_metric_value.metric_values.ds.idxmax(), "y"] > prediction["yhat_lower"][0]
                ):
                    trust_prediction = 1

            twoweeksago_metric_data = pc.get_metric_range_data(
                    metric_name=predictor_model.metric.metric_name,
                    label_config=predictor_model.metric.label_config,
                    start_time=twoweeksago_start_time,
                    end_time=twoweeksago_end_time,
            )

            if twoweeksago_metric_data and hasattr(twoweeksago_metric_data, "__len__"):
                twoweeksago_metric_value = Metric(twoweeksago_metric_data[0])
                if (
                        twoweeksago_metric_value.metric_values.loc[twoweeksago_metric_value.metric_values.ds.idxmax(), "y"] < prediction["yhat_upper"][0]
                ) and (
                        twoweeksago_metric_value.metric_values.loc[twoweeksago_metric_value.metric_values.ds.idxmax(), "y"] > prediction["yhat_lower"][0]
                ):
                    trust_prediction = 1

            current_metric_data = pc.get_metric_range_data(
                metric_name=predictor_model.metric.metric_name,
                label_config=predictor_model.metric.label_config,
                start_time=current_start_time,
                end_time=current_end_time,
            )

            # Check for all the columns available in the prediction
            # and publish the values for each of them
            for column_name in list(prediction.columns):
                GAUGE_DICT[metric_name].labels(
                    **predictor_model.metric.label_config, value_type=column_name
                ).set(prediction[column_name][0])

            if current_metric_data and hasattr(current_metric_data, "__len__"):
                current_metric_value = Metric(current_metric_data[0])
                if (
                        current_metric_value.metric_values.loc[current_metric_value.metric_values.ds.idxmax(), "y"] < prediction["yhat_upper"][0]
                ) and (
                        current_metric_value.metric_values.loc[current_metric_value.metric_values.ds.idxmax(), "y"] > prediction["yhat_lower"][0]
                ):
                    anomaly = 0
                elif trust_prediction == 0:
                    anomaly = 0

                # create a new time series that has value_type=anomaly
                # this value is 1 if an anomaly is found 0 if not
                GAUGE_DICT[metric_name].labels(
                    **predictor_model.metric.label_config, value_type="anomaly"
                ).set(anomaly)

                _LOGGER.info(
                    "Got current values in Mainhandler = %s and newest value = %s, IDXMAX = %s",
                    current_metric_value.metric_values,
                    current_metric_value.metric_values.loc[current_metric_value.metric_values.ds.idxmax(), 'y'],
                    current_metric_value.metric_values.ds.idxmax()
                )

            GAUGE_DICT[metric_name].labels(
                **predictor_model.metric.label_config, value_type="size"
            ).set(prediction_data_size)

        self.write(generate_latest(REGISTRY).decode("utf-8"))
        self.set_header("Content-Type", "text; charset=utf-8")

def get_size(obj, seen=None):
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
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size

def make_app(data_queue):
    """Initialize the tornado web app."""
    return tornado.web.Application(
        [
            (r"/metrics", MainHandler, dict(data_queue=data_queue)),
            (r"/anomaly", MainHandler, dict(data_queue=data_queue)),
            (r"/", MainHandler, dict(data_queue=data_queue)),
        ]
    )


def train_model(initial_run=False, data_queue=None):
    """Train the machine learning model."""
    for predictor_model in PREDICTOR_MODEL_LIST:
        metric_to_predict = predictor_model.metric
        data_start_time = datetime.now() - Configuration.metric_chunk_size
        if initial_run:
            data_start_time = (
                datetime.now() - Configuration.rolling_training_window_size
            )

        data_end_time = datetime.now()

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

        metric_data_size = get_size(new_metric_data)
        _LOGGER.info("Metric Data for Training: %s", metric_data_size)

        # Train the new model
        start_time = datetime.now()
        predictor_model.train(
            new_metric_data, Configuration.retraining_interval_minutes
        )
        _LOGGER.info(
            "Total Training time taken = %s, for metric: %s %s",
            str(datetime.now() - start_time),
            metric_to_predict.metric_name,
            metric_to_predict.label_config,
        )

        predictor_model.predicted_df["size"] = metric_data_size

    data_queue.put(PREDICTOR_MODEL_LIST)


if __name__ == "__main__":
    # Queue to share data between the tornado server and the model training
    predicted_model_queue = Queue()

    # Initial run to generate metrics, before they are exposed
    train_model(initial_run=True, data_queue=predicted_model_queue)

    _LOGGER.info("Before make_app")

    # Set up the tornado web app
    app = make_app(predicted_model_queue)
    _LOGGER.info("After make_app")
    app.listen(8088)
    _LOGGER.info("After listen")
    server_process = Process(target=tornado.ioloop.IOLoop.instance().start)
    _LOGGER.info("After server_process")
    # Start up the server to expose the metrics.
    server_process.start()
    _LOGGER.info("After sever_process start")

    # Schedule the model training
    schedule.every(Configuration.retraining_interval_minutes).minutes.do(
        train_model, initial_run=False, data_queue=predicted_model_queue
    )
    _LOGGER.info(
        "Will retrain model every %s minutes", Configuration.retraining_interval_minutes
    )

    while True:
        schedule.run_pending()
        time.sleep(1)

    # join the server process in case the main process ends
    server_process.join()

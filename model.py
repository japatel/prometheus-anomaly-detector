"""doctsring for packages."""
from datetime import datetime, timedelta
import logging
import pandas
from fbprophet import Prophet
from fbprophet.diagnostics import cross_validation
from fbprophet.diagnostics import performance_metrics
from prometheus_api_client import Metric
import pickle

# Set up logging
_LOGGER = logging.getLogger(__name__)


class MetricPredictor:
    """docstring for Predictor."""

    predicted_df = None
    # metric = None

    def __init__(self):
        """Initialize the Metric object."""
        self._model = None

    def train(self, metric_dict, oldest_data_datetime):
        """Train the Prophet model and store the predictions in predicted_df."""
        prediction_freq = "1MIN"

        # convert incoming metric to Metric Object
        metric = Metric(metric_dict, oldest_data_datetime)

        self._model = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)

        _LOGGER.info("training data range: %s - %s", metric.start_time, metric.end_time)

        _LOGGER.debug("begin training")

        df_fit = self._model.fit(metric.metric_values)

        if True:
            df_cv = cross_validation(self._model, horizon="1 day", period="8 hours", initial="4 days")
            df_p = performance_metrics(df_cv)
            _LOGGER.info("Performance data: %s %s", metric.metric_name, df_p)
                    
    def build_prediction_df(self, prediction_duration_min=15):
        """Train the Prophet model and store the predictions in predicted_df."""
        model_end_time = self._model.start + self._model.t_scale
        now = datetime.now()
        d = now - model_end_time

        prediction_freq = "15s"
        duration = (d.total_seconds() / 60 + prediction_duration_min) * 4
        
        future = self._model.make_future_dataframe(
            periods=int(duration),
            freq=prediction_freq,
            include_history=False,
        )
        _LOGGER.debug(future)
        forecast = self._model.predict(future)
        forecast["timestamp"] = forecast["ds"]
        forecast = forecast[["timestamp", "yhat", "yhat_lower", "yhat_upper"]]
        forecast = forecast.set_index("timestamp")
        self.predicted_df = forecast

        _LOGGER.debug(forecast)
        _LOGGER.info(forecast.memory_usage(deep=True))

    def predict_value(self, prediction_datetime):
        """Return the predicted value of the metric for the prediction_datetime."""
        nearest_index = self.predicted_df.index.get_loc(
            prediction_datetime, method="nearest"
        )
        return self.predicted_df.iloc[[nearest_index]]


    @property 
    def model(self):
        return self._model
    
    def dumps(self):
        data = pickle.dumps(self._model)
        _LOGGER.debug("Pickled data: {size} bytes".format(size=len(data)))
        return data
    
    @classmethod
    def loads(cls, data):
        obj = pickle.loads(data)
        _LOGGER.debug("Unpickled data: {size} bytes".format(size=len(data)))
        self = cls()
        _LOGGER.debug("Constructed new object {oid}".format(oid=id(self)))
        self._model = obj
        return self
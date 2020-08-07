"""docstring for installed packages."""
import os
import logging
from prometheus_api_client.utils import parse_datetime, parse_timedelta
import model_lstm as model_lstm
import model as model_prophet
import model_fourier as model_fourier

if os.getenv("FLT_DEBUG_MODE", "False") == "True":
    LOGGING_LEVEL = logging.DEBUG  # Enable Debug mode
else:
    LOGGING_LEVEL = logging.INFO
# Log record format
logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(name)s: %(message)s", level=LOGGING_LEVEL
)
# set up logging
_LOGGER = logging.getLogger(__name__)

model_module_map = {}

try: 
    _LOGGER.debug("Loading Prophet model")
    import model as model_prophet
    model_module_map["prophet"] = model_prophet
except ImportError:
    pass

try: 
    _LOGGER.debug("Loading LSTM model")
    import model_lstm as model_lstm
    model_module_map["lstm"] = model_lstm
except ImportError:
    pass

try: 
    _LOGGER.debug("Loading fourier model")
    import model_fourier as model_fourier
    model_module_map["fourier"] = model_fourier
except ImportError:
    pass

if len(model_module_map) == 0:
    raise Exception("No models loaded. At least one model should be loaded.")

class Configuration:
    """docstring for Configuration."""

    # url for the prometheus host
    prometheus_url = os.getenv("FLT_PROM_URL")

    # any headers that need to be passed while connecting to the prometheus host
    prom_connect_headers = None
    # example oath token passed as a header
    if os.getenv("FLT_PROM_ACCESS_TOKEN"):
        prom_connect_headers = {
            "Authorization": "bearer " + os.getenv("FLT_PROM_ACCESS_TOKEN")
        }

    # example basic passed as a header
    if os.getenv("FLT_PROM_ACCESS_BASIC"):
        prom_connect_headers = {
            "Authorization": "Basic " + os.getenv("FLT_PROM_ACCESS_BASIC")
        }

    # list of metrics that need to be scraped and predicted
    # multiple metrics can be separated with a ";"
    # if a metric configuration matches more than one timeseries,
    # it will scrape all the timeseries that match the config.
    metrics_list = str(
        os.getenv(
            "FLT_METRICS_LIST",
            "up{app='openshift-web-console', instance='172.44.0.18:8443'}",
        )
    ).split(";")

    # this will create a rolling data window on which the model will be trained
    # example: if set to 15d will train the model on past 15 days of data,
    # every time new data is added, it will truncate the data that is out of this range.
    rolling_training_window_size = parse_timedelta(
        "now", os.getenv("FLT_ROLLING_TRAINING_WINDOW_SIZE", "3d")
    )

    # current data window size is the time window from now where the current data value
    # will be found
    # default is 5 minutes
    current_data_window_size = parse_timedelta(
        "now", os.getenv("FLT_CURRENT_DATA_WINDOW_SIZE", "5m")
    )

    # How often should the anomaly detector retrain the model (in minutes)
    retraining_interval_minutes = int (
        os.getenv("FLT_RETRAINING_INTERVAL_MINUTES", "120")
    )

    port = int (os.getenv("PORT", "8088"))

    check_perf = bool (int(os.getenv("FLT_CHECK_PERF", "0")))

    model_name = os.getenv("FLT_MODEL", list(model_module_map.keys())[0])
    model_module = model_module_map[model_name]
    
    _LOGGER.info("Using model: \"%s\"", model_name)

    _LOGGER.info(
        "Metric data rolling training window size: %s", rolling_training_window_size
    )
    _LOGGER.info("Model retraining interval: %s minutes", retraining_interval_minutes)

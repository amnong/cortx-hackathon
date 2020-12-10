import logging
import time

from colorama import Fore, Style

from PySiddhi.DataTypes.LongType import LongType
from PySiddhi.core.SiddhiManager import SiddhiManager
from PySiddhi.core.query.output.callback.QueryCallback import QueryCallback
from PySiddhi.core.util.EventPrinter import PrintEvent

from .monitor import monitor_buckets


logger = logging.getLogger(__name__)


LOG_INPUT_STREAM_NAME = "cortxEventStream"
LOG_QUERY_NAME = "cortxEventQuery"

BUCKET_INPUT_STREAM_NAME = "cortxBucketStream"
BUCKET_QUERY_NAME = "cortxBucketQuery"
# Siddhi Query to filter events with volume less than 150 as output
SIDDHI_APP = """\
define stream {log_stream} (
    event_code string,
    logfile string
);

@info(name = '{log_query}')
from {log_stream}#window.lengthBatch(5)
select event_code,logfile
insert into outputStream;

define stream {bucket_stream} (
    event_code string,
    bucket string
);

@info(name = '{bucket_query}')
from {bucket_stream}#window.timeBatch(10 sec)
select event_code,bucket
insert into outputStream;
""".format(log_stream=LOG_INPUT_STREAM_NAME, log_query=LOG_QUERY_NAME, bucket_stream=BUCKET_INPUT_STREAM_NAME, bucket_query=BUCKET_QUERY_NAME)
#from {stream}[volume < 150]


def run(args):
    logger.info('Bootstrapping:')
    siddhi_manager = SiddhiManager()
    logger.info('Manager up...')
    runtime = siddhi_manager.createSiddhiAppRuntime(SIDDHI_APP)
    logger.info('Runtime up...')

    # Add listener to capture output events
    class LogQueryCallbackImpl(QueryCallback):
        def receive(self, timestamp, inEvents, outEvents):
            #PrintEvent(timestamp, inEvents, outEvents)
            log_filenames = [event.getData(1) for event in inEvents]
            logger.info('%sCompressing log file %s%s%s', Fore.WHITE, Fore.GREEN, ', '.join(log_filenames), Style.RESET_ALL)

    class BucketQueryCallbackImpl(QueryCallback):
        def receive(self, timestamp, inEvents, outEvents):
            #PrintEvent(timestamp, inEvents, outEvents)
            log_filenames = [event.getData(1) for event in inEvents]
            logger.info('%sCompressing log file %s%s%s', Fore.WHITE, Fore.GREEN, ', '.join(log_filenames), Style.RESET_ALL)


    runtime.addCallback(BUCKET_QUERY_NAME, BucketQueryCallbackImpl())

    # Retrieving input handler to push events into Siddhi
    input_handler = runtime.getInputHandler(BUCKET_INPUT_STREAM_NAME)

    # Starting event processing
    logger.info('Starting runtime...')
    runtime.start()

    try:
        monitor_buckets(input_handler)
        # dummy_log_events(input_handler)
        logger.info('Waiting for any residual events')
        time.sleep(10)
    except Exception as e:  # pylint: disable=broad-except
        logger.error('UNEXPECTED ERROR: %s', e)
        raise
    finally:
        logger.info('Shutting down...')
        siddhi_manager.shutdown()
        logger.info('Goodbye')


def dummy_log_events(input_handler):
    # Sending events to Siddhi
    logger.info('Sending events')
    for n in range(1, 31):
        event_data = ["LOG_CREATED", "foo.log.%d" % n]
        logger.info('Sending event %s', event_data)
        input_handler.send(event_data)
        if n % 3 == 0:
            time.sleep(5)


def integrate(input_handler):
    # Sending events to Siddhi
    logger.info('Sending events')
    input_handler.send(["IBM", 700.0, LongType(100)])
    input_handler.send(["WSO2", 60.5, LongType(200)])
    input_handler.send(["GOOG", 50, LongType(30)])
    input_handler.send(["IBM", 76.6, LongType(400)])
    input_handler.send(["WSO2", 45.6, LongType(50)])

    # Wait for response
    logger.info('Waiting...')
    time.sleep(2)

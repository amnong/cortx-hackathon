import logging
from time import sleep

from PySiddhi.DataTypes.LongType import LongType
from PySiddhi.core.SiddhiManager import SiddhiManager
from PySiddhi.core.query.output.callback.QueryCallback import QueryCallback
from PySiddhi.core.util.EventPrinter import PrintEvent


logger = logging.getLogger(__name__)


INPUT_STREAM_NAME = "cseEventStream"
QUERY_NAME = "query1"

# Siddhi Query to filter events with volume less than 150 as output
SIDDHI_APP = """\
define stream {stream} (
    symbol string,
    price float,
    volume long
);

@info(name = '{query}')
from {stream}[volume < 150]
select symbol,price
insert into outputStream;
""".format(stream=INPUT_STREAM_NAME, query=QUERY_NAME)


def run(args):
    logger.info('Bootstrapping:')
    siddhi_manager = SiddhiManager()
    logger.info('Manager up...')
    runtime = siddhi_manager.createSiddhiAppRuntime(SIDDHI_APP)
    logger.info('Runtime up...')

    # Add listener to capture output events
    class QueryCallbackImpl(QueryCallback):
        def receive(self, timestamp, inEvents, outEvents):
            PrintEvent(timestamp, inEvents, outEvents)

    runtime.addCallback(QUERY_NAME, QueryCallbackImpl())

    # Retrieving input handler to push events into Siddhi
    input_handler = runtime.getInputHandler(INPUT_STREAM_NAME)

    # Starting event processing
    logger.info('Starting runtime...')
    runtime.start()

    try:
        celery_entry_point(input_handler)
    except Exception as e:  # pylint: disable=broad-except
        logger.error('UNEXPECTED ERROR: %s', e)
        raise
    finally:
        logger.info('Shutting down...')
        siddhi_manager.shutdown()
        logger.info('Goodbye')


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
    sleep(2)

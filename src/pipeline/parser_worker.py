import threading
import time

from ingestion.log_type_checker import LogTypeChecker

from parser.parser_factory import ParserFactory

from pipeline.queues import raw_queue

from pipeline.batch_processor import BatchProcessor

from observability.logging_config import setup_logger

from observability.metrics import (
    PROCESSED_LOGS,
    PARSE_FAILURES,
    UNKNOWN_LOGS,
    ACTIVE_WORKERS,
    PROCESSING_TIME,
)

from observability.tracing import tracer


logger = setup_logger()


class ParserWorker:

    def __init__(self, worker_count=4):

        self.batch_processor = BatchProcessor()

        self.worker_count = worker_count

    def start(self):

        logger.info(
            f"Starting {self.worker_count} parser workers"
        )

        for index in range(self.worker_count):

            thread = threading.Thread(
                target=self._worker_loop,
                name=f"parser-worker-{index}"
            )

            thread.daemon = True

            thread.start()

    def _worker_loop(self):

        ACTIVE_WORKERS.inc()

        while True:

            raw_line = raw_queue.get()

            start = time.time()

            with tracer.start_as_current_span("parse_log"):

                try:

                    log_type, parsed_data = (
                        LogTypeChecker.check_log_type(raw_line)
                    )

                    if log_type == "UNKNOWN":

                        UNKNOWN_LOGS.inc()

                        logger.warning(
                            "Unknown log detected"
                        )

                    parser = ParserFactory.get_parser(
                        log_type
                    )

                    event = parser.parse(
                        raw_line,
                        parsed_data
                    )

                    self.batch_processor.add(event)

                    PROCESSED_LOGS.inc()

                except Exception as e:

                    PARSE_FAILURES.inc()

                    logger.exception(
                        f"Parsing failed: {e}"
                    )

                finally:

                    PROCESSING_TIME.observe(
                        time.time() - start
                    )

            raw_queue.task_done()
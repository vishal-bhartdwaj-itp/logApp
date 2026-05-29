import threading
import time

from storage.loki_sink import LokiSink

from observability.logging_config import setup_logger


logger = setup_logger()


class BatchProcessor:

    def __init__(self):

        self.batch = []

        self.batch_size = 100

        self.flush_interval = 5

        self.sink = LokiSink()

        threading.Thread(
            target=self._flush_loop,
            daemon=True
        ).start()

    def add(self, event):

        self.batch.append(event)

        if len(self.batch) >= self.batch_size:
            self.flush()

    def flush(self):

        if not self.batch:
            return

        try:

            self.sink.push(self.batch)

            logger.info(
                f"Flushed {len(self.batch)} logs to Loki"
            )

        except Exception as e:

            logger.exception(
                f"Loki push failed: {e}"
            )

        self.batch.clear()

    def _flush_loop(self):

        while True:

            time.sleep(self.flush_interval)

            self.flush()
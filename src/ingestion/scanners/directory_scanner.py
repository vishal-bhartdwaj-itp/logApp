import re
import os
import time
from collections import deque

from ingestion.checkpoint.state_manager import StateManager

from pipeline.queues import raw_queue

from observability.logging_config import setup_logger

from observability.metrics import (
    QUEUE_SIZE,
    SCANNER_READ_TIME
)

from observability.tracing import tracer


logger = setup_logger()


class DirectoryReader:

    LOG_START_PATTERN = re.compile(
        r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3}"
    )

    def __init__(self, base_dir="data/logs"):

        self.base_dir = base_dir

        self.state_manager = StateManager()

        self.file_queue = deque()

    def _discover_and_enqueue(self):

        logger.info(
            f"Scanning directory: {self.base_dir}"
        )

        for root, dirs, files in os.walk(self.base_dir):

            dirs.sort()

            for file in files:

                full_path = os.path.join(root, file)

                self.file_queue.append(full_path)

        logger.info(
            f"Discovered {len(self.file_queue)} files"
        )

    def scan_for_data(self):

        self._discover_and_enqueue()

        while self.file_queue:

            file_path = self.file_queue.popleft()

            with tracer.start_as_current_span("scan_file"):

                start = time.time()

                try:

                    stat_info = os.stat(file_path)

                    inode = stat_info.st_ino

                except OSError:

                    logger.exception(
                        f"Cannot access file: {file_path}"
                    )

                    continue

                last_offset = self.state_manager.get_offset(
                    inode
                )

                logger.info(
                    f"Reading {file_path} from offset {last_offset}"
                )

                with open(
                    file_path,
                    "r",
                    encoding="utf-8",
                    errors="ignore"
                ) as f:

                    f.seek(last_offset)

                    current_event: list[str] = []
                    # True when current_event started with a LOG_START_PATTERN
                    # line — meaning continuations (stack traces) should be
                    # appended rather than emitted as separate events.
                    event_has_timestamp = False

                    while True:

                        line = f.readline()

                        if not line:
                            break

                        if self.LOG_START_PATTERN.match(line):
                            # New timestamped event — flush whatever we have
                            if current_event:
                                raw_queue.put("".join(current_event))
                                QUEUE_SIZE.set(raw_queue.qsize())

                            current_event = [line]
                            event_has_timestamp = True

                        elif event_has_timestamp:
                            # Continuation of a multi-line event (stack trace
                            # or wrapped message after an APPEVOLVE header)
                            current_event.append(line)

                        else:
                            # Non-timestamped line (syslog, NGINX, JSON, etc.)
                            # Each such line is its own independent event.
                            if current_event:
                                raw_queue.put("".join(current_event))
                                QUEUE_SIZE.set(raw_queue.qsize())

                            current_event = [line]
                            event_has_timestamp = False

                    # push final event
                    if current_event:

                        raw_event = "".join(current_event)

                        raw_queue.put(raw_event)

                        QUEUE_SIZE.set(
                            raw_queue.qsize()
                        )

                    self.state_manager.save_state(
                        inode,
                        file_path,
                        f.tell()
                    )
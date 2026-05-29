from datetime import datetime

from models.log_event import LogEvent
from .base_log_parser import BaseLogParser


class AgenticLogParser(BaseLogParser):

    def parse(self, raw_line, parsed_data):

        return LogEvent(
            timestamp=datetime.utcnow(),
            log_level="UNKNOWN",
            log_type="UNKNOWN",
            service_name="unknown",
            environment="unknown",
            message=raw_line,
            metadata={},
            raw=raw_line
        )

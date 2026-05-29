from datetime import datetime

from models.log_event import LogEvent
from .base_log_parser import BaseLogParser


class JsonLogParser(BaseLogParser):

    def parse(self, raw_line, parsed_data):

        return LogEvent(
            timestamp=datetime.utcnow(),
            log_level=parsed_data.get("log_level", "INFO"),
            log_type="JSON",
            service_name=parsed_data.get("service_name", "unknown"),
            environment=parsed_data.get("environment", "unknown"),
            message=parsed_data.get("message", ""),
            metadata=parsed_data,
            raw=raw_line
        )

from datetime import datetime

from models.log_event import LogEvent
from .base_log_parser import BaseLogParser


class AppEvolveLogParser(BaseLogParser):

    def parse(self, raw_line, parsed_data):

        return LogEvent(
            timestamp=datetime.strptime(
                parsed_data["timestamp"],
                "%Y-%m-%d %H:%M:%S,%f"
            ),
            log_level=parsed_data["level"],
            log_type="APPEVOLVE",
            service_name="neurostack",
            environment="production",
            message=parsed_data["message"],
            metadata={
                "pid": parsed_data["pid"],
                "logger": parsed_data["logger"],
                "source": parsed_data["source"]
            },
            raw=raw_line
        )

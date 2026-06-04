import json
import re


class LogTypeChecker:

    NGINX_PATTERN = re.compile(
        r'^(?P<ip>\S+)\s+\S+\s+(?P<user>\S+)\s+\[(?P<timestamp>[^\]]+)\]\s+'
        r'"(?P<method>\S+)\s+(?P<request>[^\s"]+)\s+(?P<protocol>[^"]+)"\s+'
        r'(?P<status>\d{3})\s+(?P<size>\d+|-)'
    )

    APPEVOLVE_PATTERN = re.compile(
        r'^(?P<timestamp>\d{4}-\d{2}-\d{2}\s+'
        r'\d{2}:\d{2}:\d{2},\d{3})\s*\|\s*'
        r'(?P<level>[A-Z]+)\s*\|\s*'
        r'(?P<pid>\d+)\s*\|\s*'
        r'(?P<logger>.*?)\s*\|\s*'
        r'(?P<source>[^|]+?)\s*\|\s*'
        r'(?P<message>.*)$'
    )

    @staticmethod
    def check_log_type(entry: str):

        clean_entry = entry.splitlines()[0] if entry.strip() else ""

        if not clean_entry.strip():
            return "EMPTY", None

        try:
            if clean_entry.startswith("{") and clean_entry.endswith("}"):
                return "JSON", json.loads(clean_entry)
        except Exception:
            pass

        nginx_match = LogTypeChecker.NGINX_PATTERN.match(
            clean_entry
        )

        if nginx_match:
            return "NGINX", nginx_match.groupdict()

        app_match = LogTypeChecker.APPEVOLVE_PATTERN.match(
            clean_entry
        )

        if app_match:
            return "APPEVOLVE", app_match.groupdict()

        return "UNKNOWN", {
            "raw": clean_entry
        }
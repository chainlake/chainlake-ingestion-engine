# import logging
from datetime import datetime


class EventTimeCalculator:
    def calculate_event_timestamp(self, item):
        """
        Extract blockchain timestamp
        """
        if item is None or not isinstance(item, dict):
            return None

        # block
        if item.get("type") == "block":
            ts = item.get("timestamp")

        # others (tx/log/trace/etc)
        else:
            ts = item.get("block_timestamp")

        if ts is None:
            return None

        return self._to_rfc3339(ts)


    def calculate_ingest_timestamp(self):
        """
        System ingestion time
        """
        # Get current UTC time and convert to Unix timestamp in seconds
        timestamp_seconds = datetime.utcnow().timestamp()
        
        # Convert to milliseconds
        timestamp_milliseconds = int(timestamp_seconds * 1000)
        
        return timestamp_milliseconds


    def _to_rfc3339(self, ts):
        return datetime.utcfromtimestamp(int(ts)).isoformat() + "Z"
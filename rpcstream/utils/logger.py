import json
import time
import logging


class JsonLogger:
    
    LEVELS = {
        "debug": 10,
        "info": 20,
        "warn": 30,
        "error": 40,
    }
    
    def __init__(self, name="rpcstream", level="INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level.upper())
        self.level = level.lower()

        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.handlers = [handler]

    def isEnabledFor(self, level_num):
        return level_num >= self.LEVELS[self.level]

    def _log(self, level, message, **kwargs):
        log = {
            "level": level,
            "time": int(time.time() * 1000),
            "message": message,
            **kwargs
        }

        self.logger.log(getattr(logging, level.upper()), json.dumps(log))

    def debug(self, message, **kwargs):
        self._log("debug", message, **kwargs)

    def info(self, message, **kwargs):
        self._log("info", message, **kwargs)

    def warn(self, message, **kwargs):
        self._log("warn", message, **kwargs)

    def error(self, message, **kwargs):
        self._log("error", message, **kwargs)
import logging
import sys

from .request_context import get_correlation_id, get_user_id


class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return (
            f'UserID: {self.extra.get("user_id", "N/A")} | '
            f'CorrelationID: {self.extra.get("correlation_id", "N/A")} | '
            f'{msg}',
            kwargs,
        )


_LOGGER_NAME = "zpc.post"
_configured = False


def _get_logger() -> logging.Logger:
    global _configured
    logger = logging.getLogger(_LOGGER_NAME)
    if not _configured:
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(levelname)s - %(message)s",
                    datefmt="%m/%d/%Y %I:%M:%S %p",
                )
            )
            logger.addHandler(handler)
        logger.propagate = True
        _configured = True
    return logger


def log_msg(level: str, message: str, user_id: str = None, correlation_id: str = None):
    extra = {
        "user_id": user_id or get_user_id() or "N/A",
        "correlation_id": correlation_id or get_correlation_id() or "N/A",
    }
    adapter = CustomAdapter(_get_logger(), extra)
    level = (level or "info").lower()
    if level == "warn":
        level = "warning"
    log_methods = {
        "debug": adapter.debug,
        "info": adapter.info,
        "warning": adapter.warning,
        "error": adapter.error,
        "critical": adapter.critical,
    }
    log_methods.get(level, adapter.info)(message)
    print(f"{level.upper()}: {extra['user_id']} | {message}", flush=True)

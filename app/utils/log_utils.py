import logging


class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return (
            f'UserID: {self.extra.get("user_id", "N/A")} | '
            f'CorrelationID: {self.extra.get("correlation_id", "N/A")} | '
            f'{msg}',
            kwargs,
        )


def log_msg(level: str, message: str, user_id: str = None, correlation_id: str = None):
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
        level=logging.INFO,
    )
    logger = logging.getLogger("custom_logger")
    extra = {
        "user_id": user_id if user_id else "N/A",
        "correlation_id": correlation_id if correlation_id else "N/A",
    }
    adapter = CustomAdapter(logger, extra)
    level = level.lower()
    log_methods = {
        "debug": adapter.debug,
        "info": adapter.info,
        "warning": adapter.warning,
        "error": adapter.error,
        "critical": adapter.critical,
    }

    if level in log_methods:
        log_methods[level](message)
    else:
        raise ValueError(f"Invalid logging level: {level}")



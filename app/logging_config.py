import logging
import sys

import structlog


def setup_logging(service_name: str) -> None:
    timestamper = structlog.processors.TimeStamper(fmt="iso")

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.INFO,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,      # поддержка контекста
            structlog.stdlib.add_log_level,               # добавляет level
            timestamper,                                  # добавляет timestamp
            structlog.processors.JSONRenderer(),          # финально — JSON
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        cache_logger_on_first_use=True,
    )

    
    logger = structlog.get_logger(service=service_name)
    logger.info("service_started")
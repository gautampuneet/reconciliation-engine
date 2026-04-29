from __future__ import annotations

import logging


def get_logger() -> logging.Logger:
    logger = logging.getLogger("reconciliation_engine")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s"
            " match_rate=%(match_rate)s sla_breach=%(sla_breach)s"
            " matched=%(matched_count)s timing=%(timing_count)s variance=%(variance_count)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

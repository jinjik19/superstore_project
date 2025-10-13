import logging


def create_logger() -> logging.Logger:
    """Create custom logger."""
    logger = logging.Logger("crypto_tracker")

    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="{asctime} - {levelname} - {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.setLevel("INFO")

    return logger

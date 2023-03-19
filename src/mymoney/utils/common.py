import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


def raise_or_log(
    message: str,
    logs: bool = True,
    raises: bool = False,
    exception_type = Exception,
):
    """Raise an exception or log a message.

    Args:
        message (str):
            The message that should be attached.
        logs (bool):
            Whether to log the results if something went wrong.
        raises (bool):
            Whether to raise an error or not.
        exception_type:
            The exception that should be raised.
    """
    if logs:
        logging.warning(message)
    if raises:
        raise exception_type(message)

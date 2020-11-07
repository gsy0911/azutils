from datetime import datetime


def convert_datetime_to_milli_epoch(input_time):
    """
    Convert str or epoch-int to epoch-milli-int

    Args:
        input_time: ISO8601-format datetime, or epochtime

    Returns:

    """
    if type(input_time) is str:
        start_timestamp = int(datetime.fromisoformat(input_time).timestamp() * 1000)
        return start_timestamp
    elif type(input_time) is int:
        if len(str(input_time)) == 13:
            return input_time
        elif len(str(input_time)) == 10:
            return input_time * 1000

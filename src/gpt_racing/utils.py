#!/usr/bin/env python3


def seconds_to_str(seconds, precision=3):
    negative = False
    if seconds < 0:
        seconds *= -1
        negative = True

    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    # seconds, remainder = divmod(remainder, 1)
    hours = int(round(hours))
    minutes = int(round(minutes))

    if hours == 0 and minutes == 0:
        out = f"{seconds:.{precision}f}"
    elif hours == 0:
        out = f"{minutes}:{seconds:0{precision+3}.{precision}f}"
    else:
        out = f"{hours}:{minutes:02d}:{seconds:0{precision+3}.{precision}f}"

    if negative:
        out = "-" + out

    return out

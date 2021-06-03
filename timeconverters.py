from datetime import datetime, timedelta


def convert_days_since_1950_to_datetime(days_since_1950: float) -> datetime:
    return datetime(1950, 1, 1) + timedelta(days_since_1950)

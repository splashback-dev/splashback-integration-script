from datetime import datetime, timedelta

from dateutil import parser


def convert_days_since_1950_to_datetime(days_since_1950: float) -> datetime:
    return datetime(1950, 1, 1) + timedelta(days_since_1950)


def convert_bom_date_time_full_to_datetime(bom_date_time_full: str) -> datetime:
    return parser.parse(bom_date_time_full)

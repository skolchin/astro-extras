# Astro SDK Extras project
# (c) kol, 2023

""" Timezone conversion routines """

from datetime import datetime, timedelta
from dateutil.tz import tzlocal, tzutc
from dateutil.relativedelta import relativedelta

def localnow() -> datetime:
    """ Return datetime.now() in a local timezone (TZ-aware) """
    return datetime.now(tzlocal())

def datetime_is_tz_aware(dt: datetime) -> bool:
    """ Check datetime is TZ-aware """
    return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None

def datetime_to_local(dt: datetime) -> datetime:
    """ Convert datetime of any type (TZ-aware or TZ-naive) to local TZ """
    return datetime_to_tz(dt, tzlocal())

def datetime_to_utc(dt: datetime) -> datetime:
    """ Convert datetime of any type (TZ-aware or TZ-naive) to UTC """
    return datetime_to_tz(dt, tzutc())

def datetime_to_tz(dt: datetime, tz=None) -> datetime:
    """ Convert datetime of any type (TZ-aware or TZ-naive) to given timezone """
    if not dt:
        return None
    try:
        return dt.astimezone(tz)
    except (ValueError,TypeError):
        return dt.replace(tzinfo=tz)

def datetime_to_naive(dt: datetime) -> datetime:
    """ Remove TZ component making datetime TZ-naive """
    if not dt:
        return None
    return dt.replace(tzinfo=None)

def days_ago(n_days: int) -> datetime:
    """ Returns TZ-aware date/time `n_days` days in the past from now """
    return localnow() - timedelta(days=n_days)

def months_ago(n_months: int) -> datetime:
    """ Returns TZ-aware date/time `n_months` months in the past from now """
    return localnow() - relativedelta(months=n_months)

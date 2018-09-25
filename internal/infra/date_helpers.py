import datetime
from dateutil.parser import parse as parse_datetime_local
import functools
from pytz import UTC

parse_datetime = lambda str: parse_datetime_local(str).replace(tzinfo=UTC)
parse_datetime_for_df = lambda str: parse_datetime_local(str)
create_datetime_as_utc = functools.partial(datetime.datetime, tzinfo=UTC)

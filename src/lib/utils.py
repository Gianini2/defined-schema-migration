from datetime import datetime
from dateutil import parser as dateutil_parser


def date_parser(date_str: str | None) -> datetime | None:
  """Parses a date string, returning None on failure."""
  try:
    return dateutil_parser.parse(date_str)
  except (dateutil_parser.ParserError, TypeError):
    return None

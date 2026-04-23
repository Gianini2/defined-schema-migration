import csv
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from .models import RawUnit, RawRentRoll


def csv_loader(session: Session, file_path: str, model_class: type[RawUnit] | type[RawRentRoll]) -> int:
  """Reads a CSV into the given raw ORM model and verifies the inserted count."""
  try:
    with open(file_path, newline='', encoding='utf-8') as f:
      instances = [model_class.from_csv_row(row) for row in csv.DictReader(f)]

    print(f"\n-- Processing {model_class.__table__.schema}.{model_class.__tablename__} --\n")
    print(f"CSV read successfully from {file_path}. Rows: {len(instances)}\n")

    session.add_all(instances)
    session.commit()

    actual = session.scalar(select(func.count()).select_from(model_class))
    if actual != len(instances):
      raise Exception(f"Verification failed: expected {len(instances)}, found {actual} rows.")

    print(f"Verification successful for {model_class.__tablename__}. Row count: {actual}\n")
    return actual

  except Exception as e:
    session.rollback()
    raise Exception(f"Error in csv_loader for {model_class.__tablename__}: {e}") from e


def raw_pipeline(engine, session: Session) -> bool:
  """Resets raw staging tables and loads source CSVs."""
  try:
    # Drop and recreate raw staging tables to ensure a clean schema on every run
    with engine.begin() as conn:
      RawUnit.__table__.drop(conn, checkfirst=True)
      RawUnit.__table__.create(conn)
      RawRentRoll.__table__.drop(conn, checkfirst=True)
      RawRentRoll.__table__.create(conn)

    csv_loader(session, 'data/unit.csv', RawUnit)
    csv_loader(session, 'data/rentRoll.csv', RawRentRoll)
    return True
  except Exception as e:
    print(f"Error during raw_pipeline() execution: {e}")
    return False

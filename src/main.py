import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from lib.config import get_secrets
from lib.raw_tools import raw_pipeline
from lib.silver_tools import silver_main_pipeline


def main():
  """Runs the full ingestion pipeline: raw staging → silver."""
  engine = create_engine(get_secrets())

  with Session(engine) as session:
    step1_flag = raw_pipeline(engine, session)

    print("--------------------------------------------------------\n")

    if not step1_flag:
      sys.exit(1)

    if not silver_main_pipeline(engine, session):
      sys.exit(1)


if __name__ == "__main__":
  main()

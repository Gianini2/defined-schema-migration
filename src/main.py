import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from lib.config import get_secrets
from lib.raw_tools import raw_pipeline
from lib.silver_tools import silver_main_pipeline

def main():
  """Runs the full ingestion pipeline: raw staging → silver."""
  
  ## Create engine to communicate with PostgreSQL:
  engine = create_engine(get_secrets())

  ## Run the pipelines:
  with Session(engine) as session:
    
    ## First step: populate raw tables
    step1_flag = raw_pipeline(engine, session)

    print("--------------------------------------------------------\n")

    ## Second step: build and load silver tables, with validation. Run only if step 1 succeeded.
    if not step1_flag:
      sys.exit(1)

    if not silver_main_pipeline(engine, session):
      sys.exit(1)


if __name__ == "__main__":
  main()
import sys
from sqlalchemy import create_engine
from lib.raw_tools import raw_pipeline, get_secrets
from lib.silver_tools import silver_main_pipeline


def main():
  engine = create_engine(get_secrets()['database_url'])

  step1_flag = raw_pipeline(engine)

  print("--------------------------------------------------------\n")

  if not step1_flag:
    sys.exit(1)

  if not silver_main_pipeline(engine):
    sys.exit(1)


if __name__ == "__main__":
  main()

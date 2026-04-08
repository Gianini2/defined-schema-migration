import pandas as pd
from dotenv import load_dotenv
import os


def get_secrets() -> dict:
  '''Abstraction of the security layer for retrieving any secrets.'''

  ## IMPROVEMENT: Can be replaced by standard Auth methods

  load_dotenv()
  database_url = os.getenv('DATABASE_URL')


  if not database_url:
    raise Exception("secrets not found in environment variables.")

  secrets = {
    'database_url': database_url,
    ## Can contain other secrets
  }

  return secrets


class raw_tools:
  '''A class for data ingestion tools. Currently supports CSV files.'''
  def __init__(self):
    pass

  @staticmethod
  def csv_reader(
    file_path,
    **kwargs
    ) -> pd.DataFrame:
    '''Reads a CSV file and returns a DataFrame. Accepts additional keyword arguments for pd.read_csv()'''
    try:
      data = pd.read_csv(file_path, **kwargs)
      print(f".csv file read successfully from {file_path}. Shape: {data.shape}\n")
      return data
    except Exception as e:
      raise Exception(f"Error during csv_reader: {e}")

  @staticmethod
  def dataframe_schema_definer(
    dataframe: pd.DataFrame,
    data_types: dict
    ) -> pd.DataFrame:
    '''Defines the schema of a DataFrame by converting columns to specified data types.'''
    try:
      for column, dtype in data_types.items():
        dataframe[column] = dataframe[column].astype(dtype, errors='ignore')
      print(f"DataFrame schema defined successfully.\n")
      return dataframe
    except Exception as e:
      raise Exception(f"Error during dataframe_schema_definer: {e}")

  @staticmethod
  def raw_data_loader(
    data: pd.DataFrame,
    table_name: str,
    schema_name: str,
    engine,
    if_exists: str = 'replace'
    ) -> int:
    '''Function that loads a pandas dataframe to postgres table'''
    try:
      result = data.to_sql(name=table_name, schema=schema_name, con=engine, if_exists=if_exists, index=False)
      print(f"Data loaded successfully into {table_name} table. Rows affected: {result}\n")
      return result
    except Exception as e:
      raise Exception(f"Error during raw_data_loader: {e}")

  @staticmethod
  def raw_verification(
    engine,
    table_name: str,
    schema_name: str,
    expected_rows: int,
    ) -> bool:
    '''Function that verifies the data loaded to postgres by querying the database metadata'''
    try:
      result = pd.read_sql_query(
        f'SELECT COUNT(*) as row_count FROM "{schema_name}"."{table_name}"',
        engine
      )
      actual_rows = result['row_count'].values[0]

      if actual_rows != expected_rows:
        raise Exception(
          f"Table length verification failed: Expected {expected_rows} rows, "
          f"but found {actual_rows} rows in {schema_name}.{table_name} table."
        )

      print(f"Raw_verification successful for {schema_name}.{table_name} table. Row count: {actual_rows}, Expected: {expected_rows}\n")
      return True

    except Exception as e:
      print(f"raw_verification failed for {schema_name}.{table_name} table. Error: {e}")
      return False


class raw_data(raw_tools):
  '''
  Class for processing and storing raw data

  Properties:
    - `table_name`: str - name of the table to load data into
    - `schema_name`: str - name of the schema to load data into
  '''
  def __init__(
    self,
    table_name: str,
    schema_name: str,
    ) -> None:
    self.table_name: str = table_name
    self.schema_name: str = schema_name

  def ingest_data_structured(
    self,
    file_path: str,
    engine,
    table_schema_mapping: dict,
    ):
    '''Ingest data from CSV, stores locally (Runtime)'''

    ingestion_step = 0

    try:

      print(f"\n-- Processing {self.schema_name}.{self.table_name} table ingestion and verification. --\n")

      ## Create ingested dataframe
      self.dataframe = self.csv_reader(file_path) ## step 0
      ingestion_step += 1
      self.typed_dataframe = self.dataframe_schema_definer(self.dataframe, table_schema_mapping) ## step 1
      ingestion_step += 1

      ## Let's say a problem occours here, you can start the verification from here. E.g. Airflow shows you the task that failed
      ## raise(Exception("aaaaaaa"))

      ## Load data to Postgres and verify
      self.row_count_expectation = self.raw_data_loader(
        data=self.typed_dataframe,
        table_name=self.table_name,
        schema_name=self.schema_name,
        engine=engine
        ) ## step 2
      ingestion_step += 1

      self.verification_flag = self.raw_verification(
        engine=engine,
        table_name=self.table_name,
        schema_name=self.schema_name,
        expected_rows=self.row_count_expectation
      ) ## step 3

      print(f"Data ingestion completed for {self.schema_name}.{self.table_name} table. Verification flag: {self.verification_flag}\n")

    except Exception as e:
      raise Exception(f"Error during ingest_data_structured: {e}. Stopped at ingestion step: {ingestion_step}")


def raw_pipeline(engine) -> bool:
  '''Runs the initial ingestion piece'''
  try:
    unit = raw_data(table_name='unit', schema_name='monument_raw')
    unit_data_types = {
      'facilityName': 'string',
      'unitNumber': 'int64',
      'unitSize': 'string',
      'unitType': 'string',
    }
    unit.ingest_data_structured(
      file_path='data/unit.csv',
      engine=engine,
      table_schema_mapping=unit_data_types,
    )

    rent_roll = raw_data(table_name='rentRoll', schema_name='monument_raw')
    rent_roll_data_types = {
      'facilityName': 'string',
      'unitNumber': 'int64',
      'firstName': 'string',
      'lastName': 'string',
      'phone': 'string',
      'email': 'string',
      'rentStartDate': 'string',
      'rentEndDate': 'string',
      'monthlyRent': 'float32',
      'currentRentOwed': 'float32',
    }
    rent_roll.ingest_data_structured(
      file_path='data/rentRoll.csv',
      engine=engine,
      table_schema_mapping=rent_roll_data_types,
    )

    return True
  except Exception as e:
    print(f"Error during raw_pipeline() execution: {e}")
    return False

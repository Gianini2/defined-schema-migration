from lib.raw_tools import raw_data, get_secrets
from lib.silver_tools import silver_pipeline
from sqlalchemy import create_engine


def raw_pipeline():
  '''Runs the initial ingestion piece'''
  try:
    connection_string = get_secrets()['database_url']

    ## unit.csv data:

    unit = raw_data(
      table_name='unit',
      schema_name='monument_raw'
      )

    unit_data_types = {
      'facilityName': 'string',
      'unitNumber': 'int64',
      'unitSize': 'string',
      'unitType': 'string',
      }

    unit.ingest_data_structured(
      file_path='data/unit.csv',
      connection_string=connection_string,
      table_schema_mapping=unit_data_types,
      ) 

    ## rentRoll.csv data:

    rentRoll = raw_data(
      table_name='rentRoll',
      schema_name='monument_raw'
      )

    rentRoll_data_types = {
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

    rentRoll.ingest_data_structured(
      file_path='data/rentRoll.csv',
      connection_string=connection_string,
      table_schema_mapping=rentRoll_data_types,
      )
  
    return True
  except Exception as e:
    print(f"Error during raw_pipeline() execution: {e}")
    return False

def silver_main_pipeline():
  '''Creates and populates the defined tables'''
  try:
    flag = silver_pipeline()    
    if flag:
      print("Ingestion process completed successfully.")
    return flag
  except Exception as e:
    print(f"Error during silver_main_pipeline() execution: {e}")
    return False

if __name__ == "__main__":
  
  ## Main run:    
  step1_flag = raw_pipeline()
  
  print("--------------------------------------------------------\n")
  
  if step1_flag:
    silver_main_pipeline()
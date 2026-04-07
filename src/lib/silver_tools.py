import pandas as pd
from dateutil import parser
from sqlalchemy import create_engine
from .raw_tools import get_secrets, raw_tools
from time import sleep


def date_parser(date_str):
  '''Simple dateparser'''
  try:
    return parser.parse(date_str)
  except (parser.ParserError, TypeError):
    return None

## monument.facility table:
def build_facility_table(
  raw_unit: pd.DataFrame
  ) -> pd.DataFrame:
  '''Builds the facility table.'''
  
  try:
    ## Logic to build the table:
    facility_df = raw_unit.copy()
    facility_df = raw_unit.groupby('facilityName').agg({'facilityName': 'first'}).reset_index(drop=True) ## doesn't include Nulls by default
    facility_df.rename(columns={'facilityName': 'name'}, inplace=True)
    facility_df['facilityid'] = facility_df.index + 1 ## IMPROVEMENT: pegar o tamanho da tabela já no banco e somar +1
    facility_df = facility_df[['facilityid', 'name']]
  except Exception as e:
    raise Exception(f"Error during build_facility_table() while applying logic: {e}")
  
  try:
    ## Validates data schema:
    facility_df['name'] = facility_df['name'].astype(str) 
    if facility_df['name'].str.len().max() > 100:
      exceptions = facility_df[facility_df['name'].str.len() > 100]['name'].tolist()
      raise Exception(f"Facility Name has more than 100 characters: {exceptions}")
  except Exception as e:
    raise Exception(f"Error during build_facility_table() while validating schema: {e}")
  
  return facility_df


## monument.unit table:
def build_unit_table(
  facility_df: pd.DataFrame,
  raw_unit: pd.DataFrame,
  raw_rentRoll: pd.DataFrame
  ):
  '''Builds the unit table.'''
  
  try:
    ## Logic to build the table:
    unit_silver_df = raw_unit.copy()
    unit_silver_df = unit_silver_df.dropna(subset=['unitNumber', 'facilityName'])
    unit_silver_df['unitSize'] = unit_silver_df['unitSize']\
      .str.replace('  ', ' ')\
      .str.replace('(', '')\
      .str.replace(')', '')\
      .str.upper()\
      .str.strip()\
      .str.split(' ')
    unit_silver_df['unitSize'] = unit_silver_df['unitSize'].apply(
      lambda sizearray: [dimension.split('X') for dimension in sizearray] if sizearray is not None else None
    )
    unit_silver_df['unitwidth'] = unit_silver_df['unitSize'].apply(
      lambda sizearray: sizearray[0][sizearray[1].index('W')] if sizearray is not None else None
    )
    unit_silver_df['unitlength'] = unit_silver_df['unitSize'].apply(
      lambda sizearray: sizearray[0][sizearray[1].index('L')] if sizearray is not None else None
    )
    unit_silver_df['unitheight'] = unit_silver_df['unitSize'].apply(
      lambda sizearray: sizearray[0][sizearray[1].index('H')] if sizearray is not None else None
    )
    unit_silver_df.drop(columns=['unitSize'], inplace=True)
    unit_silver_df['unitNumber'] = unit_silver_df['unitNumber'].astype(int)
    unit_silver_df = unit_silver_df.merge(
      raw_rentRoll[['facilityName', 'unitNumber', 'monthlyRent']].drop_duplicates(),
      on=['facilityName', 'unitNumber'],
      how='left'
    )
    unit_silver_df = unit_silver_df.merge(
      facility_df[['facilityid', 'name']],
      left_on='facilityName',
      right_on='name',
      how='left'
    )
    unit_silver_df = unit_silver_df.sort_values(by=['facilityName','unitNumber']).reset_index(drop=True)
    unit_silver_df = unit_silver_df.drop_duplicates().reset_index(drop=True)
    unit_silver_df['unitid'] = unit_silver_df.index + 1 ## IMPROVEMENT: pegar o tamanho da tabela já no banco e somar +1
    unit_silver_df.drop(columns=['facilityName','name'], inplace=True)
    unit_silver_df.rename(columns={
      'unitNumber': 'number',
      'unitType': 'unittype',
      'monthlyRent': 'monthlyrent'
    }, inplace=True)
    unit_silver_df = unit_silver_df[['unitid','facilityid', 'number', 'unitwidth', 'unitlength', 'unitheight', 'unittype', 'monthlyrent']]
    return unit_silver_df
  
  except Exception as e:
    raise Exception(f"Error during build_unit_table() while applying logic: {e}")
  
  
## monument.tenant table:
def build_tenant_table(
  raw_rentRoll: pd.DataFrame,
):
  '''Builds the tenant table.'''
  
  try:
    tenant_df = raw_rentRoll.copy()
    tenant_df= tenant_df.drop_duplicates().reset_index(drop=True)
    tenant_df['tenantid'] = tenant_df.index + 1 ## IMPROVEMENT: pegar o tamanho da tabela já no banco e somar +1
    tenant_df.rename(columns={
      'firstName': 'firstname',
      'lastName': 'lastname'
    }, inplace=True)
    ## take out any special charaacter, doesn't cover numbers with less then 9 or more than 10 digits yet:
    tenant_df['phone'] = tenant_df['phone'].str.replace(r'[^\w\s]', '', regex=True).str.replace(' ', '')
    tenant_df = tenant_df[['tenantid', 'firstname', 'lastname', 'phone', 'email']] ## No email treatment here
    
    return tenant_df

  except Exception as e:
    raise Exception(f"Error during build_tenant_table() while applying logic: {e}")
  

## monument.rentalContract table:
def build_rentalContract_table(
  raw_rentRoll: pd.DataFrame,
  facility_silver_df: pd.DataFrame,
  unit_silver_df: pd.DataFrame,
  tenant_silver_df: pd.DataFrame
):
  '''Builds the rental contract table.'''
  
  try:
    
    unit_silver_df = unit_silver_df.copy()
    unit_silver_df = unit_silver_df.merge(
      facility_silver_df[['facilityid', 'name']],
      left_on='facilityid',
      right_on='facilityid',
      how='left'
    )
    unit_silver_df.rename(columns={'name': 'facilityname'}, inplace=True)
    
    rental_contract_df = raw_rentRoll.copy()
    rental_contract_df = rental_contract_df.dropna(subset=['rentStartDate'])
    
    ## join with tenant for tenantId
    rental_contract_df = rental_contract_df.merge(
      tenant_silver_df[['firstname', 'lastname', 'email', 'tenantid']],
      left_on=['firstName', 'lastName', 'email'],
      right_on=['firstname', 'lastname', 'email'],
      how='left'
    )
    
    ## join with unit for unitId
    rental_contract_df = rental_contract_df.merge(
      unit_silver_df[['facilityid', 'facilityname', 'number', 'unitid']],
      left_on=['facilityName', 'unitNumber'],
      right_on=['facilityname', 'number'],
      how='left'
    )
        
    rental_contract_df['rentalContractid'] = rental_contract_df.index + 1 ## IMPROVEMENT: pegar o tamanho da tabela já no banco e somar +1
    rental_contract_df.rename(columns={
      'rentStartDate': 'startdate',
      'rentEndDate': 'enddate',
      'currentRentOwed': 'currentamountowed',
      'rentalContractid': 'rentalcontractid'
    }, inplace=True)
    
    # rental_contract_df.dropna(subset=['unitid', 'tenantid'], inplace=True)
    
    rental_contract_df = rental_contract_df[['rentalcontractid','unitid','tenantid','startdate', 'enddate', 'currentamountowed']]
    
    return rental_contract_df

  except Exception as e:
    raise Exception(f"Error during build_rentalContract_table() while applying logic: {e}")
  

## monument.rentalInvoice table:
def build_rentalInvoice_table(
  raw_rentRoll: pd.DataFrame,
  rental_contract_silver_df: pd.DataFrame,
  unit_silver_df: pd.DataFrame,
):
  '''Builds the rental invoice table.'''
  try:
    
    ## Preparing data for collecting the rentalContractid
    rental_contract_silver_df = rental_contract_silver_df.copy()
    rental_contract_silver_df = rental_contract_silver_df.merge(
      unit_silver_df[['monthlyrent', 'unitid', 'number']],
      left_on='unitid',
      right_on='unitid',
      how='left'
    )
  
    rental_invoice_df = raw_rentRoll.copy()
    rental_invoice_df['invoiceduedate'] = rental_invoice_df['rentStartDate'].apply(
      lambda x: date_parser(x)
    )
    rental_invoice_df['invoiceduedate'] = rental_invoice_df['invoiceduedate'] + pd.offsets.MonthBegin(1) ## first day of the next month
    
    # As I did join by unitid before, I'll be having "unique unit numbers" at this step
    rental_invoice_df = rental_invoice_df.merge(
      rental_contract_silver_df[['rentalcontractid', 'number', 'monthlyrent']],
      left_on='unitNumber',
      right_on='number',
      how='left'
    )
    
    rental_invoice_df['invoiceid'] = rental_invoice_df.index + 1 ## IMPROVEMENT: pegar o tamanho da tabela já no banco e somar +1
    
    rental_invoice_df.rename(columns={
      'monthlyrent': 'invoiceamount',
      'currentRentOwed': 'invoicebalance',
      'rentalContractid': 'rentalcontractid',
    }, inplace=True)
    
    # rental_invoice_df.dropna(subset=['rentalcontractid'], inplace=True)
    
    rental_invoice_df = rental_invoice_df[['invoiceid', 'rentalcontractid', 'invoiceduedate', 'invoiceamount', 'invoicebalance']]
    return rental_invoice_df
  
  except Exception as e:
    raise Exception(f"Error during build_rentalInvoice_table() while applying logic: {e}")
  

###### Pipeline functions:

def silver_data_load(
  data: pd.DataFrame, 
  table_name: str,
  schema_name: str, 
  connection_string: str, 
  if_exists: str = 'append'
  ):
  '''Load data into silver layer'''
  try:
    engine = create_engine(connection_string)
    rows_affected = data.to_sql(name=table_name, schema=schema_name, con=engine, if_exists=if_exists, index=False)
    print(f"Data loaded successfully into {table_name} table. Rows affected: {rows_affected}\n")
      
    ## Fazer validação do insert
   
    return rows_affected
  except Exception as e:
    raise Exception(f" >>>> Error during silver_data_load(): {e}")
    ## to_sql method will present any error before actually insert any data (so, rollback is 'automatic')

def retreive_raw_data():
  '''Retrieve data from raw layer'''
  
  connection_string = get_secrets()['database_url']
  engine = create_engine(connection_string)
  
  ## Option, can change the methods here to pull directly from runtime, instead of pulling from SQL Database
  raw_unit_df = pd.read_sql_table(table_name='unit', schema='monument_raw', con=engine)
  raw_rentRoll_df = pd.read_sql_table(table_name='rentRoll', schema='monument_raw', con=engine)
  
  if raw_unit_df.empty or raw_rentRoll_df.empty:
    raise Exception("One of the raw tables is empty. Please check the raw layer.")
  
  return raw_unit_df, raw_rentRoll_df


def silver_verification(
    connection_string: str,
    table_name: str,
    schema_name: str,
    expected_rows: int,
    ) -> bool:
    '''Function that verifies the data loaded to postgres by querying the database metadata'''
    
    ## a bit messy because I didn't call them a good name in time (lack of time)
    success = raw_tools.raw_verification(
      connection_string=connection_string,
      table_name=table_name,
      schema_name=schema_name,
      expected_rows=expected_rows
    )
    if not success:
      raise Exception(f"Verification failed for {schema_name}.{table_name} table. Expected rows: {expected_rows}")
    
    return True
      

def silver_pipeline():
  '''Function that runs the silver pipeline.'''
  try:
    
    connection_string = get_secrets()['database_url']
    
    raw_unit_df, raw_rentRoll_df = retreive_raw_data()
    ## Creating tables (as pandas dataframes)
    ## If a problem occours in this step, no data will be uploaded.
    facility_silver_df = build_facility_table(raw_unit_df)
    unit_silver_df = build_unit_table(facility_silver_df, raw_unit_df, raw_rentRoll_df)
    tenant_silver_df = build_tenant_table(raw_rentRoll_df)
    rental_contract_silver_df = build_rentalContract_table(raw_rentRoll_df, facility_silver_df, unit_silver_df, tenant_silver_df)
    rental_invoice_silver_df = build_rentalInvoice_table(raw_rentRoll_df, rental_contract_silver_df, unit_silver_df)

    ## Loading tables to silver layer:
    ## If a problem occours in this block, i.e. on the "tenant_silver_df", it will not load problematic data, 
    ## the previous data will be correct, and the next tables wil be blank yet.
    ## I've separated individually the loads since we could be calling these "Lambda" functions from a queue, and resume the queue from where it failed.
    facility_silver_exp_rows = silver_data_load(data=facility_silver_df,table_name='facility',schema_name='monument', connection_string=connection_string)
    unit_silver_exp_rows = silver_data_load(data=unit_silver_df,table_name='unit',schema_name='monument', connection_string=connection_string)
    tenant_silver_exp_rows = silver_data_load(data=tenant_silver_df,table_name='tenant',schema_name='monument', connection_string=connection_string)
    # raise(Exception("ISSUE HEREEE")) ## Feel free to test here
    rental_contract_silver_exp_rows = silver_data_load(data=rental_contract_silver_df, table_name='rentalContract', schema_name='monument', connection_string=connection_string)
    rental_invoice_silver_exp_rows = silver_data_load(data=rental_invoice_silver_df,  table_name='rentalInvoice',  schema_name='monument', connection_string=connection_string)

    print("###### Now validating the data for the monument schema ######")

    ### Validating data load:
    sleep(10) # to enable time for postgreSQL to refresh
    silver_verification(connection_string=connection_string, table_name='facility', schema_name='monument', expected_rows=facility_silver_exp_rows)
    silver_verification(connection_string=connection_string, table_name='unit', schema_name='monument', expected_rows=unit_silver_exp_rows)
    silver_verification(connection_string=connection_string, table_name='tenant', schema_name='monument', expected_rows=tenant_silver_exp_rows)
    silver_verification(connection_string=connection_string, table_name='rentalContract', schema_name='monument', expected_rows=rental_contract_silver_exp_rows)
    silver_verification(connection_string=connection_string, table_name='rentalInvoice', schema_name='monument', expected_rows=rental_invoice_silver_exp_rows)

    return True

  except Exception as e:
    raise Exception(f"Error during silver_pipeline(): {e}")
import pandas as pd
from dateutil import parser
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from .models import Facility, Unit, Tenant, RentalContract, RentalInvoice


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
    ## take out any special charaacter, doesn't cover numbers with less then 9 or more than 10 digits yet:
    tenant_df['phone'] = tenant_df['phone'].str.replace(r'[^\w\s]', '', regex=True).str.replace(' ', '')
    tenant_df.rename(columns={
      'firstName': 'firstname',
      'lastName': 'lastname'
    }, inplace=True)
    tenant_df['firstname'] = tenant_df['firstname'].str.strip()
    tenant_df['lastname'] = tenant_df['lastname'].str.strip()
    tenant_df = tenant_df[['firstname', 'lastname', 'phone', 'email']]
    tenant_df = tenant_df.drop_duplicates().reset_index(drop=True)
    tenant_df['tenantid'] = tenant_df.index + 1 ## IMPROVEMENT: pegar o tamanho da tabela já no banco e somar +1

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

    rental_contract_df['firstName'] = rental_contract_df['firstName'].str.strip()
    rental_contract_df['lastName'] = rental_contract_df['lastName'].str.strip()

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

    rental_contract_df = rental_contract_df[['rentalcontractid', 'unitid', 'tenantid', 'startdate', 'enddate', 'currentamountowed']]
    rental_contract_df['startdate'] = rental_contract_df['startdate'].apply(date_parser)
    rental_contract_df['enddate'] = rental_contract_df['enddate'].apply(date_parser)

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
    rental_invoice_df['invoiceduedate'] = rental_invoice_df['rentStartDate'].apply(date_parser)
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

    rental_invoice_df = rental_invoice_df[['invoiceid', 'rentalcontractid', 'invoiceduedate', 'invoiceamount', 'invoicebalance']]
    return rental_invoice_df

  except Exception as e:
    raise Exception(f"Error during build_rentalInvoice_table() while applying logic: {e}")


###### Pipeline functions:

def retreive_raw_data(engine):
  '''Retrieve data from raw layer'''

  ## Option: can change to pull directly from runtime instead of pulling from the SQL Database
  raw_unit_df = pd.read_sql_table(table_name='unit', schema='monument_raw', con=engine)
  raw_rentRoll_df = pd.read_sql_table(table_name='rentRoll', schema='monument_raw', con=engine)

  if raw_unit_df.empty or raw_rentRoll_df.empty:
    raise Exception("One of the raw tables is empty. Please check the raw layer.")

  return raw_unit_df, raw_rentRoll_df


def df_to_models(df: pd.DataFrame, model_class) -> list:
  '''Converts a DataFrame to a list of ORM model instances, replacing NaN with None.'''
  records = df.where(pd.notna(df), other=None).to_dict('records')
  return [model_class(**row) for row in records]


def silver_bulk_load(session: Session, instances: list, table_name: str) -> int:
  '''Bulk inserts ORM model instances and commits the transaction.'''
  try:
    session.add_all(instances)
    session.commit()
    count = len(instances)
    print(f"Data loaded successfully into {table_name} table. Rows affected: {count}\n")
    return count
  except Exception as e:
    session.rollback()
    raise Exception(f">>>> Error during silver_bulk_load() for {table_name}: {e}")


def silver_verification(session: Session, model_class, expected_rows: int) -> bool:
  '''Verifies the loaded row count via ORM COUNT query.'''
  count = session.scalar(select(func.count()).select_from(model_class))
  if count != expected_rows:
    raise Exception(
      f"Verification failed for {model_class.__tablename__}. "
      f"Expected {expected_rows} rows, found {count}."
    )
  print(f"Verification successful for {model_class.__tablename__}. Row count: {count}\n")
  return True


def silver_pipeline(engine, session: Session):
  '''Function that runs the silver pipeline.'''
  try:

    raw_unit_df, raw_rentRoll_df = retreive_raw_data(engine)

    ## Build DataFrames — no DB writes in this block; a failure here uploads nothing.
    facility_silver_df = build_facility_table(raw_unit_df)
    unit_silver_df = build_unit_table(facility_silver_df, raw_unit_df, raw_rentRoll_df)
    tenant_silver_df = build_tenant_table(raw_rentRoll_df)
    rental_contract_silver_df = build_rentalContract_table(raw_rentRoll_df, facility_silver_df, unit_silver_df, tenant_silver_df)
    rental_invoice_silver_df = build_rentalInvoice_table(raw_rentRoll_df, rental_contract_silver_df, unit_silver_df)

    ## Load tables — each commit is independent so a failure leaves prior tables intact.
    ## Insertion order follows FK dependency: facility → unit/tenant → rentalContract → rentalInvoice
    facility_count = silver_bulk_load(session, df_to_models(facility_silver_df, Facility), 'facility')
    unit_count = silver_bulk_load(session, df_to_models(unit_silver_df, Unit), 'unit')
    tenant_count = silver_bulk_load(session, df_to_models(tenant_silver_df, Tenant), 'tenant')
    rental_contract_count = silver_bulk_load(session, df_to_models(rental_contract_silver_df, RentalContract), 'rentalContract')
    rental_invoice_count = silver_bulk_load(session, df_to_models(rental_invoice_silver_df, RentalInvoice), 'rentalInvoice')

    print("###### Now validating the data for the monument schema ######\n")

    silver_verification(session, Facility, facility_count)
    silver_verification(session, Unit, unit_count)
    silver_verification(session, Tenant, tenant_count)
    silver_verification(session, RentalContract, rental_contract_count)
    silver_verification(session, RentalInvoice, rental_invoice_count)

    return True

  except Exception as e:
    raise Exception(f"Error during silver_pipeline(): {e}")


def silver_main_pipeline(engine, session: Session) -> bool:
  '''Creates and populates the defined tables'''
  try:
    flag = silver_pipeline(engine, session)
    if flag:
      print("Ingestion process completed successfully.")
    return flag
  except Exception as e:
    print(f"Error during silver_main_pipeline() execution: {e}")
    return False

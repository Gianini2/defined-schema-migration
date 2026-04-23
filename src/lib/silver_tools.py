import pandas as pd
from sqlalchemy import delete
from sqlalchemy.orm import Session
from .models import Facility, Unit, Tenant, RentalContract, RentalInvoice


def retrieve_raw_data(engine):
  """Reads both monument_raw tables into DataFrames."""
  raw_unit_df = pd.read_sql_table(table_name='unit', schema='monument_raw', con=engine)
  raw_rentRoll_df = pd.read_sql_table(table_name='rentRoll', schema='monument_raw', con=engine)

  if raw_unit_df.empty or raw_rentRoll_df.empty:
    raise Exception("One of the raw tables is empty. Please check the raw layer.")

  return raw_unit_df, raw_rentRoll_df


def _silver_pipeline(engine, session: Session):
  """Builds all silver DataFrames and loads them in FK dependency order."""
  try:
    raw_unit_df, raw_rentRoll_df = retrieve_raw_data(engine)

    # Build DataFrames — no DB writes in this block; a failure here uploads nothing.
    facility_df = Facility.build(raw_unit_df)
    unit_df = Unit.build(facility_df, raw_unit_df, raw_rentRoll_df)
    tenant_df = Tenant.build(raw_rentRoll_df)
    rental_contract_df = RentalContract.build(raw_rentRoll_df, facility_df, unit_df, tenant_df)
    rental_invoice_df = RentalInvoice.build(raw_rentRoll_df, rental_contract_df, unit_df)

    # Truncate silver tables before loading to prevent PK conflicts on re-run (reverse FK order)
    with engine.begin() as conn:
      conn.execute(delete(RentalInvoice.__table__))
      conn.execute(delete(RentalContract.__table__))
      conn.execute(delete(Tenant.__table__))
      conn.execute(delete(Unit.__table__))
      conn.execute(delete(Facility.__table__))

    # Load tables — FK dependency order: facility → unit/tenant → rentalContract → rentalInvoice
    facility_count = Facility.load(session, facility_df)
    unit_count = Unit.load(session, unit_df)
    tenant_count = Tenant.load(session, tenant_df)
    rental_contract_count = RentalContract.load(session, rental_contract_df)
    rental_invoice_count = RentalInvoice.load(session, rental_invoice_df)

    print("###### Now validating the data for the monument schema ######\n")

    Facility.verify(session, facility_count)
    Unit.verify(session, unit_count)
    Tenant.verify(session, tenant_count)
    RentalContract.verify(session, rental_contract_count)
    RentalInvoice.verify(session, rental_invoice_count)

    return True

  except Exception as e:
    raise Exception(f"Error during silver_pipeline(): {e}") from e


def silver_main_pipeline(engine, session: Session) -> bool:
  """Entry point for the silver layer; returns True on success."""
  try:
    flag = _silver_pipeline(engine, session)
    if flag:
      print("Ingestion process completed successfully.")
    return flag
  except Exception as e:
    print(f"Error during silver_main_pipeline() execution: {e}")
    return False

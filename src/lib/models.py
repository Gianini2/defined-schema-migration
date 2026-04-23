import pandas as pd
from datetime import datetime
from decimal import Decimal
from sqlalchemy import String, Integer, Float, Numeric, DateTime, ForeignKey, quoted_name, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session
from .utils import date_parser


class Base(DeclarativeBase):
  pass


# Common silver layer mixin — provides load() and verify() on each table class
class SilverMixin:
  __tablename__: str  # provided by the concrete ORM subclass

  @classmethod
  def load(cls, session: Session, df: pd.DataFrame) -> int:
    """Inserts a DataFrame as ORM instances and commits the session."""
    records = df.where(pd.notna(df), other=None).to_dict('records')
    instances = [cls(**row) for row in records]
    try:
      session.add_all(instances)
      session.commit()
    except Exception as e:
      session.rollback()
      raise Exception(f"Error during {cls.__tablename__}.load(): {e}") from e
    count = len(instances)
    print(f"Data loaded successfully into {cls.__tablename__} table. Rows affected: {count}\n")
    return count

  @classmethod
  def verify(cls, session: Session, expected_rows: int) -> bool:
    """Asserts the table row count matches expected via COUNT query."""
    count = session.scalar(select(func.count()).select_from(cls))
    if count != expected_rows:
      raise Exception(
        f"Verification failed for {cls.__tablename__}. "
        f"Expected {expected_rows} rows, found {count}."
      )
    print(f"Verification successful for {cls.__tablename__}. Row count: {count}\n")
    return True


# Facility table
class Facility(Base, SilverMixin):
  __tablename__ = 'facility'
  __table_args__ = {'schema': 'monument'}

  facilityid: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)

  units: Mapped[list['Unit']] = relationship(back_populates='facility')

  @classmethod
  def build(cls, raw_unit: pd.DataFrame) -> pd.DataFrame:
    """Transforms raw_unit into the facility DataFrame ready for loading."""
    try:
      facility_df = raw_unit.groupby('facilityName').agg({'facilityName': 'first'}).reset_index(drop=True)
      facility_df.rename(columns={'facilityName': 'name'}, inplace=True)
      facility_df['facilityid'] = facility_df.index + 1
      facility_df = facility_df[['facilityid', 'name']]
    except Exception as e:
      raise Exception(f"Error during Facility.build() while applying logic: {e}") from e

    try:
      facility_df['name'] = facility_df['name'].astype(str)
      if facility_df['name'].str.len().max() > 100:
        exceptions = facility_df[facility_df['name'].str.len() > 100]['name'].tolist()
        raise Exception(f"Facility Name has more than 100 characters: {exceptions}")
    except Exception as e:
      raise Exception(f"Error during Facility.build() while validating schema: {e}") from e

    return facility_df


# Unit table
class Unit(Base, SilverMixin):
  __tablename__ = 'unit'
  __table_args__ = {'schema': 'monument'}

  unitid: Mapped[int] = mapped_column(Integer, primary_key=True)
  facilityid: Mapped[int] = mapped_column(ForeignKey('monument.facility.facilityid'), nullable=False)
  number: Mapped[str] = mapped_column(String(10), nullable=False)
  unitwidth: Mapped[float | None] = mapped_column(Float)
  unitlength: Mapped[float | None] = mapped_column(Float)
  unitheight: Mapped[float | None] = mapped_column(Float)
  unittype: Mapped[str | None] = mapped_column(String(20))
  monthlyrent: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))

  facility: Mapped['Facility'] = relationship(back_populates='units')
  rental_contracts: Mapped[list['RentalContract']] = relationship(back_populates='unit')

  @classmethod
  def build(cls, facility_df: pd.DataFrame, raw_unit: pd.DataFrame, raw_rentRoll: pd.DataFrame) -> pd.DataFrame:
    """Transforms raw_unit and raw_rentRoll into the unit DataFrame ready for loading."""
    try:
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
      unit_silver_df = unit_silver_df.sort_values(by=['facilityName', 'unitNumber']).reset_index(drop=True)
      unit_silver_df = unit_silver_df.drop_duplicates().reset_index(drop=True)
      unit_silver_df['unitid'] = unit_silver_df.index + 1
      unit_silver_df.drop(columns=['facilityName', 'name'], inplace=True)
      unit_silver_df.rename(columns={
        'unitNumber': 'number',
        'unitType': 'unittype',
        'monthlyRent': 'monthlyrent'
      }, inplace=True)
      return unit_silver_df[['unitid', 'facilityid', 'number', 'unitwidth', 'unitlength', 'unitheight', 'unittype', 'monthlyrent']]
    except Exception as e:
      raise Exception(f"Error during Unit.build() while applying logic: {e}") from e


# Tenant table
class Tenant(Base, SilverMixin):
  __tablename__ = 'tenant'
  __table_args__ = {'schema': 'monument'}

  tenantid: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  firstname: Mapped[str | None] = mapped_column(String(50))
  lastname: Mapped[str | None] = mapped_column(String(50))
  email: Mapped[str | None] = mapped_column(String(100))
  phone: Mapped[str | None] = mapped_column(String(20))

  rental_contracts: Mapped[list['RentalContract']] = relationship(back_populates='tenant')

  @classmethod
  def build(cls, raw_rentRoll: pd.DataFrame) -> pd.DataFrame:
    """Transforms raw_rentRoll into the tenant DataFrame ready for loading."""
    try:
      tenant_df = raw_rentRoll.copy()
      tenant_df['phone'] = tenant_df['phone'].str.replace(r'[^\w\s]', '', regex=True).str.replace(' ', '')
      tenant_df.rename(columns={
        'firstName': 'firstname',
        'lastName': 'lastname'
      }, inplace=True)
      tenant_df['firstname'] = tenant_df['firstname'].str.strip()
      tenant_df['lastname'] = tenant_df['lastname'].str.strip()
      tenant_df = tenant_df[['firstname', 'lastname', 'phone', 'email']]
      tenant_df = tenant_df.drop_duplicates().reset_index(drop=True)
      tenant_df['tenantid'] = tenant_df.index + 1
      return tenant_df
    except Exception as e:
      raise Exception(f"Error during Tenant.build() while applying logic: {e}") from e


# RentalContract table
class RentalContract(Base, SilverMixin):
  __tablename__ = quoted_name('rentalContract', quote=True)
  __table_args__ = {'schema': 'monument'}

  rentalcontractid: Mapped[int] = mapped_column(Integer, primary_key=True)
  unitid: Mapped[int] = mapped_column(ForeignKey('monument.unit.unitid'), nullable=False)
  tenantid: Mapped[int] = mapped_column(ForeignKey('monument.tenant.tenantid'), nullable=False)
  startdate: Mapped[datetime] = mapped_column(DateTime, nullable=False)
  enddate: Mapped[datetime | None] = mapped_column(DateTime)
  currentamountowed: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))

  unit: Mapped['Unit'] = relationship(back_populates='rental_contracts')
  tenant: Mapped['Tenant'] = relationship(back_populates='rental_contracts')
  invoices: Mapped[list['RentalInvoice']] = relationship(back_populates='rental_contract')

  @classmethod
  def build(
    cls,
    raw_rentRoll: pd.DataFrame,
    facility_silver_df: pd.DataFrame,
    unit_silver_df: pd.DataFrame,
    tenant_silver_df: pd.DataFrame
  ) -> pd.DataFrame:
    """Transforms raw_rentRoll into the rental contract DataFrame ready for loading."""
    try:
      unit_df = unit_silver_df.copy()
      unit_df = unit_df.merge(
        facility_silver_df[['facilityid', 'name']],
        on='facilityid',
        how='left'
      )
      unit_df.rename(columns={'name': 'facilityname'}, inplace=True)

      rental_contract_df = raw_rentRoll.copy()
      rental_contract_df = rental_contract_df.dropna(subset=['rentStartDate'])
      rental_contract_df['firstName'] = rental_contract_df['firstName'].str.strip()
      rental_contract_df['lastName'] = rental_contract_df['lastName'].str.strip()

      rental_contract_df = rental_contract_df.merge(
        tenant_silver_df[['firstname', 'lastname', 'email', 'tenantid']],
        left_on=['firstName', 'lastName', 'email'],
        right_on=['firstname', 'lastname', 'email'],
        how='left'
      )
      rental_contract_df = rental_contract_df.merge(
        unit_df[['facilityid', 'facilityname', 'number', 'unitid']],
        left_on=['facilityName', 'unitNumber'],
        right_on=['facilityname', 'number'],
        how='left'
      )

      rental_contract_df['rentalcontractid'] = rental_contract_df.index + 1
      rental_contract_df.rename(columns={
        'rentStartDate': 'startdate',
        'rentEndDate': 'enddate',
        'currentRentOwed': 'currentamountowed',
      }, inplace=True)

      rental_contract_df = rental_contract_df[['rentalcontractid', 'unitid', 'tenantid', 'startdate', 'enddate', 'currentamountowed']]
      rental_contract_df['startdate'] = rental_contract_df['startdate'].apply(date_parser)
      rental_contract_df['enddate'] = rental_contract_df['enddate'].apply(date_parser)

      return rental_contract_df
    except Exception as e:
      raise Exception(f"Error during RentalContract.build() while applying logic: {e}") from e


# RentalInvoice table
class RentalInvoice(Base, SilverMixin):
  __tablename__ = quoted_name('rentalInvoice', quote=True)
  __table_args__ = {'schema': 'monument'}

  invoiceid: Mapped[int] = mapped_column(Integer, primary_key=True)
  rentalcontractid: Mapped[int] = mapped_column(ForeignKey('monument.rentalContract.rentalcontractid'), nullable=False)
  invoiceduedate: Mapped[datetime] = mapped_column(DateTime, nullable=False)
  invoiceamount: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
  invoicebalance: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))

  rental_contract: Mapped['RentalContract'] = relationship(back_populates='invoices')

  @classmethod
  def build(
    cls,
    raw_rentRoll: pd.DataFrame,
    rental_contract_silver_df: pd.DataFrame,
    unit_silver_df: pd.DataFrame,
  ) -> pd.DataFrame:
    """Transforms raw_rentRoll into the rental invoice DataFrame ready for loading."""
    try:
      contract_df = rental_contract_silver_df.copy()
      contract_df = contract_df.merge(
        unit_silver_df[['monthlyrent', 'unitid', 'number']],
        on='unitid',
        how='left'
      )

      rental_invoice_df = raw_rentRoll.copy()
      rental_invoice_df['invoiceduedate'] = rental_invoice_df['rentStartDate'].apply(date_parser)
      rental_invoice_df['invoiceduedate'] = rental_invoice_df['invoiceduedate'] + pd.offsets.MonthBegin(1)

      rental_invoice_df = rental_invoice_df.merge(
        contract_df[['rentalcontractid', 'number', 'monthlyrent']],
        left_on='unitNumber',
        right_on='number',
        how='left'
      )

      rental_invoice_df['invoiceid'] = rental_invoice_df.index + 1
      rental_invoice_df.rename(columns={
        'monthlyrent': 'invoiceamount',
        'currentRentOwed': 'invoicebalance',
      }, inplace=True)

      return rental_invoice_df[['invoiceid', 'rentalcontractid', 'invoiceduedate', 'invoiceamount', 'invoicebalance']]
    except Exception as e:
      raise Exception(f"Error during RentalInvoice.build() while applying logic: {e}") from e


# RawUnit table (monument_raw staging)
class RawUnit(Base):
  __tablename__ = 'unit'
  __table_args__ = {'schema': 'monument_raw'}

  id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  facility_name: Mapped[str | None] = mapped_column('facilityName', String)
  unit_number: Mapped[int | None] = mapped_column('unitNumber', Integer)
  unit_size: Mapped[str | None] = mapped_column('unitSize', String)
  unit_type: Mapped[str | None] = mapped_column('unitType', String)

  @classmethod
  def from_csv_row(cls, row: dict) -> 'RawUnit':
    return cls(
      facility_name=row.get('facilityName') or None,
      unit_number=int(row['unitNumber']) if row.get('unitNumber') else None,
      unit_size=row.get('unitSize') or None,
      unit_type=row.get('unitType') or None,
    )


# RawRentRoll table (monument_raw staging)
class RawRentRoll(Base):
  __tablename__ = quoted_name('rentRoll', quote=True)
  __table_args__ = {'schema': 'monument_raw'}

  id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  facility_name: Mapped[str | None] = mapped_column('facilityName', String)
  unit_number: Mapped[int | None] = mapped_column('unitNumber', Integer)
  first_name: Mapped[str | None] = mapped_column('firstName', String)
  last_name: Mapped[str | None] = mapped_column('lastName', String)
  phone: Mapped[str | None] = mapped_column(String)
  email: Mapped[str | None] = mapped_column(String)
  rent_start_date: Mapped[str | None] = mapped_column('rentStartDate', String)
  rent_end_date: Mapped[str | None] = mapped_column('rentEndDate', String)
  monthly_rent: Mapped[float | None] = mapped_column('monthlyRent', Float)
  current_rent_owed: Mapped[float | None] = mapped_column('currentRentOwed', Float)

  @classmethod
  def from_csv_row(cls, row: dict) -> 'RawRentRoll':
    return cls(
      facility_name=row.get('facilityName') or None,
      unit_number=int(row['unitNumber']) if row.get('unitNumber') else None,
      first_name=row.get('firstName') or None,
      last_name=row.get('lastName') or None,
      phone=row.get('phone') or None,
      email=row.get('email') or None,
      rent_start_date=row.get('rentStartDate') or None,
      rent_end_date=row.get('rentEndDate') or None,
      monthly_rent=float(row['monthlyRent']) if row.get('monthlyRent') else None,
      current_rent_owed=float(row['currentRentOwed']) if row.get('currentRentOwed') else None,
    )

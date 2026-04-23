from datetime import datetime
from decimal import Decimal
from sqlalchemy import String, Integer, Float, Numeric, DateTime, ForeignKey, quoted_name
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
  pass

## Facility table
class Facility(Base):
  __tablename__ = 'facility'
  __table_args__ = {'schema': 'monument'}

  facilityid: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)

  units: Mapped[list['Unit']] = relationship(back_populates='facility')

## Unit table
class Unit(Base):
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

## Tenant table
class Tenant(Base):
  __tablename__ = 'tenant'
  __table_args__ = {'schema': 'monument'}

  tenantid: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  firstname: Mapped[str | None] = mapped_column(String(50))
  lastname: Mapped[str | None] = mapped_column(String(50))
  email: Mapped[str | None] = mapped_column(String(100))
  phone: Mapped[str | None] = mapped_column(String(20))

  rental_contracts: Mapped[list['RentalContract']] = relationship(back_populates='tenant')

## RentalContract table
class RentalContract(Base):
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

## RentalInvoice table
class RentalInvoice(Base):
  __tablename__ = quoted_name('rentalInvoice', quote=True)
  __table_args__ = {'schema': 'monument'}

  invoiceid: Mapped[int] = mapped_column(Integer, primary_key=True)
  rentalcontractid: Mapped[int] = mapped_column(ForeignKey('monument.rentalContract.rentalcontractid'), nullable=False)
  invoiceduedate: Mapped[datetime] = mapped_column(DateTime, nullable=False)
  invoiceamount: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
  invoicebalance: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))

  rental_contract: Mapped['RentalContract'] = relationship(back_populates='invoices')

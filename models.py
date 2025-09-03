"""
SQLAlchemy ORM models for the people counting database.

This file defines the Python classes that map to the database tables,
including relationships between them.
"""
from sqlalchemy import (Column, ForeignKey, Integer, SmallInteger, BigInteger, 
                        CHAR, NCHAR, Boolean, DateTime)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.schema import PrimaryKeyConstraint

Base = declarative_base()

class Store(Base):
    """Maps to the `store` table, containing information about each store."""
    __tablename__ = "store"
    tid = Column(Integer, primary_key=True)
    country = Column(CHAR(20))
    area = Column(CHAR(20))
    province = Column(CHAR(20))
    city = Column(CHAR(20))
    name = Column(CHAR(80), nullable=False)
    address = Column(CHAR(80), nullable=False)
    isbranch = Column(CHAR(3))
    code = Column(CHAR(32), nullable=False)
    cameranum = Column(Integer, nullable=False)
    manager = Column(CHAR(20))
    managertel = Column(CHAR(20))
    lastEditDate = Column(DateTime)
    formula = Column(CHAR(64))

class NumCrowd(Base):
    """Maps to the `num_crowd` table, storing people traffic data."""
    __tablename__ = "num_crowd"
    recordtime = Column(DateTime)
    in_num = Column(Integer, nullable=False)
    out_num = Column(Integer, nullable=False)
    position = Column(CHAR(30))
    storeid = Column(Integer, ForeignKey("store.tid"), nullable=False)

    store = relationship(Store)
    __table_args__ = (PrimaryKeyConstraint(recordtime, in_num, out_num, storeid),)

class ErrLog(Base):
    """Maps to the `ErrLog` table, containing system and device error logs."""
    __tablename__ = "ErrLog"
    ID = Column(BigInteger, primary_key=True)
    storeid = Column(Integer, ForeignKey("store.tid"), nullable=False)
    DeviceCode = Column(SmallInteger)
    LogTime = Column(DateTime)
    Errorcode = Column(Integer)
    ErrorMessage = Column(NCHAR(120))

    store = relationship(Store)

class Status(Base):
    """Maps to the `Status` table, storing device operational status."""
    __tablename__ = "Status"
    ID = Column(Integer, primary_key=True)
    storeid = Column(Integer, ForeignKey("store.tid"), nullable=False)
    FlashNum = Column(Integer)
    RamNum = Column(Integer)
    RC1 = Column(Boolean)
    RC2 = Column(Boolean)
    RC3 = Column(Boolean)
    RC4 = Column(Boolean)
    RC5 = Column(Boolean)
    RC6 = Column(Boolean)
    RC7 = Column(Boolean)
    RC8 = Column(Boolean)
    DcID = Column(SmallInteger)
    FV = Column(NCHAR(20))
    DcTime = Column(DateTime)
    DeviceID = Column(SmallInteger)
    IA = Column(Integer)
    OA = Column(Integer)
    S = Column(SmallInteger)
    T = Column(DateTime)

    store = relationship(Store)

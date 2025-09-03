"""
Module to handle database connections and data querying.

This module manages the connection to the SQL Server database using SQLAlchemy
and provides cached functions to fetch data for the Streamlit application.
It includes robust error handling for database connection issues.
"""
import pandas as pd
import streamlit as st

from sqlalchemy import create_engine, extract
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from urllib import parse

from models import Store, NumCrowd, ErrLog, Status

@st.cache_resource
def connectDB():
    """
    Establishes and tests the database connection using credentials from secrets.

    Returns:
        sqlalchemy.engine.Engine | None: The database engine object if the 
        connection is successful, otherwise None.
    """
    try:
        env = st.secrets["development"]
        db_host = env["DB_HOST"]
        db_port = parse.quote_plus(str(env["DB_PORT"]))
        db_name = env["DB_NAME"]
        db_user = parse.quote_plus(env["DB_USER"])
        db_pass = parse.quote_plus(env["DB_PASS"])

        DATABASE_URL = f"mssql+pyodbc://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?driver=SQL Server"

        # Initialize engine with a connection timeout to avoid long waits.
        engine = create_engine(DATABASE_URL, connect_args={"timeout": 5})

        # Test the connection to ensure it's valid before proceeding.
        connection = engine.connect()
        connection.close()

        return engine
    except OperationalError as e:
        st.error(f"Lỗi kết nối CSDL: Không thể kết nối đến máy chủ. Vui lòng kiểm tra lại thông tin.")
        st.error(f"Chi tiết kỹ thuật: {e}")
        return None
    except Exception as e:
        st.error(f"Đã xảy ra lỗi không xác định khi kết nối CSDL: {e}")
        return None

# Attempt to connect to the database.
engine = connectDB()

# Only create a Session factory if the engine was successfully created.
if engine:
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@st.cache_resource
def getSession():
    """
    Creates and returns a new database session.

    Returns:
        sqlalchemy.orm.Session | None: A new session object, or None if the 
        database connection failed.
    """
    if "Session" in globals():
        session = Session()
        try:
            return session
        finally:
            session.close()
    return None

@st.cache_data(ttl=86400, show_spinner=False)
def dbStore() -> pd.DataFrame:
    """Fetches all store data from the database."""
    if not engine: return pd.DataFrame() # Return empty DataFrame on connection failure.
    query = getSession().query(Store)
    return pd.read_sql(sql=query.statement, con=engine)

@st.cache_data(ttl=900, show_spinner=False)
def dbNumCrowd(year: int = None) -> pd.DataFrame:
    """Fetches people counting data, optionally filtered by year."""
    if not engine: return pd.DataFrame()
    query = getSession().query(NumCrowd)
    if year:
        query = query.filter(extract("year", NumCrowd.recordtime) == year)
    return pd.read_sql(sql=query.statement, con=engine)

@st.cache_data(ttl=3600, show_spinner=False)
def dbErrLog() -> pd.DataFrame:
    """Fetches the 500 most recent error logs."""
    if not engine: return pd.DataFrame()
    query = getSession().query(ErrLog).order_by(ErrLog.LogTime.desc()).limit(500)
    return pd.read_sql(sql=query.statement, con=engine)

@st.cache_data(ttl=3600, show_spinner=False)
def dbStatus() -> pd.DataFrame:
    """Fetches device status data."""
    if not engine: return pd.DataFrame()
    query = getSession().query(Status)
    return pd.read_sql(sql=query.statement, con=engine)

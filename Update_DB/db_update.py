import os
import shutil
import warnings

import functools
import pandas as pd
from pathlib import Path
import sqlalchemy
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sshtunnel import SSHTunnelForwarder, BaseSSHTunnelForwarderError

from logging_config import * # logger
from db_config import * # DB_PARAMS, SSH_TUNNEL_PARAMS

warnings.filterwarnings("ignore", category=UserWarning)

# Decorators
# Function to fix exceptions (decorator)
def exception(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError as e:
            logger.error(f"File not found error in {func.__name__}: {e}")
        except PermissionError as e:
            logger.warning(f"Permission error in {func.__name__}: {e}")
        except (IOError, shutil.Error) as e:
            logger.error(f"Error while moving file {func.__name__}: {e}")
        except ValueError as e:
            logger.error(f"Value error in {func.__name__}: {e}")
        except pd.errors.ParserError as e:
            logger.error(f"Parser error in {func.__name__}: {e}")
        except OSError as e:
            logger.error(f"OS error in {func.__name__}: {e}")
        except BaseSSHTunnelForwarderError as e:
            logger.error(f"SSH tunnel error in {func.__name__}: {e}")
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy error in {func.__name__}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {e}")
        logger.error(f"'{func.__name__}' - Function failed")            
        return None
    return wrapper

# Function to read Excel files
@exception
@log_function_execution
def read_excel_files(FOLDER_PATH_IN: Path, FOLDER_PATH_OUT: Path, SHEET: str, SKIP: int = 0, COL_NAMES: list[str] | None = None,) -> pd.DataFrame | None:
    """Reads Excel files from a folder and combines them into a single DataFrame.

    Args:
        folder_path_in (Path): Path to the folder containing Excel files.
        folder_path_out (Path): Path to the folder where processed files are moved.
        sheet_name (str): Name of the sheet to read from each Excel file.
        skiprows (int, optional): Number of rows to skip at the beginning of each sheet. Defaults to 0.
        col_names (list[str], optional): List of column names to use for the resulting DataFrame. Defaults to None.

    Returns:
        pd.DataFrame | None: The combined DataFrame if files were read successfully, otherwise None.
    """

    if not os.path.exists(FOLDER_PATH_IN):
        logger.error(f"Input folder '{FOLDER_PATH_IN}' does not exist.")
        return None

    file_list = os.listdir(FOLDER_PATH_IN)
    if not file_list:
        logger.info("No Excel files found in the input folder.")
        return None
    
    file_list = os.listdir(FOLDER_PATH_IN)
    dfs = []
    for file in file_list:
        file_path = os.path.join(FOLDER_PATH_IN, file)
        with pd.ExcelFile(file_path) as xls:
            data = pd.read_excel(xls, sheet_name=SHEET, skiprows=SKIP, names=COL_NAMES)
            dfs.append(data)
        # Moving the file after processing
        move_processed_file(file_path, FOLDER_PATH_OUT, file)
    
    # Check if the list is not empty    
    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        return df
    else:
        logger.info("No data read from the files.")
        return None
    
# Function to move file to archive folder
@exception
@log_function_execution
def move_processed_file(file_path: Path, FOLDER_PATH_OUT: Path, file: str) -> None:
    """Moves a file to the archive folder.

    Args:
        file_path (Path): Path to the file to move.
        folder_path_out (Path): Path to the archive folder.
        file_name (str): Name of the file to move.
    """

    if not os.path.exists(FOLDER_PATH_OUT):
        os.makedirs(FOLDER_PATH_OUT)

    new_path = os.path.join(FOLDER_PATH_OUT, file)
    if os.path.exists(new_path):
        os.remove(new_path)

    shutil.move(file_path, new_path)

# Function to create dict data
@exception
@log_function_execution
def load_excel_sheets(DICT_PATH: Path, LIST_OF_SHEETS: list[str]) -> dict[str, pd.DataFrame]:
    """Loads data from multiple sheets in an Excel file into a dictionary.

    Args:
        dict_path (Path): Path to the Excel file.
        list_of_sheets (list[str]): List of sheet names to read.

    Returns:
        dict[str, pd.DataFrame]: Dictionary containing data from each sheet with sheet name as key.
    """

    if not os.path.exists(DICT_PATH):
        logger.error(f"The file '{DICT_PATH}' does not exist.")
        return {}

    sheets_data = {}
    for sheet in LIST_OF_SHEETS:
        sheets_data[sheet] = pd.read_excel(DICT_PATH, sheet_name=sheet)
    return sheets_data

# Function to process data
@exception
@log_function_execution
def process_data(df: pd.DataFrame | None, COMPANIES: list[str]) -> pd.DataFrame | None:
    """Processes a DataFrame by cleaning 'Day' column, filtering companies, and converting columns to lowercase snake_case.

    Args:
        df (pd.DataFrame | None): The DataFrame to process.
        companies (list[str]): List of company names to filter the DataFrame.

    Returns:
        pd.DataFrame | None: The processed DataFrame, or None if the input DataFrame is empty.
    """

    if df is None or df.empty:
        return df

    if df['Day'].dtype == 'O':
        df['Day'] = df['Day'].str[-10:].str.replace(',', '').str.replace(' ', '')

    df["Day"] = pd.to_datetime(df["Day"]).dt.date
    df = df.loc[df['Company'].isin(COMPANIES)]
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    return df

# Function to filtering unique dates in a dataframe
@exception
@log_function_execution
def create_outer_df(df: pd.DataFrame) -> pd.DataFrame:
    """Creates a DataFrame with a single column containing unique dates from the input DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the 'day' column.

    Returns:
        pd.DataFrame: A DataFrame with a single column named 'key' containing unique dates.
    """

    unique_combinations = df['day'].unique()
    outer_df = pd.DataFrame(unique_combinations, columns=['key'])
    return outer_df

# Function for creating an SSH tunnel
@exception
@log_function_execution
def create_ssh_tunnel() -> SSHTunnelForwarder:
    """Creates an SSH tunnel using the provided SSH tunnel parameters.

    Assumes the existence of an `SSHTunnelForwarder` class and `SSH_TUNNEL_PARAMS` dictionary containing connection details.

    Returns:
        SSHTunnelForwarder: An instance of the SSH tunnel object.
    """

    ssh_tunnel = SSHTunnelForwarder(**SSH_TUNNEL_PARAMS)
    return ssh_tunnel

# Function to connecting to a database
@exception
@log_function_execution
def create_db_engine(ssh_tunnel: SSHTunnelForwarder | None) -> sqlalchemy.engine.Engine | None:
    """Creates a database engine using connection details and an optional SSH tunnel.

    Args:
        ssh_tunnel (SSHTunnelForwarder | None): An SSH tunnel object for tunneled connection (optional).

    Returns:
        sqlalchemy.engine.Engine | None: A database engine object, or None if the SSH tunnel is not established.
    """

    if ssh_tunnel is None:
        logger.error("SSH tunnel is not established.")
        return None

    DB_PARAMS['port'] = ssh_tunnel.local_bind_port
    engine_str = f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}"
    engine = create_engine(engine_str)
    return engine

# Function to get date intersections
@exception
@log_function_execution
def get_intersections(engine: sqlalchemy.engine.Engine, df: pd.DataFrame | None) -> list[str]:
    """Retrieves a list of dates intersecting between the DataFrame and the database table.

    Args:
        engine (sqlalchemy.engine.Engine): The database engine object.
        df (pd.DataFrame | None): The DataFrame containing the 'day' column (optional).

    Returns:
        list[str]: A list of dates present in both the DataFrame and the database table.
    """

    if df is None or df.empty:
        return []

    query = text('select DISTINCT day as key from sales')
    inner_df = pd.read_sql(query, engine)['key']
    inner_df = df['day'].unique()
    intersection_df = pd.merge(create_outer_df(df), pd.DataFrame({'key': inner_df}), on='key', how='inner')['key'].tolist()
    return intersection_df
    
# Function to remove intersections from the database
@exception
@log_function_execution
def delete_intersections(session: sessionmaker, intersection_df: list[str], table_name: str) -> None:
    """Deletes data from a database table based on a list of dates.

    Args:
        session (sessionmaker): A database session object.
        intersection_df (list[str]): List of dates to delete.
        table_name (str): Name of the database table.
    """
    
    if not intersection_df:
        logger.info("No intersections to delete.")
        return

    delete_query = text(f'DELETE FROM {table_name} WHERE day = ANY(:keys)')
    session.execute(delete_query, {'keys': intersection_df})
    session.commit()

# Function to delete_existing_data if need to replace data in a table 
@exception
@log_function_execution
def delete_existing_data(engine: sqlalchemy.engine.Engine, session: sqlalchemy.orm.Session, table_name: str) -> None:
    """Delete existing data from a database table.

    Args:
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object
            representing the connection to the database.
        session (sqlalchemy.orm.Session): The SQLAlchemy session object
            used to interact with the database.
        table_name (str): The name of the table from which to delete data.

    Raises:
        KeyError: If the specified table is not found in the database schema.
    """
    meta = MetaData()
    meta.reflect(bind=engine)
    
    if table_name in meta.tables:
        table = meta.tables.get(table_name)
        
        # Clearing the table before loading new data
        delete_stmt = table.delete()
        session.execute(delete_stmt)
        session.commit()

# Function to load data to database
@exception
@log_function_execution
def load_data_to_db(df: pd.DataFrame, engine: sqlalchemy.engine.Engine, session: sqlalchemy.orm.Session, name: str, IF_EXISTS: str, FOLDER_PATH_IN: str) -> None:
    """Loads a DataFrame into a database table.

    Args:
        df (pd.DataFrame): The DataFrame to load.
        engine (sqlalchemy.engine.Engine): The database engine object.
        name (str): Name of the table to load data into.
        IF_EXISTS (str): How to handle existing data in the table ('replace', 'append', or 'fail').
    """
    
    with engine.connect() as conn:
        if IF_EXISTS == 'replace':
            IF_EXISTS = 'append'
            if os.listdir(FOLDER_PATH_IN):
                delete_existing_data(engine, session, name)
            
        df.to_sql(name, conn, if_exists=IF_EXISTS, index=False)

# Function to transform and load dict data to database  
@exception 
@log_function_execution
def transform_and_load_dict(engine: sqlalchemy.engine.Engine, session: sqlalchemy.orm.Session, dfs: dict[str, pd.DataFrame]) -> None:
    """Transforms and loads data from a dictionary of DataFrames into a database.

    Args:
        engine (sqlalchemy.engine.Engine): The database engine object.
        dfs (dict[str, pd.DataFrame]): A dictionary containing DataFrames with sheet names as keys.
    """
    
    with engine.connect() as conn:
        for df_name, df in dfs.items():
            table_name = df_name.lower()
            df.columns = df.columns.str.lower()
            
            delete_existing_data(engine, session, table_name)
            
            # Load DataFrame into the database
            # with session.begin():
            df.to_sql(table_name, conn, if_exists='append', index=False)


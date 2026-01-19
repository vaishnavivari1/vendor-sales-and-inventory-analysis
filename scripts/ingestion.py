"""
    Purpose of the script:
            The script is used to move the CSV files which present a location in system to a SQL Server Database.
            We are doing this step to mimic the way of retrieving the data from the SQL Server Database. We defined 
            two functions one for making connection to SQL Server while another is for moving the csv folders into
            SQL Server Database.
            
    SQL Connection function:
        The function is used to make connection with the SQL Server Database.The function uses the SQL Server Authentication. 
        Safer than windows authentication. Make sure you have enabled SQL Server Authentication. IF not we can also use the
        Windows Authentication which is Ease of use.

    CSV to SQL function:
        creates the list to store the files which is used to create full path of each CSV file. Once full path is created we
        use the to_sql to move data into SQL Server Database. Instead of moving all the data at a time we use the chunks / batches
        which shows how much data has successfully moved into the database.
            
"""



import os
import time
import pandas as pd
from logger_setup import get_logger
log = get_logger("ingestion.log")


def sql_connection(SERVER, DATABASE, DRIVER, username = None, password = None):
    from sqlalchemy import create_engine
    import pyodbc
    """ 
        use below Connection url.
        for windows authentication:
            connection_url = f"mssql+pyodbc://@{SERVER}/{DATABASE}?driver={DRIVER}&trusted_connection=yes" 
        for SQL Server Authentication:
            connection_url = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}&Encrypt=no&TrustServerCertificate=yes"
    """
    if username and password:
        connection_url = f"mssql+pyodbc://{username}:{password}@{SERVER}/{DATABASE}?driver={DRIVER}&Encrypt=no&TrustServerCertificate=yes"
    else:
        connection_url = f"mssql+pyodbc://@{SERVER}/{DATABASE}?driver={DRIVER}&trusted_connection=yes"     
    try:
        engine = create_engine(connection_url, fast_executemany=True)
        query = "SELECT 1 as test_col"
        pd.read_sql_query(query, con=engine)
        log.info("Successfully connected to the database.")
        return engine
    except Exception as e:
        log.warning(f"Connection failed because of {e}")
        return None



def csv_to_sql(engine, csv_folder,chunkSize = 100_000):

    """
        Reads CSV files from a folder and uploads them into SQL Server in chunks.
        Args:
            csv_folder : Path to folder containing CSV files.
            engine: SQLAlchemy engine.
            chunksize: Number of rows per chunk.
    """
    csv_files = [file for file in os.listdir(csv_folder) if file.endswith(".csv")]
    if not csv_files:
        log.warning("Folder is empty! Please check the folder path.")
        return 
    
    log.info(f"Total of {len(csv_files)} CSV files found in the folder.")
    try:
        # Initial Starting time
        start = time.time()
        for file in csv_files:
            # TableName is the File name
            tableName = os.path.splitext(file)[0]
            csv_path = os.path.join(csv_folder, file)

            # Mode and First chunk is used to insert data 
            # after creating table for the first time
            total_rows = 0
            mode = "fail"
            first_chunk = True

            try:
                # Each file start time
                inStart = time.time()
                for chunk in pd.read_csv(csv_path, chunksize=chunkSize):
                    if first_chunk and mode == "fail":
                        chunk.to_sql(tableName, engine, if_exists="fail", index=False)
                        # Once table is created then we change 
                        # the mode and first chunk to perform appending of data
                        first_chunk = False
                        mode = "append"
                        log.info(f"{tableName} is created successfully.")
                        log.info(f"Inserting {len(chunk)} records into {tableName}.")
                    else:
                        chunk.to_sql(tableName, engine, if_exists="append", index=False)
                    # Total Rows is addes every time the chunk is created
                    total_rows += len(chunk)
                    log.info(f"Appended {total_rows} records into {tableName}.")
                # Each file end time
                inEnd = time.time()
                log.info(f"Insertion completed: {total_rows} records inserted into {tableName}.")
                log.info(f"Time taken for {tableName} is {inEnd - inStart:.2f} seconds.")

            except Exception as e:
                log.warning(f"Problem inserting records into {tableName}: {e}")
        # Total files insertion completion time
        end = time.time()
        log.info(f"Total of {len(csv_files)} tables inserted.")
        log.info(f"Total time taken: {end - start:.2f} seconds.")

    except Exception as e:
        log.warning(f"Problem in the insertion process: {e}")

def main():
    from dotenv import load_dotenv
    from urllib.parse import quote_plus
    # Load environment variables
    load_dotenv()
  
    CSV_FOLDER = os.getenv("CSV_FOLDER")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    SERVER = os.getenv("SERVER")
    DATABASE = os.getenv("DATABASE")
    DRIVER = quote_plus(os.getenv("DRIVER"))

    engine = sql_connection( SERVER, DATABASE, DRIVER, username, password)
    if engine:
        csv_to_sql(engine, CSV_FOLDER)
        log.info("Ingestion process completed successfully.")
    else:
        log.warning("Ingestion skipped due to failed connection.")

if __name__ == "__main__":
    main()

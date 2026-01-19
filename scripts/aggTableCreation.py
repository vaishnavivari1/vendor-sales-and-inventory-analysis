"""
    Purpose of the script:
        The script is used to explore and transform data from various SQL Server tables into an aggregated form.
        It connects to the SQL Server Database, creates a new table, and loads aggregated data for further analysis.
        The data is cleaned, transformed, and enriched with calculated metrics like profit margin and stock turnover.
        The final result is stored back into the database for reporting or further processing.

    Performance Optimizations:
        1. Using Indexes on key columns to speed up joins and queries.
        2. Using Common Table Expressions (CTEs) to structure complex queries efficiently. 
            Joining and aggregations on huge datasets will takes more time and not effective so we use the CTE for pre aggregations
            and then joins the CTE's
"""



import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from logger_setup import get_logger
from urllib.parse import quote_plus
from dotenv import load_dotenv

log = get_logger("exploringData.log")

# DB CONNECTION
def sql_connection(SERVER, DATABASE, DRIVER, username=None, password=None):
    try:
        if username and password:
            connection_url = f"mssql+pyodbc://{username}:{password}@{SERVER}/{DATABASE}?driver={DRIVER}&Encrypt=no&TrustServerCertificate=yes"
        else:
            connection_url = f"mssql+pyodbc://@{SERVER}/{DATABASE}?driver={DRIVER}&trusted_connection=yes"
        engine = create_engine(connection_url, fast_executemany=True)
        query = "SELECT 1 as test_col"
        pd.read_sql_query(query, con=engine)
        log.info("Successfully connected to the database.")
        return engine
    except Exception as e:
        log.warning(f"Connection failed because of {e}")
        return None

# CREATE TABLE
def create_table(engine):
    create_table_ddl = """
    CREATE TABLE AggregatedTable (
        VendorNumber INT,
        VendorName VARCHAR(255),
        Description VARCHAR(255),
        Brand INT,
        PurchasePrice DECIMAL(15,4),
        ActualPrice DECIMAL(15,4),
        Volume DECIMAL(15,4),
        TotalPurchaseQuantity INT,
        TotalPurchaseDollars DECIMAL(15,4),
        TotalSalesQuantity INT,
        TotalSalesDollars DECIMAL(15,4),
        TotalExciseTax DECIMAL(15,4),
        TotalAdditionalCharges DECIMAL(15,4),
        TotalSalesPrice DECIMAL(15,4),
        GrossProfit DECIMAL(15,4),
        ProfitMargin DECIMAL(15,4),
        StockTurnOver DECIMAL(15,4),
        SalesToPurchaseRatio DECIMAL(15,4),
        PRIMARY KEY (VendorNumber, Brand)
    )
    """
    index_queries = [
        "CREATE INDEX idx_purchases_vendor_brand ON purchases(VendorNumber, Brand);",
        "CREATE INDEX idx_purchase_prices_vendor_brand ON purchase_prices(VendorNumber, Brand);",
        "CREATE INDEX idx_sales_vendor_brand ON sales(VendorNo, Brand);",
        "CREATE INDEX idx_vendor_invoice_vendor ON vendor_invoice(VendorNumber);"
    ]
    try:
        with engine.connect() as connection:
            connection.execute(text("DROP TABLE IF EXISTS AggregatedTable"))
            connection.execute(text(create_table_ddl))
            log.info("AggregatedTable created successfully.")
            for q in index_queries:
                connection.execute(text(q))
            log.info("Indexes created successfully.")
    except Exception as e:
        log.error(f"Error creating table or indexes: {e}")
        return False
    return True


# AGGREGATED TABLE CREATION
def aggTable(engine):
    start_time = time.time()
    try:
        query = """
        WITH freightSummary AS (
            SELECT VendorNumber, SUM(Freight) AS AdditionalCharges
            FROM vendor_invoice 
            GROUP BY VendorNumber
        ),
        purchaseSummary AS (
            SELECT
                p.VendorNumber,
                p.VendorName,
                p.Description,
                p.Brand,
                pp.Price AS ActualPrice,
                pp.PurchasePrice,
                pp.Volume,
                SUM(p.Quantity) AS TotalPurchaseQuantity,
                SUM(p.Dollars) AS TotalPurchaseDollars
            FROM purchases AS p
            JOIN purchase_prices AS pp
              ON p.VendorNumber = pp.VendorNumber
             AND p.Brand = pp.Brand
            GROUP BY p.VendorNumber, p.VendorName, p.Description, p.Brand,
                     pp.Price, pp.PurchasePrice, pp.Volume
        ),
        salesSummary AS (
            SELECT
                VendorNo,
                VendorName,
                Brand,
                SUM(SalesQuantity) AS TotalSalesQuantity,
                SUM(ExciseTax) AS TotalExciseTax,
                SUM(SalesPrice) AS TotalSalesPrice,
                SUM(SalesDollars) AS TotalSalesDollars
            FROM sales
            GROUP BY VendorNo, VendorName, Brand
        )
        SELECT
            ps.VendorNumber,
            ps.VendorName,
            ps.Brand,
            ps.Description,
            ps.ActualPrice,
            ps.PurchasePrice,
            ps.Volume,
            ps.TotalPurchaseQuantity,
            ps.TotalPurchaseDollars,
            ss.TotalSalesQuantity,
            ss.TotalExciseTax,
            ss.TotalSalesPrice,
            fs.AdditionalCharges,
            ss.TotalSalesDollars
        FROM purchaseSummary AS ps
        LEFT JOIN salesSummary AS ss
          ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
        LEFT JOIN freightSummary AS fs
          ON ps.VendorNumber = fs.VendorNumber
        """
        finalDf = pd.read_sql_query(query, con=engine)
        time_taken = round(time.time() - start_time, 2)
        log.info(f"Aggregated Table created with {len(finalDf)} records. Shape: {finalDf.shape}. Time taken: {time_taken} sec")
        return finalDf
    except Exception as e:
        log.warning(f"Problem in Aggregated Table Creation: {e}")
        return None


# TRANSFORMATIONS
def transformations(df):
    start_time = time.time()
    try:
        # Missing Values
        nullsCount = df.isnull().sum().sum()
        if nullsCount > 0:
            df.fillna(0, inplace=True)
            log.info(f"Replaced {nullsCount} missing values with 0")

        # Duplicates
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            df.drop_duplicates(inplace=True)
            log.info(f"Removed {duplicates} duplicate rows")

        # String cleaning
        non_numeric_cols = df.select_dtypes(exclude="number").columns
        for col in non_numeric_cols:
            if df[col].dtype == "object":
                df[col] = df[col].str.strip()
        log.info("Trimmed spaces in string columns")

        # Type Conversion
        df["Volume"] = df["Volume"].astype("float64")

        # Data Enrichment
        df["GrossProfit"] = df["TotalSalesDollars"] - df["TotalPurchaseDollars"]
        df["ProfitMargin"] = df.apply(lambda row: (row["GrossProfit"] / row["TotalSalesDollars"] * 100) if row["TotalSalesDollars"] != 0 else 0, axis=1)
        df["StockTurnOver"] = df.apply(lambda row: (row["TotalSalesQuantity"] / row["TotalPurchaseQuantity"]) if row["TotalPurchaseQuantity"] != 0 else 0, axis=1)
        df["SalesToPurchaseRatio"] = df.apply(lambda row: (row["TotalSalesDollars"] / row["TotalPurchaseDollars"]) if row["TotalPurchaseDollars"] != 0 else 0, axis=1)

        df.fillna(0, inplace=True)

        time_taken = round(time.time() - start_time, 2)
        log.info(f"Transformations & Cleaning completed. Final shape: {df.shape}. Time taken: {time_taken} sec")
    except Exception as e:
        log.warning(f"Problem in Transformations: {e}")
    return df


# MAIN
def main():
    load_dotenv()

    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    SERVER = os.getenv("SERVER")
    DATABASE = os.getenv("DATABASE")
    DRIVER = quote_plus(os.getenv("DRIVER"))

    engine = sql_connection(SERVER, DATABASE, DRIVER, username, password)
    if engine:
        if create_table(engine):
            df = aggTable(engine)
            if df is not None:
                finalDf = transformations(df)
                try:
                    finalDf.to_sql("AggregatedTable", con=engine, if_exists="append", index=False)
                    log.info("Aggregated Table data inserted successfully.")
                except Exception as e:
                    log.warning(f"Problem inserting data into Aggregated Table: {e}")

if __name__ == "__main__":
    main()

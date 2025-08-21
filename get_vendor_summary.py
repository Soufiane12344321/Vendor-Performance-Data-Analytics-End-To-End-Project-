import pandas as pd
import numpy as np
import sqlite3
import logging
import os

log_dir = 'logs'
# Check if the directory exists and create it if not
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
# --- End of the fix ---

# Configure logging
logging.basicConfig(filename=os.path.join(log_dir, "get_vendor_summary.log"),
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filemode="a"
)
def create_vendor_summary(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Creates a summary DataFrame of vendor sales, purchases, and freight costs.
    
    Args:
        conn: A sqlite3.Connection object.
        
    Returns:
        A pandas DataFrame with the combined vendor summary data.
    """
    sql_query = """
    WITH freightsummary AS (
        SELECT 
            vendorNumber,
            SUM(Freight) AS freight_cost
        FROM 
            vendor_invoice
        GROUP BY 
            vendorNumber
    ),
    purchase_summary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.PurchasePrice,
            p.description,
            pp.Volume,
            pp.Price AS actual_price,
            SUM(p.Quantity) AS total_quantity,
            SUM(p.Dollars) AS total_dollars
        FROM 
            purchases p
        JOIN 
            purchase_prices pp ON p.Brand = pp.Brand
        WHERE 
            p.PurchasePrice > 0
        GROUP BY 
            p.VendorNumber, p.VendorName, p.Brand
    ),
    sales_summary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS total_sales_quantity,
            SUM(SalesDollars) AS total_sales_dollars,
            SUM(SalesPrice) AS total_sales_price,
            SUM(ExciseTax) AS total_excise_tax
        FROM 
            sales
        GROUP BY 
            VendorNo, Brand
    )
    SELECT 
        p.VendorNumber,
        p.VendorName,
        p.Brand,
        p.PurchasePrice,
        p.description,
        p.Volume,
        p.actual_price,
        p.total_quantity,
        p.total_dollars,
        s.total_sales_quantity,
        s.total_sales_dollars,
        s.total_sales_price,
        s.total_excise_tax,
        f.freight_cost
    FROM 
        purchase_summary p
    LEFT JOIN 
        sales_summary s ON p.VendorNumber = s.VendorNo AND p.Brand = s.Brand
    LEFT JOIN 
        freightsummary f ON p.VendorNumber = f.VendorNumber
    ORDER BY 
        p.VendorNumber, p.Brand
    """
    
    try:
        logging.info("Attempting to create vendor summary DataFrame.")
        vendor_sales_summary = pd.read_sql_query(sql_query, conn)
        logging.info("Vendor summary DataFrame created successfully.")
        return vendor_sales_summary
    except Exception as e:
        logging.error(f"Error executing SQL query: {e}")
        return pd.DataFrame()

def clean_vendor_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and enhances the vendor summary DataFrame with calculated metrics.

    Args:
        df: The raw vendor summary DataFrame.

    Returns:
        The cleaned DataFrame with new calculated columns.
    """
    if df.empty:
        logging.warning("Input DataFrame is empty, skipping cleaning process.")
        return df

    try:
        # Convert volume to float and fill NaNs
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
        df.fillna(0, inplace=True)
        
        # Calculate new financial and performance metrics
        df['gross_profit'] = df['total_sales_dollars'] - df['total_dollars']
        
        # Handle division by zero for gross margin and sales/purchase ratio
        df['gross_margin'] = df['gross_profit'] / df['total_sales_dollars'].replace(0, np.nan)
        df['stock_turnover'] = df['total_sales_quantity'] / df['total_quantity'].replace(0, np.nan)
        df['sales_purchase_ratio'] = df['total_sales_dollars'] / df['total_dollars'].replace(0, np.nan)

        return df
    except KeyError as e:
        logging.error(f"Missing expected column in DataFrame: {e}")
        return df
    except Exception as e:
        logging.error(f"An unexpected error occurred during cleaning: {e}")
        return df

if __name__ == "__main__":
    db_path = 'invetory.db'
    conn = None  # Initialize conn to None for proper cleanup
    
    try:
        conn = sqlite3.connect(db_path)
        vendor_sales_summary = create_vendor_summary(conn)

        if not vendor_sales_summary.empty:
            cleaned_summary = clean_vendor_summary(vendor_sales_summary)
            output_file = 'vendor_sales_summary.csv'
            cleaned_summary.to_csv(output_file, index=False)
            logging.info(f"Vendor summary saved to {output_file} successfully.")
            print(f"Vendor summary saved to {output_file} successfully.")
        else:
            logging.error("No data to process. Exiting.")

    except sqlite3.Error as e:
        logging.critical(f"Database connection error: {e}")
    except Exception as e:
        logging.critical(f"An unhandled error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

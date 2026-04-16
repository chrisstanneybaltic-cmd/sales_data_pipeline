import pandas as pd        # read and work with CSV data as tables (DataFrames)
import glob                # search for files matching a pattern
import shutil              # copy files from one folder to another


def extract_data():
    """
    Extract step of the pipeline.

    What this function does:
    1. Looks for source files in the simulated SharePoint folder
    2. Copies those files into the raw layer (Bronze layer)
    3. Reads the data into pandas DataFrames
    4. Returns the DataFrames for further processing
    """

    print("Extracting data from source sharepoint folder...")

    # Step 1: Locate files in the landing zone (SharePoint folder)-----------------------------------------------------------
    # glob searches for files that match a pattern.
    # Example pattern: RetailSales_2026-01.csv
    # The * acts as a wildcard.
    sales_files = glob.glob("data/sharepoint/RetailSales_*.csv")
    order_files = glob.glob("data/sharepoint/OrderBank_*.csv")

    # If no files are found, stop the pipeline with a clear error.
    # This prevents confusing crashes later.
    if not sales_files or not order_files:
        raise FileNotFoundError("Source files missing in data/sharepoint/")

    # Select the first matching file found
    sales_path = sales_files[0]
    order_path = order_files[0]

    print("Found sales file:", sales_path)
    print("Found order file:", order_path)

    # Step 2: Copy files into the Raw layer (Bronze)-----------------------------------------------------------
    # In real-world pipelines:
    # - The landing zone contains incoming data
    # - The raw layer stores an immutable copy for audit and traceability
    #
    # We copy the file exactly as received.
    shutil.copy(sales_path, "data/raw/")
    shutil.copy(order_path, "data/raw/")

    print("Files copied to data/raw/ (Bronze layer)")

    # Step 3: Load data into pandas DataFrames-----------------------------------------------------------
    # Pandas reads the CSV file and converts it into a table structure.
    # This allows us to clean, transform, and analyse the data.
    retail_df = pd.read_csv(sales_path)
    order_df = pd.read_csv(order_path)

    # Print shape (rows, columns) to confirm data loaded correctly
    print("Retail dataset shape:", retail_df.shape)
    print("Order dataset shape:", order_df.shape)

    # Step 4: Return data for the Transform step-----------------------------------------------------------
    return retail_df, order_df
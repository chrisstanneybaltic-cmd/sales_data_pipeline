import pandas as pd
import os


def transform_data(retail_df, order_df):
    """
    Transform step of the pipeline
    Inputs:
      - retail_df: Retail Sales dataset (monthly totals)
      - order_df: Order Bank dataset (individual orders)

    What this script does:
      1) Cleans and standardises key fields (Silver)
      2) Creates an order_month field so orders can be compared monthly
      3) Aggregates orders to monthly level (so it matches Retail Sales granularity)
      4) Joins Retail Sales with the aggregated Order Bank data (Gold)
      5) Creates a simple KPI (average selling price)
      6) Saves Silver + Gold outputs locally
    """

    print("Transforming data...")

    # Step 1: Make copies (so we don’t modify original DataFrames)------------------------------------------------------------
    retail = retail_df.copy()
    orders = order_df.copy()

    # Step 2: Basic cleaning / standardisation (Silver)------------------------------------------------------------
    # Remove extra spaces in key fields (prevents join issues)
    retail["country"] = retail["country"].astype(str).str.strip()
    retail["dealer_id"] = retail["dealer_id"].astype(str).str.strip()
    retail["model"] = retail["model"].astype(str).str.strip()
    retail["powertrain"] = retail["powertrain"].astype(str).str.strip()

    orders["country"] = orders["country"].astype(str).str.strip()
    orders["dealer_id"] = orders["dealer_id"].astype(str).str.strip()
    orders["model"] = orders["model"].astype(str).str.strip()
    orders["powertrain"] = orders["powertrain"].astype(str).str.strip()

    # Convert date fields to proper datetime format
    retail["report_month"] = pd.to_datetime(retail["report_month"]).dt.to_period("M").astype(str)
    orders["order_date"] = pd.to_datetime(orders["order_date"])

    # Create a monthly field in orders so it matches the Retail Sales grain
    orders["order_month"] = orders["order_date"].dt.to_period("M").astype(str)

    # Convert numeric fields (protects against CSV reading numbers as text)
    retail["units_sold"] = pd.to_numeric(retail["units_sold"], errors="coerce")
    retail["revenue_local"] = pd.to_numeric(retail["revenue_local"], errors="coerce")
    orders["final_price_local"] = pd.to_numeric(orders["final_price_local"], errors="coerce")

    # Remove rows missing key join fields
    retail = retail.dropna(subset=["report_month", "country", "dealer_id", "model", "powertrain"])
    orders = orders.dropna(subset=["order_month", "country", "dealer_id", "model", "powertrain"])

    # Save Silver outputs (cleaned datasets)
    retail.to_csv("data/processed/silver/retail_sales_silver.csv", index=False)
    orders.to_csv("data/processed/silver/order_bank_silver.csv", index=False)
    print("Silver outputs saved to data/processed/silver/")

    # Step 3: Aggregate Order Bank to monthly totals------------------------------------------------------------
    # Retail Sales is already monthly totals, so we aggregate orders to monthly too
    order_summary = (
        orders.groupby(["order_month", "country", "dealer_id", "model", "powertrain"])
        .agg(
            order_count=("order_id", "count"),
            avg_configured_price=("final_price_local", "mean")
        )
        .reset_index()
    )

    # Step 4: Join Retail Sales with aggregated Orders (Gold)------------------------------------------------------------
    gold = pd.merge(
        retail,
        order_summary,
        left_on=["report_month", "country", "dealer_id", "model", "powertrain"],
        right_on=["order_month", "country", "dealer_id", "model", "powertrain"],
        how="left"
    )

    # Remove duplicate month column from the right table
    gold = gold.drop(columns=["order_month"], errors="ignore")

    # Step 5: Create KPI columns (Gold)------------------------------------------------------------
    # Average Selling Price = Revenue / Units Sold
    gold["avg_selling_price"] = gold["revenue_local"] / gold["units_sold"]

    # Save Gold output
    gold.to_csv("data/processed/gold/gold_sales_orders.csv", index=False)
    print("Gold output saved to data/processed/gold/gold_sales_orders.csv")
    print("Gold dataset shape:", gold.shape)

    return gold
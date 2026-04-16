import pandas as pd
from scripts.extract import extract_data
from scripts.transform import transform_data


def test_pipeline_creates_valid_gold_dataset():
    """
    Simple integration test.

    This test:
    1. Runs the extract step
    2. Runs the transform step
    3. Verifies that the Gold dataset is valid
    """

    # Run real pipeline steps
    retail_df, order_df = extract_data()
    gold_df = transform_data(retail_df, order_df)

    # --- Basic checks ---

    # Gold should be a DataFrame
    assert isinstance(gold_df, pd.DataFrame)

    # Gold should not be empty
    assert len(gold_df) > 0, "Gold dataset is empty."

    # Important KPI column must exist
    assert "avg_selling_price" in gold_df.columns, "Missing KPI column."

    # Units sold should not be negative
    assert (gold_df["units_sold"] >= 0).all(), "Negative units_sold found."

    # Revenue should not be negative
    assert (gold_df["revenue_local"] >= 0).all(), "Negative revenue found."
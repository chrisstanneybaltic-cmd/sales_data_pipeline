# Import the extract function from the scripts folder.
# This allows us to reuse logic written in extract.py.
from scripts.extract import extract_data
from scripts.transform import transform_data

def main():
       """
    Main entry point of the pipeline.

    This function controls the flow of the pipeline.
    Think of it as the 'manager' of the data process.
    It will the differents steps of the pipeline.
    """
       print("Starting pipeline...")
       retail_df, order_df = extract_data()
       print("Done extracting!")

        # Transform
       gold_df = transform_data(retail_df, order_df)
       print("Done transforming!")
       
       print("Pipeline completed successfully!")

 # Ensures the pipeline runs only when this file is executed directly,
 # not when it is imported by another script.
if __name__ == "__main__": 
    main()
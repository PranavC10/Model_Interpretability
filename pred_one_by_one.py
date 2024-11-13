import pandas as pd
import time  # Simulating a time-consuming function
from tqdm import tqdm
import os

# Sample data and function
df = pd.DataFrame({'input_column': range(1, 1001)})  # Adjust for actual data
def some_function(value):
    # Simulate a processing function, replace with actual function
    time.sleep(0.01)  # Replace with your actual processing
    return value * 2

# Log file to keep track of the progress
log_file = 'progress_log.txt'
output_file = 'processed_data.csv'

# Find the last completed row from the log file
if os.path.exists(log_file):
    with open(log_file, 'r') as f:
        # Get the last processed index from the log file
        lines = f.readlines()
        start_index = int(lines[-1].strip()) + 1 if lines else 0
else:
    start_index = 0  # Start from the beginning if no log file is found

# Initialize the output CSV file with headers if it doesn't exist
if not os.path.exists(output_file):
    # Create a new DataFrame with the same columns as `df` plus `prediction`
    df.iloc[[0]].assign(prediction=None).to_csv(output_file, index=False)

# Iterate with a progress bar, starting from the last processed index
for index in tqdm(range(start_index, len(df)), initial=start_index, total=len(df)):
    row = df.iloc[index]
    prediction = some_function(row['input_column'])

    # Add the prediction to the row and save it to the output CSV
    row_with_prediction = row.to_frame().T  # Convert Series to DataFrame for easier appending
    row_with_prediction['prediction'] = prediction
    row_with_prediction.to_csv(output_file, mode='a', header=False, index=False)

    # Append the index to the log file after processing each row
    with open(log_file, 'a') as f:
        f.write(f"{index}\n")

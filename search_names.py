import json
import pandas as pd
from tqdm import tqdm

# Load the customer dataframe
# Replace this with your actual dataframe loading code
customer_df = pd.DataFrame({
    'customer_id': [],  # Fill with actual customer IDs
    'customer_name': []  # Fill with actual customer names
})

# Load news data from JSONL file
jsonl_file = 'filtered_news.jsonl'

# Output CSV file and log file
output_csv = 'identified.csv'
log_file = 'processed_log.txt'

# Function to check if all words in the customer name are in the news body
def check_name_in_body(customer_name, body_text):
    words = customer_name.lower().split()  # Split customer name into words
    body_text_lower = body_text.lower()    # Lowercase the body for case-insensitive search
    return all(word in body_text_lower for word in words)

# Load processed pairs from log file
def load_processed_log():
    processed_pairs = set()
    try:
        with open(log_file, 'r') as f:
            for line in f:
                customer_id, news_id = line.strip().split(',')
                processed_pairs.add((customer_id, news_id))
    except FileNotFoundError:
        pass  # Log file does not exist yet
    return processed_pairs

# Append a processed pair to the log file
def append_to_log(customer_id, news_id):
    with open(log_file, 'a') as f:
        f.write(f"{customer_id},{news_id}\n")

# Prepare the output CSV with headers if it doesn't exist
columns = ['customer_id', 'customer_name', 'id', 'original_body', 'cleaned_body', 'title']  # Add other fields as necessary
try:
    with open(output_csv, 'x') as f:
        pd.DataFrame(columns=columns).to_csv(f, index=False)
except FileExistsError:
    pass  # File already exists, so we can append to it

# Load the set of processed customer-news pairs
processed_pairs = load_processed_log()

# Iterate through each customer and each news record
with open(jsonl_file, 'r') as f:
    for line in tqdm(f, desc="Processing news records"):
        news_record = json.loads(line)
        news_id = news_record.get('id')
        
        for _, customer_row in customer_df.iterrows():
            customer_id = str(customer_row['customer_id'])
            customer_name = customer_row['customer_name']
            
            # Skip if this customer-news pair has already been processed
            if (customer_id, news_id) in processed_pairs:
                continue
            
            # Check if all words in customer_name are in the news body
            if check_name_in_body(customer_name, news_record['original_body']):
                # Prepare the row with matched record details
                matched_record = {
                    'customer_id': customer_id,
                    'customer_name': customer_name,
                    'id': news_id,
                    'original_body': news_record.get('original_body'),
                    'cleaned_body': news_record.get('cleaned_body'),
                    'title': news_record.get('title'),
                    # Include other fields from news_record as needed
                }
                
                # Append the matched record to the output CSV
                pd.DataFrame([matched_record]).to_csv(output_csv, mode='a', header=False, index=False)
                
            # Log this customer-news pair as processed
            append_to_log(customer_id, news_id)

print(f"Matching records have been written to {output_csv}.")

import os
import json
import pandas as pd
from tqdm import tqdm

# Load your customer DataFrame
# Assuming the DataFrame has columns 'customer_id' and 'customer_name'
df_customers = pd.DataFrame({
    'customer_id': [1, 2, 3], 
    'customer_name': ['Company A', 'Company B', 'Company C']
})

# Set up paths
NEWS_FOLDER_PATH = 'path_to_your_json_files'  # Folder with news JSON files
OUTPUT_FILE = 'identified.csv'  # Output file for matches
PROCESSED_FILES_LOG = 'processed_files.log'  # Log file to track processed files

# Function to normalize text (lowercase and strip)
def normalize_text(text):
    return text.lower().strip()

# Load processed files from log to avoid reprocessing
def load_processed_files():
    """Load list of processed files from log."""
    if os.path.exists(PROCESSED_FILES_LOG):
        with open(PROCESSED_FILES_LOG, 'r') as f:
            return set(line.strip() for line in f)
    return set()

# Append file name to log after processing
def append_to_log(file_name):
    """Append file name to log after processing."""
    with open(PROCESSED_FILES_LOG, 'a') as f:
        f.write(f"{file_name}\n")

# Check for exact match of customer name in news body
def search_customer_in_news(customer_name, news_body):
    """Check if customer_name matches exactly in news_body."""
    return all(word in news_body for word in customer_name.split())

# Process a single JSON file to search for customer names in news bodies
def process_json_file(file_path):
    """Process a single JSON file and append matches to output CSV."""
    with open(file_path, 'r') as f:
        data = json.load(f)  # Load JSON array

        # Iterate through each news item in JSON file
        matches = []
        for news in data:
            # Normalize news body for exact match comparison
            news_body = normalize_text(news.get('body', ''))
            
            for _, customer in df_customers.iterrows():
                customer_id = customer['customer_id']
                customer_name = normalize_text(customer['customer_name'])
                
                # Check for exact match
                if search_customer_in_news(customer_name, news_body):
                    # Collect all necessary fields including 'published date'
                    matches.append({
                        'customer_id': customer_id,
                        'customer_name': customer_name,
                        'news_id': news.get('newsReferenceId'),  # Adjust ID field if needed
                        'title': news.get('title', ''),
                        'body': news_body,
                        'published_date': news.get('newsPublishedDate', ''),  # Add published date
                        'source': news.get('source', ''),  # Add source if available
                        'sentiment': news.get('sentiment', ''),  # Add sentiment if available
                        # Add any other fields from the JSON as needed
                    })

        # Append matches to output CSV
        if matches:
            pd.DataFrame(matches).to_csv(OUTPUT_FILE, mode='a', header=not os.path.exists(OUTPUT_FILE), index=False)

def main():
    # Load previously processed files
    processed_files = load_processed_files()
    
    # Process each JSON file in the folder that hasn’t been processed yet
    json_files = sorted([f for f in os.listdir(NEWS_FOLDER_PATH) if f.endswith('.json') and f not in processed_files])
    
    for file_name in tqdm(json_files, desc="Processing JSON files"):
        file_path = os.path.join(NEWS_FOLDER_PATH, file_name)
        
        try:
            # Process file and append to CSV if matches are found
            process_json_file(file_path)
            append_to_log(file_name)  # Log file as processed after successful processing
        except Exception as e:
            print(f"Error processing {file_name}: {e}")
            break  # Stop processing on error to avoid partial file corruption

if __name__ == "__main__":
    main()

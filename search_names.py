import json
import pandas as pd

# Load your customer DataFrame
# Assuming the DataFrame has columns 'customer_id' and 'customer_name'
df_customers = pd.DataFrame({
    'customer_id': [1, 2, 3], 
    'customer_name': ['Company A', 'Company B', 'Company C']
})

# Set up paths
INPUT_JSONL_FILE = 'path_to_your_huge_jsonl_file.jsonl'  # Path to the large JSONL file
OUTPUT_FILE = 'identified.jsonl'  # Output file for matches in JSONL format
PROCESSED_LINES_LOG = 'processed_lines.log'  # Log file to track processed lines for resumption

# Function to normalize text (lowercase and strip)
def normalize_text(text):
    return text.lower().strip()

# Load previously processed lines from the log to avoid reprocessing
def load_processed_lines():
    """Load the line number of processed records from the log."""
    if os.path.exists(PROCESSED_LINES_LOG):
        with open(PROCESSED_LINES_LOG, 'r') as f:
            return int(f.readline().strip())
    return 0

# Write last processed line number to the log
def update_processed_lines(line_number):
    """Write the last processed line number to the log."""
    with open(PROCESSED_LINES_LOG, 'w') as f:
        f.write(f"{line_number}\n")

# Check for exact phrase match of customer name in news body
def search_customer_in_news(customer_name, news_body):
    """Check if customer_name appears as an exact phrase in news_body."""
    return customer_name in news_body

# Process the JSONL file line by line
def process_jsonl_file():
    """Process the large JSONL file and append matches to output JSONL file."""
    last_processed_line = load_processed_lines()
    
    with open(INPUT_JSONL_FILE, 'r') as input_file, open(OUTPUT_FILE, 'a') as output_file:
        for line_number, line in enumerate(input_file):
            # Skip lines that have already been processed
            if line_number < last_processed_line:
                continue

            # Load the JSON object from the line
            news = json.loads(line.strip())
            news_body = normalize_text(news.get('body', ''))
            matches = []
            
            # Iterate through each customer to check for an exact phrase match
            for _, customer in df_customers.iterrows():
                customer_id = customer['customer_id']
                customer_name = normalize_text(customer['customer_name'])
                
                # Check for exact phrase match in the news body
                if search_customer_in_news(customer_name, news_body):
                    # Prepare the matched data as a JSON object
                    match = {
                        'customer_id': customer_id,
                        'customer_name': customer_name,
                        'news_id': news.get('newsReferenceId'),  # Adjust ID field if needed
                        'title': news.get('title', ''),
                        'body': news_body,
                        'published_date': news.get('newsPublishedDate', ''),  # Add published date
                        'source': news.get('source', ''),  # Add source if available
                        'sentiment': news.get('sentiment', ''),  # Add sentiment if available
                        # Add any other fields from the JSON as needed
                    }
                    # Write each matched result as a single line in JSONL format
                    output_file.write(json.dumps(match) + '\n')

            # Update the log with the last processed line number
            update_processed_lines(line_number + 1)

def main():
    # Process the large JSONL file line by line
    process_jsonl_file()

if __name__ == "__main__":
    main()

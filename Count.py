import os
import json
from tqdm import tqdm
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Ensure stopwords are downloaded
import nltk
nltk.download('stopwords')
nltk.download('punkt')

STOPWORDS = set(stopwords.words('english'))
MIN_WORD_COUNT = 10  # Set your minimum word count here
OUTPUT_FILE = 'filtered_news.jsonl'  # Output file for filtered data
PROCESSED_FILES_LOG = 'processed_files.log'  # Log file to track processed files

def load_processed_files():
    """Load list of processed files from log."""
    if os.path.exists(PROCESSED_FILES_LOG):
        with open(PROCESSED_FILES_LOG, 'r') as f:
            return set(line.strip() for line in f)
    return set()

def append_to_log(file_name):
    """Append file name to log after processing."""
    with open(PROCESSED_FILES_LOG, 'a') as f:
        f.write(f"{file_name}\n")

def clean_text(text):
    """Remove stopwords from text and return cleaned text."""
    words = word_tokenize(text.lower())  # Lowercase entire text
    cleaned_words = [word for word in words if word.isalnum() and word not in STOPWORDS]
    return " ".join(cleaned_words)

def filter_news_object(obj):
    """Filter a news object based on word count after removing stopwords."""
    if 'body' not in obj:
        return None  # Discard objects without a 'body'
    
    original_body = obj['body'].lower()  # Lowercase the original body text
    cleaned_body = clean_text(original_body)
    word_count = len(cleaned_body.split())
    
    if word_count < MIN_WORD_COUNT:
        return None  # Discard objects with fewer than MIN_WORD_COUNT words
    
    # Keep original and cleaned body along with other necessary columns
    return {
        'id': obj.get('newsReferenceId', None),  # Adjust based on available ID field
        'original_body': original_body,
        'cleaned_body': cleaned_body,
        # Add other necessary fields if needed
    }

def process_json_file(file_path):
    """Process a single JSON file, filter objects, and append results to output file."""
    records_written = 0
    with open(file_path, 'r') as f:
        try:
            data = json.load(f)  # Load the entire file as a JSON array
            for obj in data:
                filtered_obj = filter_news_object(obj)
                if filtered_obj:
                    with open(OUTPUT_FILE, 'a') as output_file:
                        output_file.write(json.dumps(filtered_obj) + '\n')
                    records_written += 1  # Increment count of records written
        except json.JSONDecodeError:
            print(f"Skipping invalid JSON file: {file_path}")
    print(f"Processed {file_path}: {records_written} records written")  # Debugging output
    return records_written > 0

def main():
    folder_path = 'path_to_your_json_files'  # Update to your JSON folder path
    processed_files = load_processed_files()
    
    json_files = sorted([f for f in os.listdir(folder_path) if f.endswith('.json') and f not in processed_files])
    
    for file_name in tqdm(json_files, desc="Processing JSON files"):
        file_path = os.path.join(folder_path, file_name)
        
        try:
            if process_json_file(file_path):
                append_to_log(file_name)  # Log file as processed only if records were written
        except Exception as e:
            print(f"Error processing {file_name}: {e}")
            break  # Stop processing on error to avoid partial file corruption

if __name__ == "__main__":
    main()

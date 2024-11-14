import torch
from transformers import pipeline, LongformerTokenizer, LongformerForSequenceClassification
import pandas as pd
import gc  # For garbage collection to free memory

# Load your DataFrame (replace with your actual DataFrame)
# Assuming the DataFrame has a column 'original_body'
df = pd.DataFrame({'original_body': ["Your large text data goes here..."] * 100})  # Example data with 100 entries

# Load Longformer model and tokenizer with GPU support
tokenizer = LongformerTokenizer.from_pretrained("allenai/longformer-base-4096")
model = LongformerForSequenceClassification.from_pretrained("allenai/longformer-base-4096")
device = 0 if torch.cuda.is_available() else -1  # Use GPU if available

# Initialize the sentiment-analysis pipeline
sentiment_pipeline = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer, device=device)

# Batch processing to manage GPU memory
batch_size = 8  # Adjust batch size based on GPU memory capacity
results = []

for i in range(0, len(df), batch_size):
    # Select batch, ensuring it doesn’t exceed the DataFrame length
    batch_texts = df['original_body'].iloc[i:i + batch_size].tolist()

    # Run sentiment analysis on the batch
    try:
        batch_results = sentiment_pipeline(batch_texts)
    except Exception as e:
        print(f"Error processing batch starting at index {i}: {e}")
        batch_results = [{"label": "Error"} for _ in batch_texts]  # Assign "Error" if there’s a failure

    # Extract labels and append to results list
    results.extend([result['label'] for result in batch_results])

    # Clear GPU cache to free memory
    torch.cuda.empty_cache()
    gc.collect()

# Ensure results length matches the DataFrame length (handle any discrepancies gracefully)
df['sentiment'] = results[:len(df)]  # Slicing results in case of length mismatch

# Save the updated DataFrame to a CSV
df.to_csv("sentiment_output.csv", index=False)

print("Sentiment analysis completed and saved.")

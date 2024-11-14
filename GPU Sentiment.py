import torch
from transformers import pipeline, DistilBertTokenizer, DistilBertForSequenceClassification
import pandas as pd
import gc
from tqdm import tqdm

# Load your DataFrame (replace with your actual DataFrame)
# Assuming the DataFrame has a column 'original_body'
df = pd.DataFrame({'original_body': ["Your large text data goes here..."] * 100})  # Example data with 100 entries

# Load DistilBERT model and tokenizer with GPU support
tokenizer = DistilBertTokenizer.from_pretrained("distilbert-base-uncased-finetuned-sst-2-english")
model = DistilBertForSequenceClassification.from_pretrained("distilbert-base-uncased-finetuned-sst-2-english")

# Try to use GPU if available, otherwise fall back to CPU
device = 0 if torch.cuda.is_available() else -1

# Initialize the sentiment-analysis pipeline with return_all_scores=True
sentiment_pipeline = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer, device=device, return_all_scores=True)

# Function to calculate negative probability for large text by chunking
def get_negative_probability(text, chunk_size=512):
    tokens = tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=chunk_size)
    input_ids = tokens["input_ids"][0]
    num_chunks = len(input_ids) // chunk_size + (1 if len(input_ids) % chunk_size > 0 else 0)

    negative_probs = []
    for i in range(num_chunks):
        chunk_text = tokenizer.decode(input_ids[i * chunk_size: (i + 1) * chunk_size], skip_special_tokens=True)

        # Attempt to run on GPU, fallback to CPU in case of memory errors
        try:
            result = sentiment_pipeline(chunk_text)
        except RuntimeError as e:
            if 'out of memory' in str(e):
                print("Out of memory error encountered. Switching to CPU for this chunk.")
                # Clear GPU cache
                torch.cuda.empty_cache()
                gc.collect()
                
                # Switch to CPU for this pipeline instance
                sentiment_pipeline_cpu = pipeline(
                    "sentiment-analysis", model=model, tokenizer=tokenizer, device=-1, return_all_scores=True
                )
                result = sentiment_pipeline_cpu(chunk_text)
            else:
                raise e  # Re-raise if it's a different error

        # Extract the probability for "NEGATIVE"
        neg_prob = next(item['score'] for item in result[0] if item['label'] == "NEGATIVE")
        negative_probs.append(neg_prob)

        # Clear GPU cache after processing each chunk
        torch.cuda.empty_cache()
        gc.collect()

    # Return the average negative probability across all chunks
    return sum(negative_probs) / len(negative_probs) if negative_probs else 0

# Process each row with a progress bar and save the negative probability in a new column
df['neg_probability'] = [
    get_negative_probability(text) for text in tqdm(df['original_body'], desc="Calculating Negative Probabilities")
]

# Save the updated DataFrame to a CSV
df.to_csv("sentiment_output_with_neg_probability.csv", index=False)

print("Negative probability analysis completed and saved.")

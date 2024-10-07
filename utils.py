import io
import os
import re
import sys
import json
import gcsfs
import tempfile
from pathlib import Path
from datetime import datetime
from google.cloud import storage
from tqdm import tqdm
import polars as pl
import re
import pandas as pd

import ollama.client as client


def read_data(bucket_name: str, file_path: str) -> pl.DataFrame:
    """
    Reads data from a file stored in a Google Cloud Storage (GCS) bucket and returns it as a DataFrame.

    Args:
        bucket_name (str): The name of the GCS bucket where the file is stored.
        file_path (str): The path to the file in the GCS bucket.

    Returns:
        pl.DataFrame: A DataFrame containing the data read from the file.

    Raises:
        ValueError: If the file format is not supported. Only .csv and .xlsx files are supported.
    """
    gcs = gcsfs.GCSFileSystem()
    bucket_path = f'gs://{bucket_name}/{file_path}'
    
    with gcs.open(bucket_path, "rb") as fh:
        if file_path.endswith('.csv'):
            dataframe = pl.read_csv(fh)
        elif file_path.endswith('.xlsx'):
            dataframe = pl.read_excel(fh, engine="openpyxl")
        else:
            raise ValueError("Unsupported file format. Please use a .csv or .xlsx file.")
        
    return dataframe

def read_excel_sheet(bucket_name: str, file_path: str, sheet_name: str) -> pl.DataFrame:
    """
    Reads a specific sheet from an Excel file stored in a Google Cloud Storage (GCS) bucket and returns it as a DataFrame.

    Args:
        bucket_name (str): The name of the GCS bucket where the file is stored.
        file_path (str): The path to the file in the GCS bucket.
        sheet_name (str): The name of the sheet to read from the Excel file.

    Returns:
        pl.DataFrame: A DataFrame containing the data read from the sheet.

    Raises:
        ValueError: If the file format is not supported. Only .xlsx files are supported.
    """
    gcs = gcsfs.GCSFileSystem()
    bucket_path = f'gs://{bucket_name}/{file_path}'
    
    with gcs.open(bucket_path, "rb") as fh:
        if file_path.endswith('.xlsx'):
            dataframe = pl.read_excel(fh, sheet_name=sheet_name, engine="openpyxl")
        else:
            raise ValueError("Unsupported file format. Please use a .xlsx file.")
        
    return dataframe

def text_generation(messages: str, model: str) -> str:
    """
    Generates text using a specified model and message prompt.

    Args:
        messages (str): The message prompt.
        model (str): The model to be used for text generation.

    Returns:
        str: The generated text.
    """
    old_stdout = sys.stdout
    sys.stdout = text_trap = io.StringIO()
    response, _ = client.generate(model_name=model, prompt=messages)
    sys.stdout = old_stdout
    return str(response)

def generate_market_analysis(texts_df_grouped, text_generation_function):
    """
    Apply the text generation function to each row and create a new dataframe.

    Parameters:
    texts_df_grouped (DataFrame): A DataFrame containing the grouped text data.
    text_generation_function (function): The function to generate text based on the input.

    Returns:
    DataFrame: A DataFrame with the generated text for market analysis.
    """
    # Apply the text generation function to each row and create a new dataframe
    generated_texts_df = texts_df_grouped.assign(
        Generated_Text=lambda df: df['Text'].apply(
            lambda text: text_generation_function(f"""Act as a journalist with expertise in market analytics. Please analyze the following content: {text}. Identify from {text} a comprehensive analysis related to AI Powered Care and it's key funding statistics, market growth potential, and market size evaluation.

Follow this template for your answer: 

### 1. Funding Statistics:
- **Total Funding Amount**: 
  - Describe the cumulative amount of funding received in the European tech ecosystem. Highlight the most recent figures, comparing current investment levels to previous years.
  
- **Top Investors**: 
  - Identify the key investors, including venture capital firms, angel investors, and corporations, that are actively funding tech companies in Europe. List notable investors and include the funding amounts associated with each.

- **Top Funded Projects or Companies**: 
  - Provide a list of the most funded projects or companies in the European tech sector. Specify the total funding amount received by each company, highlighting key examples.

- **Funding as a Percentage of Market Size**: 
  - Calculate and explain the ratio of total funding to the estimated market size of the European tech ecosystem, providing context on how investment scales against the overall market.

### 2. Potential Market Growth:
- Project the potential market growth in percentage terms for the coming years starting from 2024. Include factors contributing to market growth and any relevant trends impacting the European tech landscape.

### 3. Market Size Evaluation:
- Evaluate the current market size of the European tech ecosystem in USD. Use recent valuations, economic indicators, and market data to provide a detailed market size estimate.

### 4. TAM (Total Addressable Market):
- Define the Total Addressable Market (TAM) as the maximum potential customer base a company could achieve if it captured 100% of the market share. Provide a calculation based on the current market size and market dynamics.

## Additional Requirements:
- Use specific examples and relevant statistics to support each point.
- Highlight key insights and trends within the European tech funding landscape.
- Ensure the data presented is up-to-date and accurately reflects the current market conditions.

## Output Format:
- Present the information in a structured report format, with clear headings and subheadings for each section.
- Include tables or charts where relevant to visualize key statistics.""", "llama3")
        )
    )
    return generated_texts_df


def extract_sections_from_generated_texts(generated_texts_df):
    """
    Extract sections from generated text and create a DataFrame.

    Parameters:
    generated_texts_df (DataFrame): A DataFrame containing generated text and topics.

    Returns:
    DataFrame: A DataFrame with extracted sections and their respective topics.
    """
    # Function to extract sections from the input text
    def extract_sections(text):
        # Clean the text by removing any occurrences of '**'
        cleaned_text = re.sub(r'\*\*', '', text)

        # Regular expressions to match each section
        sections = {
            'Key Statistics': re.search(r'### Key Statistics:\n\n(.*?)\n\n###', cleaned_text, re.DOTALL),
            'Trends': re.search(r'### Trends:\n\n(.*?)\n\n###', cleaned_text, re.DOTALL),
            'Competitive Insights': re.search(r'### Competitive Insights:\n\n(.*?)\n\n###', cleaned_text, re.DOTALL),
            'Consumer Insights': re.search(r'### Consumer Insights:\n\n(.*?)\n\n###', cleaned_text, re.DOTALL),
            'Emerging Opportunities or Threats': re.search(r'### Emerging Opportunities or Threats:\n\n(.*)', cleaned_text, re.DOTALL)
        }
        
        # Extracting the content for each section
        extracted_data = {section: (match.group(1).strip() if match else '') for section, match in sections.items()}
        
        return extracted_data

    # List to hold all extracted data
    all_extracted_data = []

    # Iterating over each row in generated_texts_df
    for _, row in generated_texts_df.iterrows():
        # Extracting sections from the current text
        extracted_data = extract_sections(row['Generated_Text'])
        # Adding the Topic to the extracted data
        extracted_data['Topic'] = row['Topic']
        # Appending to the list
        all_extracted_data.append(extracted_data)

    # Creating a DataFrame from the list of extracted data
    final_df = pd.DataFrame(all_extracted_data)
    
    return final_df

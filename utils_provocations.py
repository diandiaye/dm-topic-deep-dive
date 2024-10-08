import io
import os
import re
import sys
import json
import time
import gcsfs
import tempfile
from pathlib import Path
from datetime import datetime
from google.cloud import storage
from typing import List, Dict, Tuple, Any, Optional, Union
import pandas as pd
import ollama.client as client
import polars as pl
from tqdm import tqdm

def save_dataframe_to_gcs(dataframe: pl.DataFrame, bucket: str, file_path: str) -> None:
    """
    Save a Polars DataFrame to a Google Cloud Storage bucket as an Excel file.

    Args:
        dataframe: The Polars DataFrame to be saved.
        bucket: The name of the Google Cloud Storage bucket.
        file_path: The path of the file within the bucket.
        
    Returns:
        None
    """
    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmpfile:
        temp_file_name = tmpfile.name
    
    # Save the DataFrame to the temporary Excel file
    dataframe.write_excel(temp_file_name)
    
    # Initialize GCSFileSystem
    fs = gcsfs.GCSFileSystem()
    
    # Upload the Excel file to GCS
    with open(temp_file_name, 'rb') as f:
        with fs.open(f'{bucket}/{file_path}', 'wb') as gcs_file:
            gcs_file.write(f.read())
    
    # Remove the temporary file
    os.remove(temp_file_name)

def read_config(bucket_name: str, config_path: str) -> Tuple[dict, dict]:
    """
    Reads the contents of a configuration file stored in a Google Cloud Storage (GCS) bucket and extracts the values of 'description_parameters' and 'polarity_parameters'.

    Args:
        bucket_name (str): The name of the GCS bucket where the configuration file is stored.
        config_path (str): The path to the configuration file in the GCS bucket.

    Returns:
        Tuple[dict, dict]: A tuple containing 'desc_params' and 'polarity_params', which are dictionaries representing the extracted values.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(config_path)
    # here you get string
    str_json = blob.download_as_text()
    # now you have as dict
    dict_result = json.loads(str_json)
    desc_params = dict_result.get('description_parameters', {})
    polarity_params = dict_result.get('polarity_parameters', {})
    return desc_params, polarity_params

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
    # Create a GCSFileSystem object
    gcs = gcsfs.GCSFileSystem()
    
    # Formulate the full path to the file
    bucket_path = f'gs://{bucket_name}/{file_path}'
    
    # Open the file
    with gcs.open(bucket_path, "rb") as fh:
        # Determine the file type from the file extension and read accordingly
        if file_path.endswith('.csv'):
            dataframe = pl.read_csv(fh)
        elif file_path.endswith('.xlsx'):
            dataframe = pl.read_excel(fh, engine="openpyxl")
        else:
            raise ValueError("Unsupported file format. Please use a .csv or .xlsx file.")
        
    return dataframe

def read_excel_sheet(bucket_name: str, file_path, sheet_name):
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
    # Create a GCSFileSystem object
    gcs = gcsfs.GCSFileSystem()
    
    # Formulate the full path to the file
    bucket_path = f'gs://{bucket_name}/{file_path}'
    
    # Open the file
    with gcs.open(bucket_path, "rb") as fh:
        # Determine the file type from the file extension and read accordingly
        if file_path.endswith('.xlsx'):
            dataframe = pl.read_excel(fh, sheet_name=sheet_name, engine="openpyxl")
        else:
            raise ValueError("Unsupported file format. Please use a .xlsx file.")
        
    return dataframe

def save_dataframe_to_gcs_pd(dataframe: pd.DataFrame, bucket: str, file_path: str) -> None:
    """
    Save a Pandas DataFrame to a Google Cloud Storage bucket as an Excel file.

    Args:
        dataframe: The Pandas DataFrame to be saved.
        bucket: The name of the Google Cloud Storage bucket.
        file_path: The path of the file within the bucket.
        
    Returns:
        None
    """
    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmpfile:
        temp_file_name = tmpfile.name
    
    # Save the DataFrame to the temporary Excel file
    dataframe.to_excel(temp_file_name)
    
    # Initialize GCSFileSystem
    fs = gcsfs.GCSFileSystem()
    
    # Upload the Excel file to GCS
    with open(temp_file_name, 'rb') as f:
        with fs.open(f'{bucket}/{file_path}', 'wb') as gcs_file:
            gcs_file.write(f.read())
    
    # Remove the temporary file
    os.remove(temp_file_name)

def text_generation(messages: str, model: str) -> str:
    """
    Generates text using a specified model and message prompt.

    Parameters:
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

def load_config(file_path: str) -> Dict:
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

# Global variables
THEMES = [
    "Technological Innovation",
    "Societal Impact",
    "Regulatory Changes",
    "Economic Transformation",
    "Lifestyle Changes",
    "Environmental Shifts",
    "Cultural Evolution",
    "Educational Reforms"
]

TOPICS_CLASSIFICATION_PROMPT_TEMPLATE = """
I have a set of themes and a list of topics with corresponding keywords. Each topic is associated with only one theme (the most related one) based on the context provided by its keywords. Your task is to classify each topic into only one theme (the most related one) below based on its keywords. If a topic clearly aligns with multiple themes, assign it to the most relevant one. The themes are as follows:

- **Technological Innovation**: Keywords related to innovation, technology, AI, machine learning, software, etc.
- **Societal Impact**: Keywords related to society, community, social structures, demographics, etc.
- **Regulatory Changes**: Keywords related to law, policy, regulation, compliance, etc.
- **Economic Transformation**: Keywords related to the economy, market trends, finance, currency, etc.
- **Lifestyle Changes**: Keywords related to life habits, routines, work-life balance, personal development, etc.
- **Environmental Shifts**: Keywords related to the environment, climate, sustainability, green initiatives, etc.
- **Cultural Evolution**: Keywords related to culture, art, media, entertainment, traditions, etc.
- **Educational Reforms**: Keywords related to education, learning, schools, universities, pedagogy, etc.

For the topic "{topic}" with the following keywords: {keywords}, classify it into only one theme (the most related one) from the list above. Please, just give the attributed theme, no additional comments.
"""

def classify_topics_into_themes(df: pl.DataFrame) -> pl.DataFrame:
    """
    Classify a list of topics into predefined themes based on associated keywords.

    Args:
        df (pl.DataFrame): A Polars DataFrame with columns 'Topic', 'Keyword', and 'Description'.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the topics, keywords, descriptions, and attributed themes.
    """
    # Define a function to classify each row
    def classify_row(row):
        topic, keyword_list, description = row

        # Create a specific prompt for the current topic and its keywords
        specific_prompt = TOPICS_CLASSIFICATION_PROMPT_TEMPLATE.format(
            topic=topic, keywords=", ".join(keyword_list)
        )

        # Generate a response using the model
        response = text_generation(specific_prompt, "llama3")

        # Search for the mentioned themes in the response
        attributed_themes = []
        for theme in THEMES:
            if theme.lower() in response.lower():
                attributed_themes.append(theme)

        return ", ".join(attributed_themes)

    # Apply the classify_row function to each row using map_rows
    attributed_themes = df.select(["Topic", "Keyword", "Description"]).map_rows(classify_row)

    # Add the 'Attributed Themes' column to the DataFrame
    result_df = df.with_columns(
        pl.Series(name="Attributed Themes", values=attributed_themes)
    )

    # Format the 'Keyword' column as a string for better readability
    result_df = result_df.with_columns(
        pl.col("Keyword").map_elements(lambda k: ", ".join(k), return_dtype=pl.Utf8).alias("Keywords")
    )

    # Select and reorder the columns as needed
    result_df = result_df.select(
        ["Topic", "Keywords", "Description", "Attributed Themes"]
    )

    return result_df

def load_prompts(json_file_path):
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    return data

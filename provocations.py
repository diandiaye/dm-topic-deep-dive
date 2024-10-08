import polars as pl
import numpy as np
import json
from utils_provocations import read_data, classify_topics_into_themes, load_prompts, text_generation

def generate_provocations(topic_description, topic_keywords):

    # Step 2: Rename and group topic keywords
    print("Step 2: Renaming columns and grouping topic keywords...")
    topic_keywords = topic_keywords.rename({
        "keyword": "Keyword"
    })
    df = topic_keywords.group_by("Topic").agg(pl.col('Keyword'))

    # Step 3: Join data and drop unnecessary columns
    print("Step 3: Joining data and dropping unnecessary columns...")
    data = topic_description.join(df, on="Topic", how="outer").drop("Topic_right")

    # Step 4: Classify topics into themes
    print("Step 4: Classifying topics into themes...")
    df_results = classify_topics_into_themes(data.head(1))

    # Step 5: Load prompts
    print("Step 5: Loading prompt templates...")
    company = "Coty"
    prompt = load_prompts("prompt.json")

    # Function to generate 2 responses based on prompt and template
    def generate_responses(row: dict) -> list:
        theme = row["Attributed Themes"]
        topic = row["Topic"]
        description = row["Description"]
        
        # Generate the prompt from the template
        prompt_template = prompt.get(theme, {}).get("Prompt", "")
        generated_prompt = prompt_template.format(topic=topic, description=description, company=company)
        
        # Generate 3 responses using the text generation function
        responses = []
        for _ in range(2):
            response = text_generation(generated_prompt, "llama3")
            
            # Ensure the response starts with "Imagine if"
            if not response.startswith("Imagine if"):
                response = "Imagine if " + response.split("Imagine if", 1)[-1].strip()
            
            # Take only the first sentence after "Imagine if"
            response = response.split(".", 1)[0] + "."
            
            responses.append(response)
        
        return responses

    # Step 6: Generate 3 responses for each topic using `map_elements`
    print("Step 6: Generating 3 responses for each topic using map_elements...")

    # Use Polars `map_elements` to apply the `generate_responses` function to each row
    df_results = df_results.with_columns(
        pl.struct(["Attributed Themes", "Topic", "Description"])
        .map_elements(generate_responses, return_dtype=pl.List(pl.Utf8))  # Ensure we return a list of strings (Provocations)
        .alias("Provocations")
    )

    # Step 7: No need to convert to Pandas; work directly with Polars DataFrame
    print("Step 7: Working directly with Polars DataFrame...")

    # Step 8: No need to apply additional transformation, Polars handles lists natively
    print("Step 8: Polars natively handles lists, skipping manual conversion...")

    # Step 9: Convert to JSON format directly from Polars DataFrame
    print("Step 9: Converting to JSON...")
    json_data = df_results.select(["Topic", "Attributed Themes", "Provocations"]).to_dicts()

    # Step 10: Save the JSON data into a file
    file_path = 'topics_provocations.json'
    with open(file_path, 'w') as json_file:
        json.dump(json_data, json_file, indent=4)
    print("Script execution completed.")
    return json_data

    

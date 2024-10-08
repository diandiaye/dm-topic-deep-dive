from typing import List, Dict, Optional
from tqdm import tqdm
from serpapi import GoogleSearch
import pandas as pd
import requests
import justext
import re
import requests
import justext
import re
from typing import Optional, Union
from PyPDF2 import PdfReader
from io import BytesIO
from scrapegraphai.graphs import SmartScraperGraph
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Any, Union
import json
from tqdm import tqdm
import json
import pandas as pd
import nest_asyncio
nest_asyncio.apply()
from utils import text_generation
import re
import nest_asyncio
from typing import List, Dict
import pandas as pd
from tqdm import tqdm
from typing import List, Tuple

# Define types for better readability and type checking
PromptDict = Dict[str, str]
ResultDict = Dict[str, Any]


def fetch_search_results(query: str, api_key: str, time_range: Optional[str] = None) -> List[Dict[str, Optional[str]]]:
    """
    Fetches search results for a given query using SerpAPI with an optional time range filter.

    Parameters:
        query (str): The search query.
        api_key (str): The API key for SerpAPI.
        time_range (Optional[str]): The time range for search results (e.g., 'd' for past day, 
                                    'w' for past week, 'm' for past month). Default is None.

    Returns:
        List[Dict[str, Optional[str]]]: A list of formatted search result dictionaries.
    """
    formatted_results: List[Dict[str, Optional[str]]] = []

    # Mapping time_range to SerpAPI tbs values
    time_filters = {
        'hour': 'qdr:h',   # Past hour
        'day': 'qdr:d',    # Past day
        'week': 'qdr:w',   # Past week
        'month': 'qdr:m',  # Past month
        'year': 'qdr:y'    # Past year
    }
    tbs_value = time_filters.get(time_range, '')

    search_params = {
        "q": query,
        "api_key": api_key,
        "num": 250,
        "tbs": tbs_value  # Apply the time filter if provided
    }
    
    search = GoogleSearch(search_params)
    result = search.get_dict()

    for item in result.get("organic_results", []):
        formatted_item = {
            "title": item.get("title"),
            "date": item.get("date"),
            "url": item.get("link"),
            "tags_matched": item.get("snippet_highlighted"),
        }
        formatted_results.append(formatted_item)

    return formatted_results


def scrape(url: str) -> Optional[requests.Response]:
    """
    Scrapes content from a given URL.
    
    Parameters:
        url (str): The URL to scrape.
    
    Returns:
        Optional[requests.Response]: The HTTP response if successful, None otherwise.
    """
    try:
        response = requests.get(url)
        return response
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None

def parse_pdf_content(pdf_content: bytes) -> str:
    """
    Parses text content from a PDF file.
    
    Parameters:
        pdf_content (bytes): The PDF file content as bytes.
    
    Returns:
        str: The extracted text from the PDF.
    """
    try:
        reader = PdfReader(BytesIO(pdf_content))
        text = []
        for page in reader.pages:
            text.append(page.extract_text())
        return ' '.join(text).strip()
    except Exception as e:
        print(f"Error parsing PDF content: {e}")
        return ""

def parse_html_content(response: requests.Response) -> str:
    """
    Parses content from the HTTP response using jusText.
    
    Parameters:
        response (requests.Response): The HTTP response object.
    
    Returns:
        str: The extracted and concatenated text.
    """
    eng_stoplist = justext.get_stoplist("English")
    fr_stoplist = justext.get_stoplist("French")
    stoplist = eng_stoplist.union(fr_stoplist)
    
    try:
        paragraphs = justext.justext(response.content, stoplist)
        filtered_paragraphs = [paragraph.text for paragraph in paragraphs if not paragraph.is_boilerplate]
        concatenated_text = '. '.join(filtered_paragraphs)
        concatenated_text = re.sub(r"\.\. ", ". ", concatenated_text)
        return concatenated_text
    except Exception as e:
        print(f"Error parsing HTML content: {e}")
        return ""

def parse_content(response: requests.Response) -> str:
    """
    Determines the type of content and parses it accordingly.
    
    Parameters:
        response (requests.Response): The HTTP response object.
    
    Returns:
        str: The extracted text from the content.
    """
    content_type = response.headers.get('Content-Type', '').lower()
    if 'application/pdf' in content_type:
        return parse_pdf_content(response.content)
    else:
        return parse_html_content(response)

def process_scraping(url: str) -> Optional[str]:
    """
    Processes scraping and parsing for a given URL.
    
    Parameters:
        url (str): The URL to process.
    
    Returns:
        Optional[str]: The extracted content text if successful, None otherwise.
    """
    response = scrape(url)
    if response:
        return parse_content(response)
    return None


def run_smart_scraper(prompts: PromptDict, df: Any, field: str, topic: str, OPENAI_API_KEY:str) -> List[ResultDict]:
    """
    Run the SmartScraperGraph on a list of URLs with specified prompts.

    Args:
        prompts (PromptDict): Dictionary with prompt names as keys and prompt texts as values.
        df (Any): DataFrame containing the URLs to be scraped.
        field (str): Specific field to validate in the scraping results.
        topic (str): Topic associated with the scraping.

    Returns:
        List[ResultDict]: A list of dictionaries with the results of the scraping.
    """

    # Configuration for the graph model
    graph_config: Dict[str, Any] = {
        "llm": {
            "api_key": OPENAI_API_KEY,
            "model": "openai/gpt-4-turbo",
            "temperature": 0,
        },
        "verbose": True,
        "headless": True
    }

    all_results: List[ResultDict] = []
    last_result: Optional[ResultDict] = None

    # Ensure URLs are correctly populated
    urls: List[str] = list(df.URL)

    def scrape_url(url: str, prompt_name: str, prompt: str) -> Optional[ResultDict]:
        """
        Scrape a single URL with the given prompt and validate the result.

        Args:
            url (str): The URL to be scraped.
            prompt_name (str): The name of the prompt.
            prompt (str): The prompt text.

        Returns:
            Optional[ResultDict]: The result dictionary if a valid result is found, otherwise None.
        """
        nonlocal last_result
        try:
            # Initialize the SmartScraperGraph
            smart_scraper_graph = SmartScraperGraph(
                prompt=prompt,
                source=url,
                config=graph_config
            )

            # Run the smart scraper on the URL
            result = smart_scraper_graph.run()

            # Save the last attempted result
            last_result = {
                "result": result,
                "topic": topic,
                "url": url,
                "analysis_type": prompt_name
            }

            # Validation for specific fields
            main_field_data: Dict[str, str] = result.get(field, {})
            valid_result = False

            if isinstance(main_field_data, dict):
                cagr = main_field_data.get('CAGR', '')
                interpretation = main_field_data.get('Interpretation', '')
                estimated_market_size = main_field_data.get('Estimated Market Size', '')
                description = main_field_data.get('Description', '')

                # Validate if any of the key fields are present and correctly populated
                valid_result = (
                    isinstance(cagr, str) and cagr.strip() not in ['NA', '', 'No amount found'] or
                    isinstance(interpretation, str) and interpretation.strip() not in ['NA', '', 'No amount found'] or
                    isinstance(estimated_market_size, str) and estimated_market_size.strip() not in ['NA', '', 'No amount found'] or
                    isinstance(description, str) and description.strip() not in ['NA', '', 'No amount found']
                )

            # Stop the process if a valid result is found
            if valid_result:
                print(f"Valid result found, stopping process. URL: {url}")
                return last_result
        except Exception as e:
            if "context length exceeded" in str(e):
                print(f"Skipping due to context length error: {e}")
            else:
                print(f"An error occurred: {e}")
        return None

    # Use ThreadPoolExecutor for parallel scraping
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(scrape_url, url, prompt_name, prompt)
            for prompt_name, prompt in prompts.items()
            for url in urls
        ]

        # Process results as they complete
        for future in as_completed(futures):
            result = future.result()
            if result:
                all_results.append(result)
                # Break the loop early if a valid result is found
                break

    # If no valid results were found, append the last result
    if not all_results and last_result is not None:
        all_results.append(last_result)
        print(f"No valid results found. Taking the last result as the final output: {last_result['url']}")

    return all_results


nest_asyncio.apply()

def get_market(topic: str) -> list[str]:
    """
    Identify relevant markets associated with a given topic.

    This function takes a topic as input and generates a list of two relevant markets related to the topic.
    The function uses a language model to identify markets based on business sectors, industries, types of 
    products or services involved, and any economic trends related to the topic.

    Args:
        topic (str): The topic for which to identify relevant markets.

    Returns:
        list[str]: A list of relevant market names related to the topic.

    Example:
        >>> get_market('Face & Body Regeneration')
        ["Healthcare Market", "AI in Healthcare Market"]
    """
    
    prompt = (
        f"Based on the following topic: {topic}, can you identify the 2 relevant market(s) associated with this topic? "
        "Describe the relevant markets considering the business sectors, industries, types of products or services involved, "
        "and any economic trends related to the topic. Give just a list of market, no additional comment.\n\n"
        "Follow this template for your answer:\n\n"
        '["Healthcare Market", "AI in Healthcare Market", "Digital Health Market", "Telemedicine Market"]'
    )
    text_gen = text_generation(prompt, "llama3")

    # Extract markets from the generated text using regex
    markets_list = re.findall(r'"\s*(.*?)\s*"', text_gen)
    
    return markets_list


def get_prompts(topic: str, domain: str) -> List[Tuple[str, str]]:
    """
    Generate a list of prompts related to market analysis for a given topic and domain.

    This function generates various prompts to gather information about potential market growth, 
    actual market size, future market size, and investment growth for the relevant markets 
    associated with the given topic. It utilizes the `get_market` function to identify 
    relevant markets and constructs prompts based on those markets.

    Args:
        topic (str): The topic for which to generate prompts.
        domain (str): The domain context for which prompts are generated.

    Returns:
        List[Tuple[str, str]]: A list of tuples, each containing a prompt string and its corresponding 
                               description.
    """
    # Get the relevant markets for the given topic
    markets = get_market(topic)

    # Define prompt templates
    prompts_templates = {
        "Potential Market Growth": f"Worldwide Potential Market Growth in these markets {markets} in {domain} in this topic : ",
        "Actual Market Size": f"Actual Market Size in these markets {markets} in {domain} in this topic : ",
        "Future Market Size": f"Future Market Size in these markets {markets} in {domain} in this topic : ",
        "Actual Investment": f"Actual Investment in these markets {markets} in {domain} in this topic : ",
        # "Investment Growth": f"Actual percentage of investment growth in these markets {markets} in {domain} in this topic : ",
    }

    # Convert the prompts dictionary into a list of tuples with their descriptions
    prompts = [(prompt, description) for description, prompt in prompts_templates.items()]

    return prompts


def search(API_KEY: str, QUERIES: List[str], domain: str) -> pd.DataFrame:
    """
    Fetches search results for each query using SerpAPI and processes them into a structured DataFrame.

    Args:
        API_KEY (str): The API key for accessing SerpAPI.
        QUERIES (List[str]): A list of search queries to process.
        domain (str): The domain for contextualizing the search prompts.

    Returns:
        pd.DataFrame: A DataFrame containing processed search results with Topic, URL, and Prompt information.
    """
    results_list = []

    print("Starting to process queries...")

    # Loop through each query and corresponding prompts, fetching search results
    for query in tqdm(QUERIES, desc="Processing queries"):
        prompts = get_prompts(query, domain)
        for prompt, prompt_name in prompts:
            print(f"Fetching search results for query: {query} with prompt: {prompt_name}")
            results = fetch_search_results(prompt + query, API_KEY)
            print(f"Fetched {len(results)} results for query: {query} with prompt: {prompt_name}")

            # Convert results to DataFrame and clean it
            dataframe = pd.DataFrame(results)
            print(f"DataFrame created with {len(dataframe)} rows.")

            # Drop duplicate URLs
            dataframe.drop_duplicates(subset=['url'], inplace=True)
            print(f"DataFrame now has {len(dataframe)} rows after dropping duplicates.")

            # Append results with necessary fields
            results_list.extend(
                [{'Topic': query, 'URL': row['url'], 'Prompt': prompt_name} for index, row in dataframe.iterrows()]
            )

    # Create a final DataFrame with the aggregated results
    final_dataframe = pd.DataFrame(results_list)
    print(f"Final DataFrame created with {len(final_dataframe)} rows.")

    return final_dataframe


def run_multiple_configs(OPENAI_API_KEY, topic, df, n):
    # Define a function to create prompts based on the given type and topic
    def create_prompt(prompt_type, topic):
        prompt_templates = {
            "Market Growth": f"""
            As a journalist with expertise in market analytics. Analyse the content and provide these informations:

            **Potential Market Growth in {topic}:**
            - The Estimated potential market growth percentage. e.g., "Potential Market Growth in 2022": "9.9% CAGR"
            - Give a short Description (sentence from the text where the potential market growth percentage is mentioned.)
            """,
            "Actual Market Size": f"""
            As a journalist with expertise in market analytics, analyze the content and provide the following information:

            **Market Size (Actual Market Size) in {topic}:**
            - Estimated Market Size. e.g., "USD 196.20 billion".
            - Give a short Description (sentence from the text where the Current market size is mentioned.)
            - Don't put the "CAGR" in the description
            """,
            "Future Market Size": f"""
            As a journalist with expertise in market analytics, analyze the content and provide the following information:

            **Future Market Size in {topic} for the next coming years:**
            - Future Estimated market size . e.g., "$100 billion".
            - Give a short Description (sentence from the Estimated market size is mentioned.)
            - Don't put the "CAGR" in the description
            """,
            "Actual Investment": f"""
            As a journalist with expertise in market analytics, analyze the content and provide the following information:

            **Actual Investment in {topic}:**
            **Actual percentage of investment growth in 2023, 2022, and 2024 in {topic}:**
            - Actual Amount of investment in the {topic}. e.g., "USD 16.3 billion"
            - Actual percentage of investment growth in the {topic}. e.g., "12% in 2022, 13.1% in 2023, 14.1% in 2024" (Don't take the values of these examples)
            - Give a short Description (sentence from the Actual Amount of investment is mentioned.)
            - Don't put the "CAGR" in the description
            """,
            "Investment Growth": f"""
            As a journalist with expertise in market analytics, analyze the content and provide the following information:

            **Actual percentage of investment growth in {topic}:**
            - Actual percentage of investment growth in the {topic}. e.g., "12% in 2023"
            - Give a short Description (sentence where the Actual percentage of investment growth is mentioned.)
            - Don't put the "CAGR" in the description
            """
        }
        return prompt_templates[prompt_type]

    # Configurations for each prompt type
    prompt_configs = [
        {"type": "Market Growth", "results_field": "Potential Market Growth"},
        {"type": "Actual Market Size", "results_field": "Actual Market Size"},
        {"type": "Future Market Size", "results_field": "Future Market Size"},
        {"type": "Actual Investment", "results_field": "Actual Investment"},
        {"type": "Investment Growth", "results_field": "Investment Growth"}
    ]

    # Run the smart scraper based on the provided configurations
    results = []
    for config in prompt_configs:
        prompt = create_prompt(config["type"], topic)
        filtered_df = df[(df.Topic == topic) & (df.Prompt == config["results_field"])].head(n)
        result = run_smart_scraper(
            {config["type"]: prompt},
            filtered_df,
            config["type"] + ' in ' + topic,
            topic,
            OPENAI_API_KEY
        )
        print(result)
        results.extend(result)
    return results


from typing import Dict, Union, List, Any

# Define types for better readability and type checking
InsightDict = Dict[str, Union[str, Dict[str, Any]]]
InputData = List[Dict[str, Union[str, InsightDict]]]
OutputData = Dict[str, List[Dict[str, Any]]]


from typing import List, Dict, Any

def transform_market_insights_data(data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Transform the input data into a structured format with topics and insights.

    Args:
        data (List[Dict[str, Any]]): A list of dictionaries containing 'topic', 'result', and 'url'.

    Returns:
        Dict[str, List[Dict[str, Any]]]: A dictionary with topics as keys, each containing a list of insights.
    """
    output: Dict[str, List[Dict[str, Any]]] = {}

    for item in data:
        topic: str = item['topic']
        insight: Dict[str, Any] = item['result']
        url: str = item['url']

        # Flattening and formatting the insight data correctly
        flattened_insight: Dict[str, Any] = {}

        for key, value in insight.items():
            if isinstance(value, dict):
                flattened_insight.update(value)
            else:
                flattened_insight[key] = value

        # Adding the 'Source' and 'Insight_category' fields
        flattened_insight["Source"] = url
        flattened_insight["Insight_category"] = "Market"

        # Adding insights to the corresponding topic
        if topic not in output:
            output[topic] = []

        output[topic].append(flattened_insight)

    return output

from typing import List, Dict, Optional, Any, Union, Tuple
import json
import re
import nest_asyncio
import pandas as pd
from tqdm import tqdm
import requests
import justext
from PyPDF2 import PdfReader
from io import BytesIO
from serpapi import GoogleSearch
import serpapi
from scrapegraphai.graphs import SmartScraperGraph
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import text_generation

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

# Define type aliases for better readability and type checking
PromptDict = Dict[str, str]
ResultDict = Dict[str, Any]
InsightDict = Dict[str, Union[str, Dict[str, Any]]]
InputData = List[Dict[str, Union[str, InsightDict]]]
OutputData = Dict[str, List[Dict[str, Any]]]


def fetch_search_results(query: str, api_key: str, time_range: Optional[str] = None) -> List[Dict[str, Optional[str]]]:
    """
    Fetches search results for a given query using SerpAPI with an optional time range filter.

    Parameters:
        query (str): The search query.
        api_key (str): The API key for SerpAPI.
        time_range (Optional[str]): The time range for search results (e.g., 'hour', 'day', 'week', 'month', 'year').

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
    #search = serpapi.search(search_params)
    search = GoogleSearch(search_params)
    result = search.get_dict()

    for item in result.get("organic_results", []):
        formatted_item = {
            "title": item.get("title"),
            "date": item.get("date"),
            "url": item.get("link"),
            "tags_matched": item.get("snippet_highlighted_words"),
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


def run_smart_scraper(
    prompts: PromptDict,
    df: pd.DataFrame,
    field: str,
    topic: str,
    OPENAI_API_KEY: str
) -> List[ResultDict]:
    """
    Run the SmartScraperGraph on a list of URLs with specified prompts.

    Args:
        prompts (PromptDict): Dictionary with prompt names as keys and prompt texts as values.
        df (pd.DataFrame): DataFrame containing URLs to be scraped. Must contain a 'URL' column.
        field (str): Specific field to validate in the scraping results.
        topic (str): Topic associated with the scraping.
        OPENAI_API_KEY (str): OpenAI API key for accessing the models.

    Returns:
        List[ResultDict]: A list containing the first valid result found.
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
            print(result)

            # Save the last attempted result
            last_result = {
                "result": result,
                "topic": topic,
                "url": url,
                "analysis_type": prompt_name
            }

            # Validation: Check all dictionaries in the result
            valid_result = False

            for key, main_field_data in result.items():

                if isinstance(main_field_data, dict) and main_field_data:
                    # Check if all values in main_field_data are not empty or 'NA'
                    if all(
                        value and str(value).strip().lower() not in ['na', '', 'no amount found']
                        for value in main_field_data.values()
                    ):
                        valid_result = True
                        break  # Valid result found

            # Return the result if valid
            if valid_result:
                print(f"Valid result found for URL: {url}")
                return last_result
            else:
                print(f"No valid data found in URL: {url}")
        except Exception as e:
            print(f"Error occurred while processing URL: {url}, Error: {e}")
        return None

    # Process URLs sequentially
    for prompt_name, prompt in prompts.items():
        for url in urls:
            result = scrape_url(url, prompt_name, prompt)
            if result:
                all_results.append(result)
                # Stop processing further URLs
                return all_results

    # If no valid results were found, append the last attempted result
    if not all_results and last_result is not None:
        all_results.append(last_result)
        print(f"No valid results found. Taking the last result as the final output: {last_result['url']}")

    return all_results


def get_market(topic: str) -> List[str]:
    """
    Identify relevant markets associated with a given topic.

    This function takes a topic as input and generates a list of relevant markets related to the topic.
    The function uses a language model to identify markets based on business sectors, industries, types of 
    products or services involved, and any economic trends related to the topic.

    Args:
        topic (str): The topic for which to identify relevant markets.

    Returns:
        List[str]: A list of relevant market names related to the topic.

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
        List[Tuple[str, str]]: A list of tuples, each containing the description and the prompt string.
    """
    # Get the relevant markets for the given topic
    markets = get_market(topic)

    # Define prompt templates
    prompts_templates = {
        "Potential Market Growth": f"Worldwide Potential Market Growth in these markets {markets} in the industry of {domain} in this topic: ",
        "Actual Market Size": f"Actual Market Size in these markets {markets} in the industry of {domain} in this topic: ",
        "Future Market Size": f"Future Market Size in these markets {markets} in the industry of {domain} in this topic: ",
        "Actual Investment": f"Actual Investment in these markets {markets} in the industry of {domain} in this topic: ",
        "Investment Growth": f"Actual percentage of investment growth in these markets {markets} in {domain} in this topic: ",
    }

    # Convert the prompts dictionary into a list of tuples with their descriptions
    prompts = [(description, prompt) for description, prompt in prompts_templates.items()]

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
        for prompt_name, prompt in prompts:
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


def run_multiple_configs(
    OPENAI_API_KEY: str,
    topic: str,
    df: pd.DataFrame,
    n: int
) -> List[ResultDict]:
    """
    Runs the smart scraper with multiple configurations based on different prompt types.

    Args:
        OPENAI_API_KEY (str): The OpenAI API key for accessing language models.
        topic (str): The topic for which to run the scraper.
        df (pd.DataFrame): DataFrame containing URLs to be scraped.
        n (int): The number of URLs to process for each prompt type.

    Returns:
        List[ResultDict]: A list of result dictionaries obtained from the scraper.
    """
    # Define a function to create prompts based on the given type and topic
    def create_prompt(prompt_type: str, topic: str) -> str:
        prompt_templates = {
            "Market Growth": f"""
As a journalist with expertise in market analytics, analyze the content and provide the following information:

**Potential Market Growth in {topic}:**
- The estimated potential market growth percentage. e.g., "Potential Market Growth in 2022": "9.9% CAGR"
- Give a short description (sentence from the text where the potential market growth percentage is mentioned.)
""",
            "Actual Market Size": f"""
As a journalist with expertise in market analytics, analyze the content and provide the following information:

**Market Size (Actual Market Size) in {topic}:**
- Estimated market size. e.g., "USD 196.20 billion".
- Give a short description (sentence from the text where the current market size is mentioned.)
- Don't include "CAGR" in the description.
""",
            "Future Market Size": f"""
As a journalist with expertise in market analytics, analyze the content and provide the following information:

**Future Market Size in {topic} for the next coming years:**
- Future estimated market size. e.g., "$100 billion".
- Give a short description (sentence where the estimated market size is mentioned.)
- Don't include "CAGR" in the description.
""",
            "Actual Investment": f"""
As a journalist with expertise in market analytics, analyze the content and provide the following information:

**Actual Investment in {topic}:**
- Actual amount of investment in {topic}. e.g., "USD 16.3 billion"
- Actual percentage of investment growth in {topic}. e.g., "12% in 2022, 13.1% in 2023, 14.1% in 2024"
- Give a short description (sentence where the actual amount of investment is mentioned.)
- Don't include "CAGR" in the description.
""",
            "Investment Growth": f"""
As a journalist with expertise in market analytics, analyze the content and provide the following information:

**Actual percentage of investment growth in {topic}:**
- Actual percentage of investment growth in {topic}. e.g., "12% in 2023"
- Give a short description (sentence where the actual percentage of investment growth is mentioned.)
- Don't include "CAGR" in the description.
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
    results: List[ResultDict] = []
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


def transform_market_insights_data(data: InputData) -> OutputData:
    """
    Transform the input data into a structured format with topics and insights.

    Args:
        data (InputData): A list of dictionaries containing 'topic', 'result', and 'url'.

    Returns:
        OutputData: A dictionary with a list of topics, each containing insights.
    """
    output: OutputData = {"TOPICS": []}
    topics: Dict[str, Dict[str, Any]] = {}

    for item in data:
        topic: str = item['topic']
        insight: InsightDict = item['result']
        url: str = item['url']

        # Flattening nested dictionaries and ensuring the desired order of fields
        flattened_insight: Dict[str, Any] = {}

        # Flattening and formatting the insight data correctly
        for key, value in insight.items():
            if isinstance(value, dict):
                flattened_insight.update(value)
            else:
                flattened_insight[key] = value

        # Adding the 'url' and 'insight_category' at the end
        flattened_insight["url"] = url
        flattened_insight["insight_category"] = "Market"

        # Organizing topics and insights
        if topic not in topics:
            topics[topic] = {"topic": topic, "insights": []}

        topics[topic]["insights"].append(flattened_insight)

    # Convert the topics dictionary to a list of values
    output["TOPICS"] = list(topics.values())
    return output

import streamlit as st
import json
import os
from urllib.parse import urlparse
import polars as pl
from time import sleep  # For simulating progression
from open_ai_market_insigth import (
    fetch_search_results, 
    process_scraping, 
    run_smart_scraper, 
    transform_market_insights_data, 
    run_multiple_configs, 
    search
)

# Load data from external JSON file
with open("kraft_market_insigths.json") as f:
    data = json.load(f)

# Inject custom CSS for styling the header, sidebar, and links
st.markdown(
    """
    <style>
    .header {
        margin-top: 20px;
        display: flex;
        align-items: center;
        gap: 10px;
    }
    .header h1 {
        color: #1f77b4;  /* Blue color */
        font-size: 2.5rem;
        font-weight: bold;
    }
    .tabs {
        display: flex;
        gap: 30px;
        margin-top: 10px;
        font-size: 1.2rem;
    }
    .tabs a {
        color: #6c757d;
        text-decoration: none;
    }
    .tabs a.active {
        color: #1f77b4;
        font-weight: bold;
        border-bottom: 2px solid #1f77b4;
    }
    .blue-text {
        color: #1f77b4;
        font-weight: bold;
        font-size: 1.5rem;
    }
    .card {
        border: 1px solid #ddd;
        border-radius: 10px;
        padding: 15px;
        margin: 10px;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
    }
    .card:hover {
        box-shadow: 0 6px 16px rgba(0, 0, 0, 0.2);
    }
    .external-link {
        display: inline-block;
        color: #1f77b4;
        font-weight: bold;
        text-decoration: none;
        font-size: 1rem;
    }
    .external-link:hover {
        text-decoration: underline;
    }
    .external-link::after {
        content: ' \\2197';  /* Unicode for external link arrow icon */
    }
    </style>
    """, 
    unsafe_allow_html=True
)

# Function to extract domain and path from the URL
def format_source_url(url):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    path = parsed_url.path
    formatted_url = domain + path
    return formatted_url

# Sidebar with Navigation and Buttons (Reorganized)
st.sidebar.title("📊 Navigation")
page_selection = st.sidebar.radio("Go to", ["🏠 Home", "🤖 Get Insights", "📈 Market Insights"], index=0)

# Adding space to separate buttons
st.sidebar.markdown("---")

# Initialize session state to track if 'Get Insights' is clicked
if 'insights_in_progress' not in st.session_state:
    st.session_state['insights_in_progress'] = False

# Home Page Logic
if page_selection == "🏠 Home" and not st.session_state['insights_in_progress']:
    # Home page content
    st.title("Welcome to the Market Insights Dashboard")
    st.write(
        """
        This dashboard provides insights into various emerging markets, such as 3D printing, AI in recipes, and affordable nutrition.
        Use the sidebar to navigate between the Home page, Get Insights, and Market Insights to explore detailed data points.
        """
    )

# Get Insights Page Logic
elif page_selection == "🤖 Get Insights":
    # File uploader to upload the Excel file
    st.title("Upload the Insights File")
    uploaded_file = st.file_uploader("Please upload the `kraft_framework_market_insigths_generation.xlsx` file", type=["xlsx"])

    # Only run the insights process if a file has been uploaded
    if uploaded_file is not None:
        # Set the flag for 'insights_in_progress' to True
        st.session_state['insights_in_progress'] = True

        # "Get Insights" section content
        st.title("Fetching New Market Insights")

        # Add a progress bar (circle style)
        progress_bar = st.progress(0)  # Initialize the progress bar at 0%

        st.write("Running the scraper and processing the data...")

        # Simulating a step-by-step progress (replace this with actual function calls)
        steps = 5  # Number of steps (for simulation)
        for step in range(steps):
            # Simulate a delay for each step (this is where real processes would happen)
            sleep(1)  # Simulate some work being done
            progress_bar.progress((step + 1) / steps)  # Update progress bar

        # Process the uploaded Excel file using Polars
        dataframe = pl.read_excel(uploaded_file)

        # Your OpenAI API Key
        OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
        
        # SerpAPI Key
        API_KEY = os.environ.get("API_KEY") 

        # User input for queries as a list
        QUERIES = list(dataframe["Topic"])[:3]

        # Fetching search results for all queries
        texts_df = search(API_KEY, QUERIES, "Beauty")  # Modify as needed for specific context

        # User input for unique topics as a list
        UNIQUE_TOPICS = list(dataframe["Topic"])[:3]

        # Loop through each unique topic and run configurations
        results = []
        for unique_topic in UNIQUE_TOPICS:
            # Running multiple configurations with OpenAI API Key, unique topic, and fetched data
            tab = run_multiple_configs(OPENAI_API_KEY, unique_topic, texts_df, 50)

            # Transforming and storing market insights data
            transformed_data = transform_market_insights_data(tab)
            results.append({unique_topic: transformed_data})

        # Display the results
        st.write(json.dumps(results, indent=4))

        # Notify user that the task is complete
        st.success("Market insights generation complete!")

        # Reset the 'insights_in_progress' flag to False once done
        st.session_state['insights_in_progress'] = False
    else:
        st.write("Please upload the `kraft_framework_market_insigths_generation.xlsx` file to proceed.")

# Market Insights Page Logic
elif page_selection == "📈 Market Insights" and not st.session_state['insights_in_progress']:
    # Save selected topic to session state to prevent disappearing when re-rendering
    if 'selected_topic' not in st.session_state:
        st.session_state['selected_topic'] = None

    # Top left corner header with title and tabs (removed extra tabs)
    st.markdown(
        """
        <div class="header">
            <h1>Get insights from your data</h1>
        </div>
        """, 
        unsafe_allow_html=True
    )

    # Streamlit app code for visualizing data points by topic selection
    st.subheader("Select a topic to explore the latest market insights:")

    # Select a topic, this will be saved in the session state to keep the selection persistent
    topics = list(data.keys())
    selected_topic = st.selectbox("Pick a topic", topics, index=0 if st.session_state['selected_topic'] is None else topics.index(st.session_state['selected_topic']), help="Select a market segment to see detailed insights")

    # Update session state with the newly selected topic
    st.session_state['selected_topic'] = selected_topic

    # Display the insights for the selected topic
    st.header(f"🔍 Insights for **{selected_topic}**")

    # Get the nested data for the selected topic
    insights = data[selected_topic][selected_topic]

    # Display insights in columns, creating a card-like layout with custom blue text for important values
    cols_per_row = 2  # You can adjust this based on screen size preference

    for i in range(0, len(insights), cols_per_row):
        cols = st.columns(cols_per_row)
        for col, insight in zip(cols, insights[i:i + cols_per_row]):
            # Check for NA values and skip if present
            if 'NA' in insight.values():
                continue
            
            with col:
                # Card style for each data point
                st.markdown('<div class="card">', unsafe_allow_html=True)
                
                # Display the title (either 'Estimated potential market growth percentage', 'Estimated Market Size', etc.)
                title = insight.get('Estimated potential market growth percentage', 
                                    insight.get('Estimated Market Size', 
                                                insight.get('Future Estimated market size', 
                                                            insight.get('Actual Amount of investment in the 3D Printed at Home', ''))))
                st.markdown(f'<p class="blue-text">{title}</p>', unsafe_allow_html=True)
                
                # Display description
                description = insight.get('Description', '')
                st.write(description)
                
                # Extract and display formatted URL as a clickable text
                source_url = insight["Source"]
                formatted_url = format_source_url(source_url)
                st.markdown(f'<a href="{source_url}" class="external-link" target="_blank">{formatted_url}</a>', unsafe_allow_html=True)
                
                # End of card
                st.markdown('</div>', unsafe_allow_html=True)

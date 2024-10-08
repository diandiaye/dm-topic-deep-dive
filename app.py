import streamlit as st
import json
from time import sleep  # For simulating progression
import polars as pl
from provocations import generate_provocations  # Import the provocations function
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

# Initialize session state variables if not already set
if 'insights_in_progress' not in st.session_state:
    st.session_state['insights_in_progress'] = False
if 'selected_topic' not in st.session_state:
    st.session_state['selected_topic'] = None

# Add a logo to the top left corner
st.image("logo-dm.png", width=150)

# Inject custom CSS for styling
st.markdown(
    """
    <style>
    .header { margin-top: 20px; display: flex; align-items: center; gap: 10px; }
    .header h1 { color: #1f77b4; font-size: 2.5rem; font-weight: bold; }
    .tabs { display: flex; gap: 30px; margin-top: 10px; font-size: 1.2rem; }
    .tabs a { color: #6c757d; text-decoration: none; }
    .tabs a.active { color: #1f77b4; font-weight: bold; border-bottom: 2px solid #1f77b4; }
    .blue-text { color: #1f77b4; font-weight: bold; font-size: 1.5rem; }
    .card { border: 1px solid #ddd; border-radius: 10px; padding: 15px; margin: 10px; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1); }
    .card:hover { box-shadow: 0 6px 16px rgba(0, 0, 0, 0.2); }
    .external-link { display: inline-block; color: #1f77b4; font-weight: bold; text-decoration: none; font-size: 1rem; }
    .external-link:hover { text-decoration: underline; }
    .external-link::after { content: ' \\2197'; }
    .provocation-card {
        background-color: #f9f9f9;
        border: 1px solid #ddd;
        border-radius: 8px;
        padding: 20px;
        margin-bottom: 15px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .provocation-title {
        color: #1f77b4;
        font-weight: bold;
        font-size: 1.25rem;
    }
    .provocation-content {
        font-size: 1rem;
        color: #333;
        margin-top: 10px;
    }
    .provocation-theme {
        font-size: 0.9rem;
        font-weight: bold;
        color: #555;
        margin-top: 5px;
    }
    </style>
    """, 
    unsafe_allow_html=True
)

# Sidebar with Navigation and Buttons (Reorganized)
st.sidebar.title("📊 Navigation")
page_selection = st.sidebar.radio("Go to", ["🏠 Home", "🤖 Get Insights", "📈 Market Insights", "💡 Provocations"], index=0)

# Home Page Logic
if page_selection == "🏠 Home":
    st.title("Welcome to the Market Insights Dashboard")
    st.write(
        """
        This dashboard provides insights into various emerging markets, such as 3D printing, AI in recipes, and affordable nutrition.
        Use the sidebar to navigate between the Home page, Get Insights, and Market Insights to explore detailed data points.
        """
    )

# Get Insights Page Logic
elif page_selection == "🤖 Get Insights":
    st.title("Upload the Topic framework File")
    uploaded_file = st.file_uploader("Please upload an Excel file containing a 'Topic' column", type=["xlsx"])

    # Only run the insights process if a file has been uploaded
    if uploaded_file is not None:
        # Read the Excel file using Polars
        try:
            dataframe = pl.read_excel(uploaded_file)
            
            # Check if 'Topic' column exists
            if 'Topic' not in dataframe.columns:
                st.error("The uploaded file does not contain a 'Topic' column. Please upload a valid file.")
            else:
                topic_list = list(dataframe["Topic"])

                # Allow the user to select specific topics or choose all
                selected_topics = st.multiselect("Select topics to run insights on", topic_list)

                # Only proceed if at least one topic is selected
                if len(selected_topics) > 0:
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

                    OPENAI_API_KEY = "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"
                    
                    # SerpAPI Key
                    API_KEY = "82fc16c43b8c8d9e2cad8e06a7927bd2d32c9fc9194c89b2a55057ebc4162ad1"

                    # Fetching search results for the selected topics
                    texts_df = search(API_KEY, selected_topics, "Food")  # Modify as needed for specific context

                    # Loop through each selected topic and run configurations
                    results = []
                    for unique_topic in selected_topics:
                        # Running multiple configurations with OpenAI API Key, unique topic, and fetched data
                        tab = run_multiple_configs(OPENAI_API_KEY, unique_topic, texts_df, 50)

                        # Transforming and storing market insights data
                        transformed_data = transform_market_insights_data(tab)
                        results.append({unique_topic: transformed_data})

                    # Save the results to a JSON file named with the first letter of the uploaded Excel file + "_generated_market_insights.json"
                    first_letter = uploaded_file.name[0]  # Get the first letter of the uploaded file's name
                    output_filename = f"{first_letter}_generated_market_insights.json"
                    
                    with open(output_filename, "w") as output_file:
                        json.dump(results, output_file, indent=4)
                    
                    # Display the results in a more beautiful way
                    st.header("📝 Generated Market Insights")
                    for result in results:
                        topic_name = list(result.keys())[0]
                        insights_data = result[topic_name]
                        
                        st.markdown(f"### **Topic: {topic_name}**")
                        for insight in insights_data:
                            st.markdown(
                                f"""
                                <div class="card">
                                    <p class="blue-text">Estimated Growth: {insight.get('Estimated potential market growth percentage', 'N/A')}</p>
                                    <p>Market Size: {insight.get('Estimated Market Size', 'N/A')}</p>
                                    <p>Future Market Size: {insight.get('Future Estimated market size', 'N/A')}</p>
                                    <p>Description: {insight.get('Description', 'N/A')}</p>
                                    <a href="{insight.get('Source', '#')}" class="external-link" target="_blank">Source</a>
                                </div>
                                """, 
                                unsafe_allow_html=True
                            )
                    
                    # Notify user that the output file is saved
                    st.success(f"Market insights generation complete! File saved as `{output_filename}`.")

                    # Reset the 'insights_in_progress' flag to False once done
                    st.session_state['insights_in_progress'] = False
                else:
                    st.write("Please select at least one topic to proceed.")
        except Exception as e:
            st.error(f"Error reading the file: {e}")
    else:
        st.write("Please upload an Excel file to proceed.")

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
                st.markdown(f'<a href="{source_url}" class="external-link" target="_blank">{source_url}</a>', unsafe_allow_html=True)
                
                # End of card
                st.markdown('</div>', unsafe_allow_html=True)

# Provocations Page Logic
elif page_selection == "💡 Provocations":
    st.title("Generate Provocations")

    # File uploader to upload both Excel files at once
    uploaded_files = st.file_uploader("Please upload two Excel files (1 for Topic Description and 1 for Topic Keywords)", type=["xlsx"], accept_multiple_files=True)

    if len(uploaded_files) == 2:
        try:
            # Identify which file corresponds to which data by checking the content of the columns
            topic_description_file = None
            topic_keywords_file = None

            # Loop through the uploaded files and assign them based on column names
            # Loop through the uploaded files and assign them based on column names
            for file in uploaded_files:
                df = pl.read_excel(file)
                if 'Description' in df.columns:
                    topic_description_file = df
                elif 'keyword' in df.columns:
                    topic_keywords_file = df
            # Check if both files are correctly assigned
            if topic_description_file is not None and topic_keywords_file is not None:
                # Allow the user to select a topic
                topics = topic_description_file["Topic"].unique()
                selected_topic = st.selectbox("Choose a topic to generate provocations for:", topics)

                if selected_topic:
                    # Filter the description and keywords for the selected topic
                    selected_topic_description = topic_description_file.filter(pl.col("Topic") == selected_topic)
                    selected_topic_keywords = topic_keywords_file.filter(pl.col("Topic") == selected_topic)

                    # Call the `generate_provocations` function for the selected topic
                    st.write(f"Generating provocations for topic: **{selected_topic}**")
                    provocations_data = generate_provocations(selected_topic_description, selected_topic_keywords)
                    st.write("provocations_data")
                    st.write(provocations_data)

                    # Display the provocations in a beautiful way
                    st.header(f"🧠 Generated Provocations for {selected_topic}")

                    for provocation in provocations_data:
                        st.markdown(
                            f"""
                            <div class="provocation-card">
                                <div class="provocation-theme">Theme: {provocation["Attributed Themes"]}</div>
                                <div class="provocation-title">Topic: {provocation["Topic"]}</div>
                                <div class="provocation-content">{provocation["Provocations"][0]}</div>
                                <div class="provocation-content">{provocation["Provocations"][1]}</div>
                            </div>
                            """, 
                            unsafe_allow_html=True
                        )

                    # Display a success message
                    st.success(f"Provocations generated and displayed successfully for {selected_topic}.")
            else:
                st.error("Please upload files with the correct columns: 'Topics', 'Description' for topic description and 'Topics', 'Keyword' for topic keywords.")
        except Exception as e:
            st.error(f"Error processing the files: {e}")
    else:
        st.write("Please upload both Excel files at the same time to proceed.")

import streamlit as st
import json
import time  # For simulating the progress bar
import polars as pl
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

# Load provocations data from JSON file (assumed to be a list of dictionaries)
with open("DATA/Kraft_topics_provocations.json") as prov_file:
    provocations_data = json.load(prov_file)

# Load Topic Evolution data from JSON file
with open("DATA/Kraft_topic_evolution.json") as evo_file:
    topic_evolution_data = json.load(evo_file)

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

    .provocations-container {
        display: flex;
        flex-direction: column;
        gap: 20px; /* Space between the provocations */
        margin-top: 20px;
    }
    .provocation-box {
        background-color: #6C63FF;
        color: white;
        padding: 20px;
        border-radius: 10px;
        text-align: left;
    }
    .provocation-box h3 {
        font-size: 1.2rem;
        font-weight: bold;
        margin-bottom: 10px;
        color: rgba(255, 255, 255, 0.8);
    }
    .provocation-box p {
        font-size: 1rem;
    }

    /* Timeline styles */
    .timeline-container {
        display: flex;
        justify-content: space-between;
        margin-top: 20px;
        margin-bottom: 20px;
    }
    .timeline-button {
        padding: 10px;
        border-radius: 5px;
        border: 1px solid #ccc;
        background-color: white;
        cursor: pointer;
        text-align: center;
    }
    .timeline-button:hover {
        background-color: #f1f1f1;
    }
    .timeline-button.active {
        border: 2px solid #1f77b4;
        color: #1f77b4;
        font-weight: bold;
    }
    .evolution-text {
        border: 1px solid #1f77b4;
        background-color: #f9f9f9;
        padding: 15px;
        border-radius: 5px;
        font-size: 1rem;
        color: #333;
        margin-top: 10px;
    }
    </style>
    """, 
    unsafe_allow_html=True
)

# Sidebar with Navigation and Buttons (Reorganized)
st.sidebar.title("📊 Navigation")
page_selection = st.sidebar.radio("Go to", ["🏠 Home", "📈 Market Insights", "💡 Topic Provocations", "📉 Topic Evolution"], index=0)

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

    if uploaded_file is not None:
        try:
            dataframe = pl.read_excel(uploaded_file)
            
            if 'Topic' not in dataframe.columns:
                st.error("The uploaded file does not contain a 'Topic' column. Please upload a valid file.")
            else:
                topic_list = list(dataframe["Topic"])

                selected_topics = st.multiselect("Select topics to run insights on", topic_list)

                if len(selected_topics) > 0:
                    st.session_state['insights_in_progress'] = True
                    st.title("Fetching New Market Insights")
                    progress_bar = st.progress(0)
                    st.write("Running the scraper and processing the data...")

                    steps = 5
                    for step in range(steps):
                        time.sleep(1)
                        progress_bar.progress((step + 1) / steps)

                    OPENAI_API_KEY = ""
                    API_KEY = ""
                    
                    texts_df = search(API_KEY, selected_topics, "Food")

                    results = []
                    for unique_topic in selected_topics:
                        tab = run_multiple_configs(OPENAI_API_KEY, unique_topic, texts_df, 50)
                        transformed_data = transform_market_insights_data(tab)
                        results.append({unique_topic: transformed_data})

                    first_letter = uploaded_file.name[0]
                    output_filename = f"{first_letter}_generated_market_insights.json"
                    
                    with open(output_filename, "w") as output_file:
                        json.dump(results, output_file, indent=4)
                    
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
                    
                    st.success(f"Market insights generation complete! File saved as `{output_filename}`.")
                    st.session_state['insights_in_progress'] = False
                else:
                    st.write("Please select at least one topic to proceed.")
        except Exception as e:
            st.error(f"Error reading the file: {e}")
    else:
        st.write("Please upload an Excel file to proceed.")

# Market Insights Page Logic
elif page_selection == "📈 Market Insights" and not st.session_state['insights_in_progress']:
    st.title("Market Insights")

    # No default topic selected
    st.subheader("Select a topic to explore the latest market insights:")

    topics = list(data.keys())
    selected_topic = st.selectbox("Pick a topic", topics, index=None)  # No default selection

    if selected_topic:
        # Show progress bar for 3 seconds
        with st.spinner("Looking for interesting insights, please wait..."):
            time.sleep(3)  # Simulate delay

        st.header(f"🔍 Insights for **{selected_topic}**")
        insights = data[selected_topic][selected_topic]

        cols_per_row = 2

        for i in range(0, len(insights), cols_per_row):
            cols = st.columns(cols_per_row)
            for col, insight in zip(cols, insights[i:i + cols_per_row]):
                if 'NA' in insight.values():
                    continue

                with col:
                    st.markdown('<div class="card">', unsafe_allow_html=True)

                    title = insight.get('Estimated potential market growth percentage', 
                                        insight.get('Estimated Market Size', 
                                                    insight.get('Future Estimated market size', 
                                                                insight.get('Actual Amount of investment in the 3D Printed at Home', ''))))
                    st.markdown(f'<p class="blue-text">{title}</p>', unsafe_allow_html=True)

                    description = insight.get('Description', '')
                    st.write(description)

                    source_url = insight["Source"]
                    st.markdown(f'<a href="{source_url}" class="external-link" target="_blank">{source_url}</a>', unsafe_allow_html=True)

                    st.markdown('</div>', unsafe_allow_html=True)

# Provocations Page Logic
elif page_selection == "💡 Provocations":
    st.write("Welcome to the D&M Provocations generation page. Our algorithms make it possible to generate a provocative and forward thinking according to a given topic.")

    st.subheader("Select a topic to generate provocations:")

    # Extract the list of unique topics from the provocations JSON
    topics = [item["Topic"] for item in provocations_data]
    selected_topic = st.selectbox("Pick a topic", topics, index=None)  # No default selection

    if selected_topic:
        # Show progress bar
        with st.spinner("Generating provocations, please wait..."):
            time.sleep(4)  # Simulate delay of 4 seconds

        # Find the provocations associated with the selected topic
        selected_provocations = [p for p in provocations_data if p["Topic"] == selected_topic]

        # Display provocations in vertical boxes, separated by a space
        st.markdown('<div class="provocations-container">', unsafe_allow_html=True)
        for provocation in selected_provocations:
            st.markdown(
                f"""
                <div class="provocation-box">
                    <h3>Provocations...</h3>
                    <p>{provocation.get("Provocations")[0]}</p>
                    <p>{provocation.get("Provocations")[1]}</p>
                    <p>{provocation.get("Provocations")[2]}</p>  <!-- Displaying the third provocation -->
                </div>
                """, 
                unsafe_allow_html=True
            )
        st.markdown('</div>', unsafe_allow_html=True)

# Topic Evolution Page Logic
elif page_selection == "📉 Topic Evolution":
   ## st.title("Topic Evolution")
    st.write("Welcome to the Topic Evolution page. Our algorithms make it possible to explore the evolution and key events influencing the growth of a given topic over time.")

    st.subheader("Select a topic to explore its evolution over time:")

    # Extract the list of unique topics from the topic evolution JSON
    topics = list(topic_evolution_data.keys())
    selected_topic = st.selectbox("Select a topic", topics, index=None)  # No default selection

    if selected_topic:
        # Show progress bar for 4 seconds
        with st.spinner("Generating topic evolution, please wait..."):
            time.sleep(4)  # Simulate delay

        # Retrieve timeline data for the selected topic
        timeline_data = topic_evolution_data[selected_topic]

        # Sort the years to display in chronological order
        sorted_years = sorted(timeline_data.keys())

        # Select a year to display the corresponding content
        selected_year = st.selectbox("Select a year", sorted_years)

        # Display the corresponding content for the selected year
        st.markdown(f"<div class='evolution-text'>{timeline_data[selected_year]}</div>", unsafe_allow_html=True)

        # Timeline display
        st.markdown('<div class="timeline-container">', unsafe_allow_html=True)
        for year in sorted_years:
            button_class = "timeline-button"
            if year == selected_year:
                button_class += " active"
            st.markdown(f"<div class='{button_class}'>{year}</div>", unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

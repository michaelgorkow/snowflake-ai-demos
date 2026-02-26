import streamlit as st
from snowflake.core import Root
from snowflake.snowpark.context import get_active_session

st.title(":material/search: Software Apps Search")

session = get_active_session()
root = Root(session)

search_service = (root
    .databases["AI_DEMOS"]
    .schemas["MASTER_DATA_MAPPING_DEMO"]
    .cortex_search_services["SOFTWARE_APPS_SEARCH"]
)

query = st.text_input("Search for software apps:", placeholder="Type and press Enter...")
min_score = st.slider("Minimum semantic similarity", 0, 100, 30, 5, format="%d%%")

if query:
    with st.spinner("Searching..."):
        response = search_service.search(
            query=query,
            columns=["SOFTWARE_NAME"],
            limit=10
        )
        results = response.results
    
    filtered_results = []
    for result in results:
        scores = result.get("@scores", {})
        semantic_score = scores.get("cosine_similarity", 0)
        if semantic_score >= min_score / 100:
            filtered_results.append(result)
    
    if filtered_results:
        st.caption(f"Showing {len(filtered_results)} of {len(results)} results for **{query}**")
        
        for i, result in enumerate(filtered_results):
            scores = result.get("@scores", {})
            name = result.get("SOFTWARE_NAME", "Unknown")
            
            text_score = scores.get("text_match", 0)
            semantic_score = scores.get("cosine_similarity", 0)
            avg_score = (text_score + semantic_score) / 2
            
            with st.container(border=True):
                col1, col2 = st.columns([2, 3])
                
                with col1:
                    st.subheader(f":material/apps: {name}")
                    st.badge(f"#{i + 1} Result", icon=":material/trophy:")
                
                with col2:
                    st.caption("Relevance Scores")
                    score_cols = st.columns(2)
                    with score_cols[0]:
                        st.metric("Text Match", f"{text_score:.1%}")
                    with score_cols[1]:
                        st.metric("Semantic", f"{semantic_score:.1%}")
                    
                    st.progress(avg_score, text=f"Overall: {avg_score:.1%}")
    else:
        st.info(f"No results above {min_score}% semantic similarity")
else:
    st.caption("Enter a search term to find software apps")
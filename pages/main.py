import os
import pandas as pd
import streamlit as st
from clarifai.client.dataset import Dataset
from clarifai.client.input import Inputs
from clarifai.client.auth.helper import ClarifaiAuthHelper
from clarifai.modules.css import ClarifaiStreamlitCSS
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
from google.protobuf.json_format import MessageToJson

from utils.functions import validate_databricks_config, set_global_pat
from utils.tab_contents import show_import_page, show_export_page, show_update_page

st.set_page_config(layout="wide")
ClarifaiStreamlitCSS.insert_default_css(st)

# This must be within the display() function.
auth = ClarifaiAuthHelper.from_streamlit(st)
query_params = st.experimental_get_query_params()
clarifai_pat=query_params.get("pat", [])[0]

if clarifai_pat:
    set_global_pat(clarifai_pat)

def show_tabs():    
    import_tab, export_tab, update_tab = st.tabs(["Import", "Export", "Update"])
    with import_tab:
        show_import_page(auth, st.session_state['dconfig'])
    with export_tab:
        show_export_page(auth, st.session_state['dconfig'])
    with update_tab:
        show_update_page(auth, st.session_state['dconfig'])

st.title("Databricks-Connect UI Module")

## Settings
if not 'dconfig' in st.session_state:
    st.markdown("<h3 style='margin-bottom: 1px;'>Configuration</h3>", unsafe_allow_html=True)
    st.markdown("Please provide the following information to connect to your Databricks Workspace.")
    #st.markdown("<hr style='margin: 5px 0;'>", unsafe_allow_html=True)

    with st.form(key='my_form'):
        c1, c2, c3 = st.columns([1,1,0.5])
        with c1:
            st.markdown("<div style='margin-bottom: 12px;'><strong>Databricks Host</strong></div>", unsafe_allow_html=True)
            st.markdown("Please enter the Databricks Host")

        with c2:
            dhost = st.text_input("-", key="2", label_visibility="hidden", type="password", placeholder="Example: https://dbc-a1b2345c-d6e7.cloud.databricks.com")

        st.markdown("<hr style='margin: 5px 0;'>", unsafe_allow_html=True)

        c1, c2, c3 = st.columns([1,1,0.5])
        with c1:
            st.markdown("<div style='margin-bottom: 12px;'><strong>Databricks Token</strong></div>", unsafe_allow_html=True)
            st.markdown("Please enter the Databricks Token")

        with c2:
            dtoken = st.text_input("-", key="4", label_visibility="hidden", type="password" , placeholder="Starts with dapib***********************")

        st.markdown("<hr style='margin: 5px 0;'>", unsafe_allow_html=True)

        c1, c2, c3 = st.columns([1,1,0.5])
        with c1:
            st.markdown("<div style='margin-bottom: 12px;'><strong>Databricks Cluster ID</strong></div>", unsafe_allow_html=True)
            st.markdown("Please enter the Databricks Cluster ID")

        with c2:
            dcluster = st.text_input("-", key="6", label_visibility="hidden", type="password", placeholder="1234-567890-abc12abc")

        #st.markdown("<hr style='margin: 5px 0;'>", unsafe_allow_html=True)
        submitted = st.form_submit_button('Authenticate')

    if submitted:
        with st.spinner("Authenticating to Databricks Workspace..."):
            status=validate_databricks_config(dhost, dtoken, dcluster)
    
    # st.write(f"dhost: {dhost}, dtoken: {dtoken}, dcluster: {dcluster}") 
    if submitted and status:
        st.session_state['dconfig'] = Config(
            host       = dhost,
            token      = dtoken,
            cluster_id = dcluster
            )
    
st.markdown("""
<style>
    .stTabs [data-baseweb="tab"] p {
        font-size: 24px;
        font-weight: bold;
    }
    .st-cd {
        gap: 3rem;
    }
</style>""", unsafe_allow_html=True)

if 'dconfig' in st.session_state:

    show_tabs()
    



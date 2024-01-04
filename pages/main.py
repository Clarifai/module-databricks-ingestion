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

from utils.tab_contents import show_import_page, show_export_page, show_update_page

st.set_page_config(layout="wide")
ClarifaiStreamlitCSS.insert_default_css(st)

# This must be within the display() function.
auth = ClarifaiAuthHelper.from_streamlit(st)
os.environ['CLARIFAI_PAT']=st.secrets.CLARIFAI_PAT

def show_tabs():    
    import_tab, export_tab, update_tab = st.tabs(["Import", "Export", "Update"])
    with import_tab:
        show_import_page(auth, st.session_state['dconfig'])
    with export_tab:
        show_export_page(auth, st.session_state['dconfig'])
    with update_tab:
        show_update_page(auth, st.session_state['dconfig'])

st.title("Databricks UI Module")

## Settings
if not 'dconfig' in st.session_state:
    st.header("Configuration")
    st.markdown("Please provide the following information to connect to your Databricks Workspace.")
    st.markdown("---")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Databricks Host**")
        st.markdown("Please enter the Databricks Host")
    with c2:
        dhost = st.text_input("", key="dhost", label_visibility="hidden", type="password")
    st.markdown("---")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Databricks Token**")
        st.markdown("Please enter the Databricks Token")
    with c2:
        dtoken = st.text_input("", key="dtoken", label_visibility="hidden", type="password")
    st.markdown("---")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Databricks Cluster ID**")
        st.markdown("Please enter the Databricks Cluster ID")
    with c2:
        dcluster = st.text_input("", key="dcluster", label_visibility="hidden", type="password")

    st.markdown("---")
    
    # st.write(f"dhost: {dhost}, dtoken: {dtoken}, dcluster: {dcluster}") 
    if dhost and dtoken and dcluster:
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
    



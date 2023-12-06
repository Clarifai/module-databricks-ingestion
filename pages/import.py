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

from utils.functions import upload_from_dataframe, list_user_apps, list_dataset, upload_trigger_function, change_font_size

st.set_page_config(layout="wide")
ClarifaiStreamlitCSS.insert_default_css(st)

# This must be within the display() function.
auth = ClarifaiAuthHelper.from_streamlit(st)
os.environ['CLARIFAI_PAT']=st.secrets.CLARIFAI_PAT


config = Config(
  host       = st.secrets.host,
  token      = st.secrets.token,
  cluster_id = st.secrets.cluster_id
)
spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
wc = WorkspaceClient(host= st.secrets.host,token= st.secrets.token,)

st.title("Databricks UI module for Import")
if 'reset_session' not in st.session_state:
    st.session_state.reset_session = False

query_params = st.experimental_get_query_params()
user_id=query_params.get("user_id", [])[0]
space="&nbsp;"*17
st.markdown(
  """
  <style>
  .logo-container {
      position: absolute;
      top:  -60px;
      left: -25px;
      padding: 10px;
      display: flex;
      align-items: center;
  }
  .logo {
      margin-left: 10px;
      font-weight: bold;
  }
  .title {
      position: relative;
      z-index: 1;
  }
  </style>
  """,
  unsafe_allow_html=True,
)
st.write(f'<div class="logo">{space} Databricks </div>', unsafe_allow_html=True)
st.write(
  '<div class="logo-container">'
  '<img class="logo" src="https://i.imgur.com/ivXkUoH.png" width="65">'
  '</div>',
  unsafe_allow_html=True,
)


tab1,tab2=st.tabs([f'**Databricks Unity catalog Volume**',f'**S3**'])

with tab1:
    catalog=[catalog.full_name for catalog in wc.catalogs.list()]
    catalog_selected = st.selectbox(f"**List of catalogs available**", catalog, index=1)

    if catalog_selected:
        st.session_state.reset_session = True
        schema= [schema.name for schema in wc.schemas.list(catalog_name=catalog_selected)]
        schema_selected = st.selectbox(f"**List of schemas available**", schema)

    if schema_selected:
        st.session_state.reset_session = True
        volumes=[vol.name for vol in wc.volumes.list(catalog_name=catalog_selected, schema_name=schema_selected)]
        volume_selected = st.selectbox(f"**List of volumes available**", volumes)
        st.session_state.reset_session = True
    
    cat_sch_vol=""
    if volume_selected:
      cat_sch_vol='/Volumes/'+catalog_selected+'/'+schema_selected+'/'+volume_selected
      st.session_state.reset_session = True
    
    file_type=st.radio(f"**choose file type**",['csv','Delta'],key="file_type",horizontal=True)
    if volume_selected:
            file_path3=st.text_input(f"**Please enter source file name of the file to be uploaded**", key="volumepath")
            file_path3=cat_sch_vol+'/'+file_path3+'/'
    
    #To give some space above the logo
    st.write("##")

    st.write(f'<div class="logo">{space}Clarifai App</div>', unsafe_allow_html=True)
    st.write(
    '<div class="logo-container">'
    '<img class="logo" src=https://logodix.com/logo/1715434.jpg width="65">'
    '</div>',
    unsafe_allow_html=True,)
    st.write("</div>", unsafe_allow_html=True)

    apps=st.selectbox(f"**Select App**",list_user_apps(user_id=user_id) ,key="apps")
    if apps:
        st.session_state.reset_session = True
        dataset_id=st.selectbox(f"**Select the dataset**",list_dataset(app_id=apps,user_id=query_params.get("user_id", [])[0]),key="dataset_volume")
        params={
            "user_id": user_id,
            "app_id": apps,
            "dataset_id": dataset_id
        }
        ann_labels_only=st.checkbox("_upload inputs with labels/concepts_",key="ann_labels_only")
        input_obj=Inputs(user_id=params['user_id'], app_id=params['app_id'])
        dataset = Dataset(dataset_id=params['dataset_id'])
    
    if st.button('Upload', key='one'):
        upload_trigger_function(file_path3,dataset_id,input_obj,ann_labels_only,file_type,spark)
              

with tab2:
  file_type=st.radio(f"**choose file type**",['csv','Delta'],key="file_type2",horizontal=True)
  file_path3=st.text_input(f"**Please enter source file name of the file to be uploaded**", key="s3path")
  st.write("##")

  st.write(f'<div class="logo">{space}Clarifai App</div>', unsafe_allow_html=True)
  st.write(
  '<div class="logo-container">'
  '<img class="logo" src=https://logodix.com/logo/1715434.jpg width="65">'
  '</div>',
  unsafe_allow_html=True,)
  st.write("</div>", unsafe_allow_html=True)

  apps=st.selectbox(f"**Select App**",list_user_apps(user_id=user_id) ,key="apps2")
  if apps:
        st.session_state.reset_session = True
        dataset_id=st.selectbox(f"**Select the dataset**",list_dataset(app_id=apps,user_id=query_params.get("user_id", [])[0]),key="dataset_S3")
        params={
            "user_id": user_id,
            "app_id": apps,
            "dataset_id": dataset_id
        }
        ann_labels_only=st.checkbox("_upload inputs with labels/concepts_",key="ann_labels_only2")
        input_obj=Inputs(user_id=params['user_id'], app_id=params['app_id'])
        dataset = Dataset(dataset_id=params['dataset_id'])

  if st.button('Upload', key='two'):
        upload_trigger_function(file_path3,dataset_id,input_obj,ann_labels_only,file_type,spark)
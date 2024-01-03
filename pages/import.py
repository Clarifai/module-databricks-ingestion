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

from utils.functions import upload_from_dataframe, list_user_apps, list_dataset, upload_trigger_function, upload_images_from_volume

st.set_page_config(layout="wide")
ClarifaiStreamlitCSS.insert_default_css(st)

# This must be within the display() function.
auth = ClarifaiAuthHelper.from_streamlit(st)
os.environ['CLARIFAI_PAT']=st.secrets.CLARIFAI_PAT


config = Config(
  host       = st.secrets.DATABRICKS_HOST,
  token      = st.secrets.DATABRICKS_TOKEN,
  cluster_id = st.secrets.DATABRICKS_CLUSTER_ID
)
spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
wc = WorkspaceClient(host= st.secrets.DATABRICKS_HOST,token= st.secrets.DATABRICKS_TOKEN,)

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


tab1,tab2=st.tabs([f'**Databricks Unity Catalog**',f'**S3**'])

with tab1:
    st.write(f"**Please select the images folder**")
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
            if volume_selected:
                 volume_folder_path='/Volumes/'+catalog_selected+'/'+schema_selected+'/'+volume_selected+'/'
                 cols=st.columns(6)
                 with cols[0]:
                        job_id=st.text_input("**Enter Job-id :**", key="job_id")
    #file_type=st.radio(f"**Choose file type**",['Delta Table','csv file','Volume/Folder path'],key="file_type",horizontal=True)
    
    #Clarifai APP 
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

        #if not file_type=="Volume/Folder path" :
            #ann_labels_only=st.checkbox("_upload inputs with labels/concepts_",key="ann_labels_only")

        input_obj=Inputs(user_id=params['user_id'], app_id=params['app_id'])
        dataset = Dataset(dataset_id=params['dataset_id'])
    
    if st.button('Upload', key='one'):
            with st.spinner('Uploading images from volume'):
                upload_images_from_volume(st.secrets.DATABRICKS_HOST,st.secrets.DATABRICKS_TOKEN,
                                         volume_folder_path,params['user_id'],params['app_id'],
                                        params['dataset_id'], job_id)
              
 
with tab2:
  file_type=st.radio(f"**Choose file format type with source information as URLs**",['csv file','Delta format (parquet)'],key="file_type2",horizontal=False)
  file_path3=st.text_input(f"**Please enter source file S3 link**", key="s3path")
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
        #ann_labels_only=st.checkbox("_upload inputs with labels/concepts_",key="ann_labels_only2")
        input_obj=Inputs(user_id=params['user_id'], app_id=params['app_id'])
        dataset = Dataset(dataset_id=params['dataset_id'])

  if st.button('Upload', key='two'):
        upload_trigger_function(file_path3,dataset_id,input_obj,file_type,spark, source="S3")
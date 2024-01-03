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

from utils.functions import export_annotations_to_dataframe, export_inputs_to_dataframe, list_dataset, list_user_apps, export_images_to_volume

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

st.title("Databricks UI Module for Export")

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

# Display the Clarifai logo image and text
st.write(f'<div class="logo">{space}Clarifai App</div>', unsafe_allow_html=True)
st.write(
  '<div class="logo-container">'
  '<img class="logo" src=https://logodix.com/logo/1715434.jpg width="65">'
  '</div>',
  unsafe_allow_html=True,
)
st.write("</div>", unsafe_allow_html=True)

apps=st.selectbox("**Select the app**",list_user_apps(user_id=user_id) ,key="apps")
if apps:
    st.session_state.reset_session = True
    dataset_id=st.selectbox("**Select the dataset**",list_dataset(app_id=apps,user_id=user_id),key="dataset")
    params={
        "user_id": user_id,
        "app_id": apps,
        "dataset_id": dataset_id
    }
    #ann_labels_only=st.checkbox("_Only export inputs with annotations_",key="ann_labels_only")

st.write("##")

st.write(f'<div class="logo">{space} Databricks </div>', unsafe_allow_html=True)
st.write(
  '<div class="logo-container">'
  '<img class="logo" src="https://i.imgur.com/ivXkUoH.png" width="65">'
  '</div>',
  unsafe_allow_html=True,
)

st.write("##")
table_landing=st.toggle("Create table under catalog")
if table_landing:
    catalog=[catalog.full_name for catalog in wc.catalogs.list()]
    catalog_selected = st.selectbox("**List of catalogs available**", catalog)

    if catalog_selected:
        schema= [schema.name for schema in wc.schemas.list(catalog_name=catalog_selected)]
        schema_selected = st.selectbox("**List of schemas available**", schema)
export_images=st.checkbox("**Export images**",key="export_images")
if export_images and not table_landing:
    export_path=st.text_input("_Please enter volume path for image_", key="export_volume_path") 
if export_images and table_landing:
    if schema_selected:
        volumes=[vol.name for vol in wc.volumes.list(catalog_name=catalog_selected, schema_name=schema_selected)]
        volume_selected = st.selectbox("**List of volumes available**", volumes)
        export_path='/Volumes/'+catalog_selected+'/'+schema_selected+'/'+volume_selected+'/' 

with st.form(key="data-inputs-2"):
    table_name=st.text_input("**Please enter the delta table name**")
    submitted_1=st.form_submit_button('Export')
    if submitted_1:
        try:
            obj=Inputs(user_id=params['user_id'], app_id=params['app_id'])
            dataset = Dataset(dataset_id=params['dataset_id'])
            my_bar = st.progress(0, text="Exporting ! Please wait.")
            df2,df3=export_annotations_to_dataframe(input_obj=obj,dataset_id=dataset.id, bar=my_bar)
            if export_images and export_path is not None :
                    export_images_to_volume(df3, export_path, wc)
            
            with st.spinner('In progress...'):
                st.write("File to Export (Preview of sample structure from first 10 records)",df2.head(10))
                df2=spark.createDataFrame(df2)

                my_bar.progress(int(80))
                if table_landing:
                    table_name= f"`{catalog_selected}`.`{schema_selected}`.{table_name}"  
                df2.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(table_name)
                my_bar.progress(int(100))
                st.success(f'Export annotation done successfully !!') 
                st.balloons()

        except Exception as e:
            st.write(f'error:{e}')

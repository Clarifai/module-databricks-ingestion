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

from utils.functions import list_user_apps, list_dataset, export_annotations_to_dataframe, export_inputs_to_dataframe

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


st.title("Databricks UI Module to Update tables")

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


st.write("###")
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
st.write("###")

catalog=[catalog.full_name for catalog in wc.catalogs.list()]
catalog_selected = st.selectbox(f"**List of catalogs available**", catalog, index=1)

if catalog_selected:
    st.session_state.reset_session = True
    schema= [schema.name for schema in wc.schemas.list(catalog_name=catalog_selected)]
    schema_selected = st.selectbox(f"**List of schemas available**", schema)
    if schema_selected:
        st.session_state.reset_session = True
        cols=st.columns(4)
        with cols[0]:
            tables=[table.name for table in wc.tables.list(catalog_name=catalog_selected, schema_name=schema_selected)]
            delta_table_selected = st.selectbox(f"**Select Annotations delta table**", tables)
        with cols[1]:
            if delta_table_selected:
                inputs_delta_table=st.selectbox(f"**Select Inputs delta table**", tables, index=1)
                if delta_table_selected == inputs_delta_table:
                    st.warning("_Please select a different table_")
                    
if delta_table_selected != inputs_delta_table:
    obj=Inputs(user_id=params['user_id'], app_id=params['app_id'])
    dataset = Dataset(dataset_id=params['dataset_id'])
    if st.button('Update', key='first_update'):
        my_bar = st.progress(0, text="Updating annotations ! Please wait.")
        df2,df3=export_annotations_to_dataframe(input_obj=obj,dataset_id=dataset.id, bar=my_bar)
        with st.spinner('In progress...'):
            df2=spark.createDataFrame(df2)
            temp_table=df2.createOrReplaceTempView("source_table")
            st.write("Annotations Data to upsert (Preview of sample structure from first 5 records)",spark.sql(f"SELECT * FROM source_table LIMIT 5").toPandas())
            spark.sql(f"""USE CATALOG `{catalog_selected}` ;""")
            spark.sql(f"USE SCHEMA `{schema_selected}` ;")
            spark.sql(f"""MERGE INTO `{delta_table_selected}` as target_table
                    USING source_table ON target_table.annotation_id = source_table.annotation_id
                    WHEN MATCHED AND source_table.annotation_modified_at > target_table.annotation_modified_at
                    THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT * ; """)
            
            if inputs_delta_table:
                input_df=export_inputs_to_dataframe(params['user_id'],params['app_id'],params['dataset_id'],spark)
                input_df.createOrReplaceTempView("input_source_table")
                st.write("Input Data to upsert (Preview of sample structure from first 5 records)",spark.sql(f"SELECT * FROM input_source_table LIMIT 5").toPandas())
                spark.sql(f"""MERGE INTO `{inputs_delta_table}` as target_table
                        USING input_source_table ON target_table.input_id = input_source_table.input_id
                        WHEN MATCHED THEN UPDATE SET * 
                        WHEN NOT MATCHED THEN INSERT * ; """)
            
            my_bar.progress(int(100))
            st.success("Tables updated successfully")
         
    


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
from utils.functions import export_inputs_to_dataframe, list_user_apps, list_dataset, upload_trigger_function, upload_images_from_volume


def show_import_page(auth, config):
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
    wc = WorkspaceClient(host=config.host,token= config.token,)

    st.markdown("""<h5>Import data from Databricks to Clarifai App </h5>""", unsafe_allow_html=True)
    st.text(" ")
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
        catalog_selected = st.selectbox(label=f"**List of catalogs available**", options=catalog, index=1, key="catg1")
        if catalog_selected:
            st.session_state.reset_session = True
            schema= [schema.name for schema in wc.schemas.list(catalog_name=catalog_selected)]
            schema_selected = st.selectbox(label=f"**List of schemas available**", options=schema, key="sch1")
            if schema_selected:
                st.session_state.reset_session = True
                volumes=[vol.name for vol in wc.volumes.list(catalog_name=catalog_selected, schema_name=schema_selected)]
                volume_selected = st.selectbox(label=f"**List of volumes available**", options=volumes, key="vol1")
                if volume_selected:
                    volume_folder_path='/Volumes/'+catalog_selected+'/'+schema_selected+'/'+volume_selected+'/'
                    cols=st.columns(4)
                    with cols[0]:
                            input_table=st.text_input("**Enter Inputs Delta table name :**", key="input_table_name")
                            input_table_name=f"`{catalog_selected}`.`{schema_selected}`.`{input_table}`"
                            
        #file_type=st.radio(f"**Choose file type**",['Delta Table','csv file','Volume/Folder path'],key="file_type",horizontal=True)
        st.markdown("""<hr style="height:2px;border:none;color:#aaa;background-color:#aaa;" /> """, unsafe_allow_html=True)

        #Clarifai APP 
        #To give some space above the logo
        st.write(f'<div class="logo">{space}Clarifai App</div>', unsafe_allow_html=True)
        st.write(
        '<div class="logo-container">'
        '<img class="logo" src=https://logodix.com/logo/1715434.jpg width="65">'
        '</div>',
        unsafe_allow_html=True,)
        st.write("</div>", unsafe_allow_html=True)

        apps=st.selectbox(f"**Select App**",list_user_apps(user_id=user_id) ,key="apps1")
        if apps:
            st.session_state.reset_session = True
            dataset_id=st.selectbox(label=f"**Select the dataset**", options=list_dataset(app_id=apps,user_id=query_params.get("user_id", [])[0]), key="dataset_volume1")
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
                    image_upload=upload_images_from_volume(config.host, config.token, 
                                              config.cluster_id, params['app_id'],
                                              params['dataset_id'], volume_folder_path, params['user_id'] )
                    if image_upload == "SUCCESS":
                        st.success(f'Images uploaded successfully to Clarifai App')
                    else:
                        st.error(f'Error uploading images to Clarifai App')

                if len(input_table) > 0 and image_upload == "SUCCESS":
                    with st.spinner('Exporting inputs to Delta table'):
                        df = export_inputs_to_dataframe(params['user_id'],params['app_id'],params['dataset_id'],spark)
                        df.write.mode("overwrite").saveAsTable(input_table_name)
                        st.success(f'Inputs exported successfully to {input_table_name}')
                    
    
    with tab2:
        file_type=st.radio(f"**Choose file format type with source information as URLs**",['csv file','Delta format (parquet)'],key="file_type2",horizontal=False)
        file_path3=st.text_input(f"**Please enter source file S3 link**", key="s3path")
        
        st.markdown("""<hr style="height:2px;border:none;color:#555;background-color:#555;" /> """, unsafe_allow_html=True)
        st.write(f'<div class="logo">{space}Clarifai App</div>', unsafe_allow_html=True)
        st.write(
        '<div class="logo-container">'
        '<img class="logo" src=https://logodix.com/logo/1715434.jpg width="65">'
        '</div>',
        unsafe_allow_html=True,)
        st.write("</div>", unsafe_allow_html=True)

        apps=st.selectbox(label=f"**Select App**", options=list_user_apps(user_id=user_id) ,key="apps2")
        if apps:
            st.session_state.reset_session = True
            dataset_id=st.selectbox(label=f"**Select the dataset**", options=list_dataset(app_id=apps,user_id=query_params.get("user_id", [])[0]), key="dataset_S31")
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


def show_export_page(auth, config):
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
    wc = WorkspaceClient(host=config.host, token=config.token,)

    st.markdown("""<h5>Export data from Clarifai App to Databricks </h5>""", unsafe_allow_html=True)
    st.text(" ")

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

    apps=st.selectbox(label="**Select the app**", options=list_user_apps(user_id=user_id) ,key="apps3")
    if apps:
        st.session_state.reset_session = True
        dataset_id=st.selectbox(label="**Select the dataset**", options=list_dataset(app_id=apps,user_id=user_id), key="dataset1")
        params={
            "user_id": user_id,
            "app_id": apps,
            "dataset_id": dataset_id
        }
        #ann_labels_only=st.checkbox("_Only export inputs with annotations_",key="ann_labels_only")
    
    st.markdown("""<hr style="height:2px;border:none;color:#aaa;background-color:#aaa;" /> """, unsafe_allow_html=True)
    st.write("##")

    st.write(f'<div class="logo">{space} Databricks </div>', unsafe_allow_html=True)
    st.write(
    '<div class="logo-container">'
    '<img class="logo" src="https://i.imgur.com/ivXkUoH.png" width="65">'
    '</div>',
    unsafe_allow_html=True,
    )

    st.write("##")
    export_images=st.checkbox("**Export images**",key="export_images")

    catalog=[catalog.full_name for catalog in wc.catalogs.list()]
    catalog_selected = st.selectbox(label="**List of catalogs available**", key="catg2", options=catalog)

    if catalog_selected:
        schema= [schema.name for schema in wc.schemas.list(catalog_name=catalog_selected)]
        schema_selected = st.selectbox(label="**List of schemas available**", key="sch2", options=schema)

    if export_images :
        if schema_selected:
            volumes=[vol.name for vol in wc.volumes.list(catalog_name=catalog_selected, schema_name=schema_selected)]
            volume_selected = st.selectbox(label="**List of volumes available**", key="vol3", options=volumes)
            if volume_selected:
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
                
                if export_images: 
                        if export_path:
                            export_images_to_volume(df3, export_path, wc)

                with st.spinner('In progress...'):
                    st.write("File to Export (Preview of sample structure from first 10 records)",df2.head(10))
                    df2=spark.createDataFrame(df2)
                    my_bar.progress(int(80))
                    table_name= f"`{catalog_selected}`.`{schema_selected}`.{table_name}"  
                    df2.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(table_name)
                    my_bar.progress(int(100))
                    st.success(f'Export annotation done successfully !!') 

            except Exception as e:
                st.write(f'error:{e}')


def show_update_page(auth, config):
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
    wc = WorkspaceClient(host=config.host, token=config.token)

    st.markdown("""<h5>Update Databricks Delta Tables with New Annotations </h5>""", unsafe_allow_html=True)
    
    st.text(" ")

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

    apps=st.selectbox(label="**Select the app**", options=list_user_apps(user_id=user_id) ,key="apps4")
    if apps:
        st.session_state.reset_session = True
        dataset_id=st.selectbox(label="**Select the dataset**", options=list_dataset(app_id=apps,user_id=user_id), key="dataset2")
        params={
            "user_id": user_id,
            "app_id": apps,
            "dataset_id": dataset_id
        }

    st.markdown("""<hr style="height:2px;border:none;color:#aaa;background-color:#aaa;" /> """, unsafe_allow_html=True)
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
    catalog_selected = st.selectbox(label=f"**List of catalogs available**", options=catalog, index=1, key="catg3")

    if catalog_selected:
        st.session_state.reset_session = True
        schema= [schema.name for schema in wc.schemas.list(catalog_name=catalog_selected)]
        schema_selected = st.selectbox(label=f"**List of schemas available**", options=schema, key="sch3")
        if schema_selected:
            st.session_state.reset_session = True
            cols=st.columns(4)
            with cols[0]:
                tables=[table.name for table in wc.tables.list(catalog_name=catalog_selected, schema_name=schema_selected)]
                delta_table_selected = st.selectbox(label=f"**Select Annotations delta table**", options=tables, key="table1")
            with cols[1]:
                if delta_table_selected and len(tables) > 1:
                    inputs_delta_table=st.selectbox(label=f"**Select Inputs delta table**", options=tables, key="table2")
                    if delta_table_selected == inputs_delta_table:
                        st.warning("_Please select a different table_")
    if delta_table_selected:                   
        if (delta_table_selected != inputs_delta_table):
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
            
        


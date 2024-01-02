import os
import json
import uuid
import time
import requests
from stqdm import stqdm
import pandas as pd
import streamlit as st
from clarifai.errors import UserError
from google.protobuf.json_format import MessageToJson
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
from clarifai.utils.misc import Chunker
from google.protobuf.struct_pb2 import Struct
from clarifai.client.input import Inputs
from clarifai.client.user import User
from clarifai.client.app import App

def custom_warning_handler(message, category, filename, lineno, file=None, line=None):
    warning_message = f"{category.__name__}: {message} (File: {filename}, Line: {lineno})"
    st.warning(warning_message)

def list_user_apps(user_id : str ):
   apps =list(User(user_id).list_apps())
   app_list=[]
   for app in apps:
      app_list.append(app.id)
   return app_list

def list_dataset(app_id,user_id):
   app = App(app_id=app_id, user_id=user_id)
   dataset_list=[]
   for dataset in list(app.list_datasets()):
      dataset_list.append(dataset.id)
   return dataset_list

def export_images_to_volume(df_url, volumepath, workspace_client):
    
    for i in stqdm(range(len(df_url)), desc="Downloading Images"):
        imgid = df_url.iloc[i]['input_id']
        url = df_url.iloc[i]['image_url']
        ext = df_url.iloc[i]['img_format']
        img_name = os.path.join(volumepath, f"{imgid}.{ext.lower()}")
        headers = {"Authorization": st.secrets.CLARIFAI_PAT}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            workspace_client.files.upload(file_path=img_name, contents =response.content)

    return 'done'

def export_inputs_to_dataframe(input_obj ,dataset_id, bar):
    """Export all the inputs from clarifai App's dataset to spark dataframe.

    Args:
        input_obj (Clarifai INPUT object): Input object for the clarifai app.
        dataset_id (str): Dataset id of the clarifai app.

    Examples:
        TODO

    Returns:
        spark dataframe with inputs"""
    

    input_list = []
    response = list(input_obj.list_inputs(dataset_id=dataset_id))
    counter=0
    for inp in response:
      temp = {}
      temp['input_id'] = inp.id
      temp['image_url'] = inp.data.image.url
      temp['image_info'] = str(inp.data.image.image_info)
      try:
        created_at = float(f"{inp.created_at.seconds}.{inp.created_at.nanos}")
        temp['input_created_at'] = time.strftime('%m/%d/% %H:%M:%5', time.gmtime(created_at))
        modified_at = float(f"{inp.modified_at.seconds}.{inp.modified_at.nanos}")
        temp['input_modified_at'] = time.strftime('%m/%d/% %H:%M:%5', time.gmtime(modified_at))
      except:
        temp['input_created_at'] = float(f"{inp.created_at.seconds}.{inp.created_at.nanos}")
        temp['input_modified_at'] = float(f"{inp.modified_at.seconds}.{inp.modified_at.nanos}")
      input_list.append(temp)
      bar.progress(int((counter + 1) / len(response) * 60))
      counter+=1

    return pd.DataFrame(input_list)

def export_annotations_to_dataframe(input_obj,
                                    dataset_id,
                                    bar):

    annotation_list = []
    all_inputs = list(input_obj.list_inputs(dataset_id=dataset_id))
    response = list(input_obj.list_annotations(batch_input=all_inputs))
    counter=0
    inp_dict={}
    images_to_download=[]
    for inp in all_inputs:
      temp={}
      temp['image_url'] = inp.data.image.url
      temp['img_format']= inp.data.image.image_info.format
      inp_dict[inp.id]=temp

    for an in stqdm(response,desc="Exporting annotations"):

      temp = {}
      temp['annotation'] = str(an.data)
      if not temp['annotation'] or temp['annotation'] == '{}' or temp['annotation'] == {}:
        continue
      temp['annotation_id'] = an.id
      temp['annotation_user_id'] = an.user_id
      temp['input_id'] = an.input_id

      if temp['input_id'] not in images_to_download :
        val={"input_id":an.input_id}
        val.update(inp_dict[an.input_id])
        images_to_download.append(val)

      created_at = float(f"{an.created_at.seconds}.{an.created_at.nanos}")
      temp['annotation_created_at'] = time.strftime('%m/%d/% %H:%M:%5', time.gmtime(created_at))
      modified_at = float(f"{an.modified_at.seconds}.{an.modified_at.nanos}")
      temp['annotation_modified_at'] = time.strftime('%m/%d/% %H:%M:%5', time.gmtime(modified_at))
      annotation_list.append(temp)
      bar.progress(int((counter + 1) / len(response) * 60))
      counter+=1
    df = pd.DataFrame(annotation_list)
    df_url= pd.DataFrame(images_to_download)
    return (df,df_url)


def get_inputs_from_dataframe(dataframe,
                              bar,
                              input_obj,
                              input_type: str ,
                              df_type: str ,
                              dataset_id: str = None,
                              labels: str = True):
  input_protos = []
  columns=dataframe.columns
  dataframe = dataframe.collect()

  for i,row in enumerate(dataframe):  
    time.sleep(0.01) 
    if labels:
          labels_list = row["concepts"].split(',')
          labels =labels_list if len(row['concepts']) > 0 else None
    else:
          labels = None

    if 'metadata' in columns:
          if row['metadata'] is not None and len(row['metadata']) > 0:
            metadata_str = row['metadata'].replace("'", '"')
            print((metadata_str))
            try:
              metadata_dict = json.loads(metadata_str)
            except json.decoder.JSONDecodeError:
              raise UserError("metadata column in CSV file should be a valid json")
            metadata = Struct()
            metadata.update(metadata_dict)
          else:
            metadata = None
    else:
          metadata = None
        
    if 'geopoints' in columns:
          if row['geopoints'] is not None and len(row['geopoints']) > 0:
            geo_points = row['geopoints'].split(',')
            geo_points = [float(geo_point) for geo_point in geo_points]
            geo_info = geo_points if len(geo_points) == 2 else UserError(
                "geopoints column in CSV file should have longitude,latitude")
          else:
            geo_info = None
    else:
        geo_info = None
  
    input_id = row['inputid'] if 'inputid' in columns else uuid.uuid4().hex
    text = row['input'] if input_type == 'text' else None
    image = row['input'] if input_type == 'image' else None
    video = row['input'] if input_type == 'video' else None
    audio = row['input'] if input_type == 'audio' else None

    if df_type == 'raw':
      input_protos.append(input_obj.get_text_input(
              input_id=input_id,
              raw_text=text,
              dataset_id=dataset_id,
              labels=labels,
              metadata=metadata,
              geo_info=geo_info))
    elif df_type == 'url':
      input_protos.append(input_obj.get_input_from_url(
              input_id=input_id,
              image_url=image,
              text_url=text,
              audio_url=audio,
              video_url=video,
              dataset_id=dataset_id,
              labels=labels,
              metadata=metadata,
              geo_info=geo_info))
    else:
      input_protos.append(
        input_obj.get_input_from_file(
              input_id=input_id,
              image_file=image,
              text_file=text,
              audio_file=audio,
              video_file=video,
              dataset_id=dataset_id,
              labels=labels,
              metadata=metadata,
              geo_info=geo_info))
  
    bar.progress(int((i + 1) / len(dataframe) * 60))
  time.sleep(0.001)
      
  return input_protos

def upload_from_dataframe(dataframe,
                          bar,
                          input_obj,
                          dataset_id: str,
                          input_type: str,
                          df_type: str = None,
                          labels: bool = True,
                          batch_size: int = 128) -> None:

    if input_type not in ('image', 'text', 'video', 'audio'):
      raise UserError('Invalid input type, it should be image,text,audio or video')
    
    if df_type not in ('raw', 'url', 'file_path'):
      raise UserError('Invalid csv type, it should be raw, url or file_path')

    if df_type == 'raw' and input_type != 'text':
      raise UserError('Only text input type is supported for raw csv type')
  
    
    batch_size = min(128, batch_size)
    
    input_protos = get_inputs_from_dataframe(
        dataframe=dataframe,
        bar=bar,
        input_obj=input_obj,
        df_type=df_type,
        input_type=input_type,
        dataset_id=dataset_id,
        labels=labels)
    try:
       input_obj._bulk_upload(inputs=input_protos,batch_size=batch_size)
    except Exception as e:
        st.write(f'error:{e}')

def upload_trigger_function(table_name,dataset_id,input_obj,file_type,spark_session, source = "Databricks"):
   
   """Uploads the file to the clarifai app's dataset. Once upload button is triggered
   Args:
       file_path (str): path of the file to be uploaded
       dataset_id (str): Dataset id of the clarifai app.
       input_obj (Clarifai INPUT object): Input object for the clarifai app.
       ann_labels_only (bool): True if the file to be uploaded has labels/concepts.
       file_type (str): type of the file to be uploaded.
       spark_session (spark session): spark session object.

    Returns:
       Displays success message once the upload is done."""
   try:
        if file_type=="csv file":
            df1 = spark_session.read.csv(table_name,header=True, inferSchema=True)
        elif file_type=="parquet":
            if source=="S3":
              df1=spark_session.read.format("delta").option("header", True).load(table_name)
            else:
              df1=spark_session.sql(f"SELECT * FROM {table_name}" )

        with st.spinner('In progress...'):
            st.write("File to be uploaded (Preview of sample structure from first 10 records)",df1.toPandas().head(5))  
        my_bar = st.progress(0, text="Uploading ! Please wait.")
        upload_from_dataframe(dataframe=df1,
                                bar=my_bar,
                                input_obj=input_obj,
                                dataset_id=dataset_id,
                                input_type="image",
                                df_type="url",
                                labels=True)
        my_bar.progress(int(100))
        st.success(f'datasets uploaded successfully')
        st.balloons()
            
   except Exception as e:
                st.write(f'error : {e}') 
                

def upload_images_from_volume (databricks_host, databricks_token, volume_folder_path, userid, appid, datasetid):
  url = f'{databricks_host}/api/2.1/jobs/run-now'

  headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {databricks_token}',
  }

  json_payload = {
    "job_id": 130100571063000, # job id for upload images from volume
    "queue": {
      "enabled": True
    },
    "param": "overriding_val",
    "job_parameters": 
      {
        "app_id": appid,
        "dataset_id": datasetid,
        "file_path":volume_folder_path,
        "user_id": userid
      } 
  }
  response = (requests.post(url, headers=headers, json=json_payload))
  
  if response.status_code != 200:
    print(f"Failed to trigger job run. Status code: {response.status_code}")
    print(response.text) 

  runid=response.json()['run_id']

  while True:
      response_get = (requests.get(f'{databricks_host}/api/2.1/jobs/runs/get', headers=headers, json={
      "run_id": runid})).json()
      #st.write(response_json)
      if response_get['state']['life_cycle_state'] not in ['RUNNING', 'QUEUED'] and response_get['state']['result_state'] in ['SUCCESS','FAILED']:
        status=response_get['state']['result_state']
        if status=="SUCCESS":
          st.success('Images uploaded successfully')
        else:
          st.error('Images upload failed')
        return status
      time.sleep(15)

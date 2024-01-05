import os
import json
import uuid
import time
import requests
from stqdm import stqdm
import pandas as pd
import streamlit as st

from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict
from google.protobuf.json_format import MessageToJson

from clarifai.client.app import App
from clarifai.client.user import User
from clarifai.errors import UserError
from clarifai.client.input import Inputs
from clarifai.client.search import Search

from concurrent.futures import ThreadPoolExecutor, as_completed

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

def validate_databricks_config(host,token,cluster_id):
    url = f'{host}/api/2.0/clusters/list'

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
    }
    response = requests.get(url, headers=headers, json={})
    try:
      if response.status_code == 200:
          if cluster_id in [cl["cluster_id"] for cl in (response.json())["clusters"]]:
            st.success(f'Authenticated successfully')
            return True
          else:
             st.error(f'Invalid cluster id')
      if response.status_code != 200:
          st.write(response.json())
          st.error(f'Authentication error')   

    except Exception as e : 
       st.error(f'Authentication error : {e}')

def export_images_to_volume(df_url, volumepath, workspace_client):
    
    for i in stqdm(range(len(df_url)), desc="Downloading Images"):
        imgid = df_url.iloc[i]['input_id']
        url = df_url.iloc[i]['image_url']
        ext = df_url.iloc[i]['img_format']
        img_name = os.path.join(volumepath, f"{imgid}.{ext.lower()}")
        headers = {"Authorization": f"Key {st.secrets.CLARIFAI_PAT}" }
        try:
          response = requests.get(url, headers=headers)
          if response.status_code == 200:
              workspace_client.files.upload(file_path=img_name, contents =response.content)
        except Exception as e: 
          st.write(f'error downloading images : {e}')

def export_inputs_to_dataframe(user_id, app_id, dataset_id, spark_session):
    
    """Export all the inputs from clarifai App's dataset to deltatable.

    Args:
        user_id (str): User id of the clarifai app.
        app_id (str): App id of the clarifai app.
        dataset_id (str): Dataset id of the clarifai app.
        spark_session (spark session): spark session object.

    Examples:
        TODO

    Returns:
        None """
    
    search_obj = Search(user_id=user_id, app_id=app_id)
    search_response = search_obj.query(filters=[{"input_types":["image"]},{"input_dataset_ids":[dataset_id]}])
    inputs=[hit.input for response in search_response for hit in response.hits ]
    input_list=[]
    
    for inp in inputs:
      temp = {}
      temp['input_id'] = inp.id
      temp['image_url'] = inp.data.image.url
      temp['image_info'] = str(inp.data.image.image_info)
      try:
        created_at = float(f"{inp.created_at.seconds}.{inp.created_at.nanos}")
        temp['input_created_at'] = time.strftime('%m/%d/% %H:%M:%5', time.gmtime(created_at))
        modified_at = float(f"{inp.modified_at.seconds}.{inp.modified_at.nanos}")
        temp['input_modified_at'] = time.strftime('%m/%d/% %H:%M:%5',
                                                       time.gmtime(modified_at))
      except:
        temp['input_created_at'] = float(f"{inp.created_at.seconds}.{inp.created_at.nanos}")
        temp['input_modified_at'] = float(f"{inp.modified_at.seconds}.{inp.modified_at.nanos}")
      try:
        temp['input_metadata'] = str(MessageToDict(inp.data.metadata))
      except:
        temp['input_metadata'] = None
      input_list.append(temp)
    try:
        spark_df=spark_session.createDataFrame(input_list)
        
    except Exception as e:
          raise RuntimeError(f'Error: {e}')
    
    return spark_df

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
        elif file_type=="Delta format (parquet)":
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
        st.success(f'Inputs uploaded successfully')
            
   except Exception as e:
                st.write(f'error : {e}') 
                

def upload_images_from_volume(db_host,db_token,cluster_id,app_id,dataset_id,file_path,user_id):
    
    databricks_host = db_host
    databricks_token = db_token

    url = f'{databricks_host}/api/2.1/jobs/runs/submit'

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {databricks_token}',
    }

    json_payload = {
        "git_source" :{
            "git_url" :"https://github.com/Clarifai/clarifai-pyspark",
            "git_provider" :"gitHub",
            "git_branch" :"main", 
        },

        "tasks": [
    {
      "task_key": "test_ingestion",
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": "Job_script/image_ingestion_clarifai_job_nb",
        "source": "GIT",
        "base_parameters": {
          "app_id": app_id,
          "dataset_id": dataset_id,
          "file_path":file_path,
          "user_id":user_id
        },
      },
      "existing_cluster_id": cluster_id,
      "libraries": 
        {
          "pypi": {
            "package": "clarifai-pyspark==0.0.3"
          }
        }
      ,
    }
      ],
        
    "queue": {
    "enabled": True
    }
  }
    

    response = requests.post(url, headers=headers, json=json_payload)
    c=response.json()
    # Check the response
    runid=c['run_id']
    while True:
      response2 = requests.get(f'{databricks_host}/api/2.1/jobs/runs/get', headers=headers, json={
      "run_id": runid})
      c2=response2.json()
      if c2['state']['life_cycle_state'] != 'RUNNING' and c2['state']['result_state'] in ['SUCCESS','FAILED']:
        status=c2['state']['result_state']
        break
      time.sleep(5)
    return status

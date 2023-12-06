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

def change_font_size(label, font_size='12px', font_color='black'):
    html = f"""
    <script>
        var elems = window.parent.document.querySelectorAll('p');
        var elem = Array.from(elems).find(x => x.innerText == '{label}');
        elem.style.fontSize = '{font_size}';
        elem.style.color = '{font_color}';
    </script>
    """
    st.components.v1.html(html)

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
    for an in stqdm(response,desc="Exporting annotations"):
      temp = {}
      temp['annotation'] = json.loads(MessageToJson(an.data))
      if not temp['annotation'] or temp['annotation'] == '{}' or temp['annotation'] == {}:
        continue
      temp['id'] = an.id
      temp['user_id'] = an.user_id
      temp['input_id'] = an.input_id
      created_at = float(f"{an.created_at.seconds}.{an.created_at.nanos}")
      temp['created_at'] = time.strftime('%m/%d/% %H:%M:%5', time.gmtime(created_at))
      modified_at = float(f"{an.modified_at.seconds}.{an.modified_at.nanos}")
      temp['modified_at'] = time.strftime('%m/%d/% %H:%M:%5', time.gmtime(modified_at))
      annotation_list.append(temp)
      bar.progress(int((counter + 1) / len(response) * 60))
      counter+=1
    df = pd.DataFrame(annotation_list)
    return df

def export_images_to_volume(path,user_id,app_id):
    input_obj = Inputs(user_id=user_id, app_id=app_id)
    input_response = list(input_obj.list_inputs())
    for resp in stqdm(input_response , desc="Downloading images"):
      imgid = resp.id
      ext = resp.data.image.image_info.format
      url = resp.data.image.url
      img_name = path + '/' + imgid + '.' + ext.lower()
      headers = {"Authorization":f"Bearer {os.environ['CLARIFAI_PAT']}"}
      response = requests.get(url, headers=headers)
      with open(img_name, "wb") as f:
        f.write(response.content)


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

def upload_trigger_function(file_path,dataset_id,input_obj,ann_labels_only,file_type,spark_session):
   
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
        if file_type=="csv":
            #df1=df1.toPandas() -->works fast
            df1 = spark_session.read.csv(file_path,header=True, inferSchema=True)
        elif file_type=="Delta":
            df1=spark_session.read.format("delta").option("header", True).load(file_path)

        with st.spinner('In progress...'):
            st.write("File to be uploaded",df1.toPandas().head(5))  
        my_bar = st.progress(0, text="Uploading ! Please wait.")
        upload_from_dataframe(dataframe=df1,
                                bar=my_bar,
                                input_obj=input_obj,
                                dataset_id=dataset_id,
                                input_type="image",
                                df_type="url",
                                labels=ann_labels_only)
        my_bar.progress(int(100))
        st.success(f'datasets uploaded successfully')
        st.balloons()
            
   except Exception as e:
                st.write(f'error : {e}') 
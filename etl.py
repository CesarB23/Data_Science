import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 
import json
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
loaded_data_file = "etl.csv"
log_file = "logs.txt"

#Extract data from Multiple DataFormats
    # CSV
    # Json
    # XML

def read_csv(file_path):
    df = pd.read_csv(file_path)
    return df

def read_json(file_path):
    df = pd.read_json(file_path,lines=True)
    return df

def extract_from_xml(file_to_process): 
    dataframe = pd.DataFrame(columns=["name", "height", "weight"]) 
    tree = ET.parse(file_to_process) 
    root = tree.getroot() 
    for person in root: 
        name = person.find("name").text 
        height = float(person.find("height").text) 
        weight = float(person.find("weight").text) 
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name, "height":height, "weight":weight}])], ignore_index=True) 
    return dataframe 

def data_concat():
    data_structure = pd.DataFrame(columns=['name','height','weight'])

    for csvfile in glob.glob("**/*.csv"):
        data_structure = pd.concat([data_structure, pd.DataFrame(read_csv(csvfile))], ignore_index=True) 

    for js_file in glob.glob("**/*.json"):
        data_structure = pd.concat([data_structure, pd.DataFrame(read_json(js_file))], ignore_index=True)
    
    for xml in glob.glob("**/*.xml"):
        data_structure = pd.concat([data_structure, pd.DataFrame(extract_from_xml(xml))], ignore_index=True)
    
    return data_structure


# Data Transformation
# Make operations over data columns

def transform(data):
    data['height'] = round(data['height'] * 0.0254,2)
    
    data['weight'] = round(data['weight'] * 0.453592, 2)

    return data

def load_data(loaded_data,file_path):
    loaded_data.to_csv(file_path,index=False)

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
  
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = data_concat() 
  
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
  
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
  
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
  
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(transformed_data,loaded_data_file) 
  
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
  
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 
import logging
import json
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage

def load_trigger(event, context):
    """
    Nomenclatura de tablas:
         	- Transaccionales terminan con _t
            - Maestras terminan con _m
    """
    
    # Global Parameters
    log_dataset = "admin"
    log_table = "logging"
    
    bucket_name = "load_event"
    
    try:
        file = event
        start_date = datetime.now()
        write_disposition = ''
        # get file info
        file_date = file['name'].split('/')[-1].split('_')[-3]
        file_name = file['name'].split('/')[5]
        dataset =  file['name'].split('/')[1]
        table = file['name'].split('/')[-1].split('_2')[0]

        # Read config json file
        storage_client = storage.Client()
        config_bucket = storage_client.get_bucket('latam-cf-config')
        blob_file = config_bucket.get_blob('partitions.json')
        json_string = blob_file.download_as_string()
        conf = json.loads(json_string)

        client = bigquery.Client()
		
        # If transactional table Delete BQ Partition
        
        if file_name.__contains__("_t.psv.gz"):

            partition_name = conf[dataset]['transactional'][table]
            partition_date = f'{file_date[0:4]}-{file_date[4:6]}-{file_date[6:8]}'
            write_disposition = 'WRITE_APPEND'
            
            if partition_name != 'none':
                qry_delete = (f"DELETE FROM `{dataset}.{table}` WHERE {partition_name} = '{partition_date}'")
                qry_job_delete = client.query(qry_delete)
                qry_job_delete.result()
                logging.info(f'Particion para la tabla {table} con fecha {partition_date} eliminada!!!!')
        
                
        # If master table Truncate table
        if file_name.__contains__("_m.psv.gz"):
        	write_disposition = "WRITE_TRUNCATE"

        # Logging begin load
        qry_start = (f"""INSERT INTO `{log_dataset}.{log_table}` (date,platform,table,file,status,description) VALUES('{start_date}','{dataset}','{table}','{file_name}','Start','')""")
        qry_job_start = client.query(qry_start)
        qry_job_start.result()

        dataset_id = dataset

        dataset_ref = client.dataset(dataset_id)
        job_config = bigquery.LoadJobConfig()
        job_config.skip_leading_rows = 1
        job_config.field_delimiter = '|'
        job_config.write_disposition=write_disposition
    
        # Load file
        logging.info(f"Begin Load File: {file['name']}.")
        job_config.source_format = bigquery.SourceFormat.CSV
        uri = f"gs://{bucket_name}/{file['name']}"
        
        load_job = client.load_table_from_uri(
            uri, dataset_ref.table(table), job_config=job_config
        )
        
        logging.info("Starting job {}".format(load_job.job_id))

        load_job.result()
        destination_table = client.get_table(dataset_ref.table(table))
        logging.info(f'Tabla {table} Cargada correctamente!!!!!')
        
        # Logging end load
        end_date = datetime.now()
        qry_end = (f"INSERT INTO `{log_dataset}.{log_table}` (date,platform,table,file,status,description) VALUES('{end_date}','{dataset}','{table}','{file_name}','End','')")
        qry_job_end = client.query(qry_end)
        qry_job_end.result() 
        
    except Exception as e:
        # Logging exception
        qry_error = (f"INSERT INTO `{log_dataset}.{log_table}` (date,platform,table,file,status,description) VALUES('{start_date}','{dataset}','{table}','{file_name}','Error', '{e}')")
        qry_job_error = client.query(qry_error)
        qry_job_error.result()
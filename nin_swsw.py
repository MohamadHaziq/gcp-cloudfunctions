import pandas as pd
import numpy as np
import google.cloud.storage as storage
from datetime import datetime, timedelta
import traceback
import pymysql
import openpyxl
pymysql.install_as_MySQLdb()

sqlengine = None

def prepare_wyeth_report(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    ## Initial Receive Trigger

    file_name = event['name']
    bucket_name = event['bucket']

    print(f"Processing file: {file_name}.")
    print(f"From Bucket : {bucket_name}. ")


    ## Connect to Uploaded File

    try:
        storage_client = storage.Client()
        blob = storage_client.bucket(bucket_name).get_blob(file_name)
        source_bucket = storage_client.bucket(bucket_name)
        output_bucket = storage_client.bucket('wyeth_processed')
        blob_uri = f'gs://{bucket_name}/{file_name}'
        
        print ('Declared Cloud Variables')

    except:
        print ("Error is with Cloud Setup")
        print(traceback.format_exc())


    ## Merge File into DataFrame

    try:
        if str(file_name).endswith('.csv'):
            df = pd.read_csv(blob_uri)
        else:
            df = pd.read_excel(blob_uri)

    except:
        print ("Dataframe couldn't start")
        print (f"The problem URI : {blob_uri}")
        print(traceback.format_exc())


    ## Move File to Archive Folder
    try:
        new_blob_name = 'processed/' + str(datetime.today().date()) + '_' + file_name
        new_blob = source_bucket.copy_blob(blob, output_bucket, new_blob_name)
        blob.delete()
        print("Blob {} has been renamed to {}".format(blob.name, new_blob.name))
    except:
        print ("Unable to move file !")
        print (traceback.format_exc())
    

    ## Rename Columns, Clean Dataframe, Generate New Composite Key

    try:
        renamecolumns = { 
        'Created' : 'm_date', 'First Name': 'firstname',
        'Last Name': 'lastname', 'Email': 'email',
        'Mobile': 'mobileno', 'Address': 'address1',
        'State': 'state', 'Town': 'city',
        'Zip': 'postcode', 'Pregnant': 'pregnant',
        'Child name': 'childname', 'Child gender': 'child_gender',
        'Child birth date': 'childdob', 'Source': 'm_source', 
        'Product': 'm_product'
        }

        df = df.rename(renamecolumns, axis = 1)
        df = df.drop(['Status','Type'], axis = 1 )

        try:
            df = df.drop(['Photo'], axis = 1)
        except:
            print ('Photos didn\'t exist')
        
        print (df.columns)

        df['m_date'] = pd.to_datetime(df['m_date']).dt.date
        df['composite_key'] = df['mobileno'].astype(str) + '_' + df['m_product']
        df = df.sort_values('m_date').reset_index(drop = True)

    except:
        print ('Error is in File Processing')
        print(traceback.format_exc())
    
    ## Create Estimate Age
    try:
        df['childdob'] = pd.to_datetime(df.childdob, errors = 'coerce').dt.date
        df['estimateage'] = datetime.today().date() - df.childdob 

    ## Slicing the kids age by category
        df.estimateage = pd.cut(df.estimateage, 
            bins = [
            pd.Timedelta(days = -20000),
            pd.Timedelta(days = 0),
            pd.Timedelta(days = 179),
            pd.Timedelta(days = 364),
            pd.Timedelta(days = 1094),
            pd.Timedelta(days = 2554),
            pd.Timedelta(weeks = 2555)
            ], labels = ['Unborn', '6 Months and Less', '11 Months and Less','1 Year - 3 Years', '4 - 6 Years', '7 Years Old and Above'] )

        df['estimateage'] = np.where(df['childdob'].isna(), "Pregnant / Unborn", df['estimateage'])

    except:
        print('Couldn\'t Process Estimate Age')
        print(traceback.format_exc())

    ## Call MySQL Data from past 6 months and compare
    ## Check for valid and duplicates

    try:
        lookback_period = (datetime.today() - timedelta(days=60)).date()
        query = f'SELECT composite_key FROM wyeth_samples WHERE m_date > {lookback_period}'
        lookback_data = pd.read_sql(query, sqlengine)
        
        df['status'] = np.where(df['composite_key'].isin(lookback_data['composite_key']), 
                                'Duplicate Found In Last 6 Months', 'Valid')

    except: 
        print ('SQL didn\'t quite work')
        print(traceback.format_exc())
    
    ## Input all into dB with label
    try:
        destination = 'wyeth_samples'
        df.to_sql(destination, sqlengine, index=False, if_exists = 'append')
    
    except:
        print ("Could not load into dB")
        print(traceback.format_exc())

    ## Generate valid and duplicate and summary

    try:
        valid_df = df.drop_duplicates('mobileno')
        valid_df = valid_df.loc[valid_df['status'] == 'Valid']

        print (f'Records in DF {len(df)}')
        print (f'Records in Valid_DF {len(valid_df)}')
    except:
        print ('Error is in setting up final variables')
        print(traceback.format_exc())

    try:
        report = df[['m_date','m_product','status','postcode']]
        report['postcode_valid'] = np.where(report['postcode'].apply(len) == 5, 'Valid', 'Invalid Postcode')
        report['final_status'] = np.where(((report['status'] == "Valid") & (report['postcode_valid'] == "Valid")), "Valid", "Invalid")
        report['Duplicates'] = np.where(report['status'] == 'Duplicate Found In Last 6 Months', 1, 0)
        report['Invalid Postcode'] = np.where(report['postcode_valid'] == 'Invalid Postcode', 1, 0)

        report = pd.concat([report[['m_date','m_product','Duplicates','Invalid Postcode']],pd.get_dummies(report['final_status'])], axis = 1)
        report = report.groupby('m_product', as_index = False).agg({'m_date':'count', 'Valid' : 'sum', 'Invalid' : 'sum',
                                                                'Duplicates' : 'sum', 'Invalid Postcode' : 'sum'})
        report.rename({'m_date':'Total Records'}, axis = 1, inplace = True)
        report.Valid = report.Valid.astype(int)
        report['start_date'] = min(df['m_date'])
        report['end_date'] = max(df['m_date'])
        report = report[['start_date','end_date','m_product','Total Records', 'Valid', 'Invalid', 'Duplicates', 'Invalid Postcode']]

    except:
        print ('Summary Report Creation Failure')
        print(traceback.format_exc())

    try:
        output = str(datetime.today().date()) + '_' + 'wyeth_sample_report.xlsx'
        blob = output_bucket.blob('reports/' + output)
        date = str(datetime.today().date())

        with pd.ExcelWriter('/tmp/' + output) as writer:  
            report.to_excel(writer, sheet_name = 'Summary', index_label = 'id')
            df.to_excel(writer, sheet_name='All Data', index_label = 'id', startrow = 5)
            valid_df.to_excel(writer, sheet_name='Valid Data', index_label = 'id', startrow = 5)

        workbook = openpyxl.load_workbook('/tmp/' + output)

        ws1 = workbook["All Data"]
        start_date = min(df['m_date']).strftime('%d %B %Y')
        end_date = max(df['m_date']).strftime('%d %B %Y')
        
        ws1['A1'].value = 'This Is The Auto Generated Report For All Data'
        ws1['A2'].value = f'This Report Was Generated On : {datetime.today().date()}'
        ws1['A3'].value = f'The Report Was For The Duration Of : {start_date} until {end_date}'
        ws1['A4'].value = f'The Total Number Of Sample Requests Are : {len(df)}'
        ws1['A1'].font = openpyxl.styles.Font(bold = True)

        
        ws2 = workbook["Valid Data"]        
        start_date = min(valid_df['m_date']).strftime('%d %B %Y')
        end_date = max(valid_df['m_date']).strftime('%d %B %Y')

        ws2['A1'].value = 'This Is The Auto Generated Report For All Valid Data'
        ws2['A2'].value = f'This Report Was Generated On : {datetime.today().date()}'
        ws2['A3'].value = f'The Report Was For The Duration Of : {start_date} until {end_date}'
        ws2['A4'].value = f'The Total Number Of Valid Sample Requests Are : {len(valid_df)}'
        ws2['A1'].font = openpyxl.styles.Font(bold = True) 
        workbook.save('/tmp/' + output)     
            
   
        blob.upload_from_filename('/tmp/' + output)           


    except:
        print ('Error Generating Report')
        print(traceback.format_exc())

    ## Fulfillment Report

    try:
        output = str(datetime.today().date()) + '_' + 'wyeth_fulfillments.xlsx'
        blob = output_bucket.blob('reports/' + output)

        with pd.ExcelWriter('/tmp/' + output) as writer:  
            for product in valid_df['m_product'].unique():
                valid_df.loc[valid_df['m_product'] == product].to_excel(writer, sheet_name=product , index_label = 'id', startrow = 5)

        blob.upload_from_filename('/tmp/' + output)

    except:
        print ('Error Generating Fufillment Report')
        print(traceback.format_exc())


    ##TODO Generate three files for download

    print ("There we go, function complete.")


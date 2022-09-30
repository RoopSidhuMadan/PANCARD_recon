from pickle import TRUE
import numpy as np
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from datetime import date as d
import datetime as dt
import os
from google.oauth2 import service_account
from google.cloud import storage
from datetime import timedelta

import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

dates = d.today()
times = datetime.now()

f = open('/home/sdlreco/crons/pan_card_process/stat/stat-'+str(dates)+'.txt', 'a+')
f.close()

fa=open('/home/sdlreco/crons/pan_card_process/error/missing-'+str(dates)+'.txt', 'w')
fa.close()



def main():
    
    current_date = d.today()-timedelta(1)

    date = d.today()
    current_year = date.strftime('%Y')

    date = d.today()
    current_month = date.strftime('%m')

    date = d.today()-timedelta(5)

    current_day = date.strftime('%d')
    
    date = d.today()-timedelta(6)
    previous_day = date.strftime('%d')

    date = d.today()-timedelta(6)
    previous_date = date.strftime('%Y-%m-%d')

    date = d.today()
    current_mon = date.strftime('%b')

    date = d.today()
    current_yr = date.strftime('%y')

    project_id = 'spicemoney-dwh'
    client = bigquery.Client(project=project_id, location='asia-south1')
    
    file_exists = []
    file_exists.append(True) 
    '''
    try:
        pd.read_excel('gs://sm-prod-rpa/' + str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/NJRIpan_card_processCommissionReport/CommissionReport_'+'*.xlsx')
        file_exists.append(True)

    except:
        file_exists.append(False) 
        
        with open('/home/sdlreco/crons/pan_card_process/error/missing-'+str(dates)+'.txt', 'a+') as f:
            f.write(str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/NJRIpan_card_processCommissionReport/CommissionReport_'+'*.xlsx')
            f.write('\n')
    
    try:
        pd.read_csv('gs://sm-prod-rpa/' + str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/Think Walletpan_card_processsales_summary/sales_summary.csv')
        file_exists.append(True)

    except:
        file_exists.append(False) 
        
        with open('/home/sdlreco/crons/pan_card_process/error/missing-'+str(dates)+'.txt', 'a+') as f:
            f.write(str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/Think Walletpan_card_processsales_summary/sales_summary.csv')
            f.write('\n')
     
    try:
        pd.read_csv('gs://sm-prod-rpa/' +str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/Think Walletpan_card_processLogs/logs.csv')
        file_exists.append(True)

    except:
        file_exists.append(False) 
        
        with open('/home/sdlreco/crons/pan_card_process/error/missing-'+str(dates)+'.txt', 'a+') as f:
            f.write(str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/Think Walletpan_card_processLogs/logs.csv')
            f.write('\n')  

    try:
        pd.read_excel('gs://sm-prod-rpa/' + str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/NJRIpan_card_processTransactionReport/TransactionReport.xlsx')
        file_exists.append(True)

    except:
        file_exists.append(False) 
        
        with open('/home/sdlreco/crons/pan_card_process/error/missing-'+str(dates)+'.txt', 'a+') as f:
            f.write(str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/NJRIpan_card_processTransactionReport/TransactionReport.xlsx')
            f.write('\n')       
    '''     
        
    if False in file_exists:
      print('Files missing : Logged at -- /home/sdlreco/crons/pan_card_process/error/missing-'+str(dates)+'.txt')

    else:

        print('All files found, Processing ...')
        # bank statement
        #log file 1
        # credentials = service_account.Credentials.from_service_account_file(key_path)
        project_id = 'spicemoney-dwh'

        client = bigquery.Client( project=project_id, location='asia-south1')
        

        #---------------------------------------------------------------------------------------------------------------------
        #Loading PAYMENT_REPORT.xls into the database
        #---------------------------------------------------------------------------------------------------------------------
        print("Data movement started to ts_pan_card_payment_report_log table")
        schema = [{'name':'reference_no','type':'STRING'},
                    {'name':'application_no','type':'STRING'},
                    {'name':'trans_no','type':'STRING'},
                    {'name':'trans_date','type':'DATETIME'},
                    {'name':'vle_id','type':'STRING'},
                    {'name':'res_trans_no','type':'STRING'},
                    {'name':'res_csc_trans_no','type':'STRING'},
                    {'name':'res_status','type':'STRING'},
                    {'name':'res_msg','type':'STRING'},
                    {'name':'res_amount','type':'FLOAT'},
                    {'name':'res_other_values','type':'STRING'},
                    {'name':'dverify_trans_no','type':'STRING'},
                    {'name':'dverify_status','type':'STRING'},
                    {'name':'dverify_msg','type':'STRING'},
                    {'name':'dverify_csc_trans_no','type':'STRING'},
                    {'name':'payment_status','type':'STRING'}
                ]
                    
        #Specifying the header column            
        header_list = ['reference_no',
                        'application_no',
                        'trans_no',
                        'trans_date',
                        'vle_id',
                        'res_trans_no',
                        'res_csc_trans_no',
                        'res_status',
                        'res_msg',
                        'res_amount',
                        'res_other_values',
                        'dverify_trans_no',
                        'dverify_status',
                        'dverify_msg',
                        'dverify_csc_trans_no',
                        'payment_status'
                        ]
        
        list1= ['reference_no',
                        'application_no',
                        'trans_no',
                        'vle_id',
                        'res_trans_no',
                        'res_csc_trans_no',
                        'res_status',
                        'res_msg',
                        'res_other_values',
                        'dverify_trans_no',
                        'dverify_status',
                        'dverify_msg',
                        'dverify_csc_trans_no',
                        'payment_status']
        list2=['res_amount']
        

        print('PAYMENT_REPORT.xls')
        # Reading data from excel to dataframe            
        df = pd.read_excel('PAYMENT_REPORT.xls',skiprows=1,names=header_list,header=None,parse_dates = (['trans_date']))  
        
        df[list1]=df[list1].astype(str)
        df[list2]=df[list2].astype(float)
        
        df.to_gbq(destination_table='sm_recon.ts_pan_card_payment_report_log', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema)
        print("Data moved to ts_pan_card_payment_report_log table")
        df.to_gbq(destination_table='prod_sm_recon.prod_pan_card_payment_report_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema)
        print("Data moved to prod_pan_card_payment_report_log table")
        print("---------------------------------------------------------")
        #---------------------------------------------------------------------------------------------------------------------
        #Loading CSF_PAYMENT_REPORT.xls into the database
        #---------------------------------------------------------------------------------------------------------------------
        print("Data movement started to ts_pan_card_csf_payment_report_log table")
        schema_csf= [{'name':'reference_no','type':'STRING'},
                    {'name':'application_no','type':'STRING'},
                    {'name':'trans_no','type':'STRING'},
                    {'name':'trans_date','type':'DATETIME'},
                    {'name':'vle_id','type':'STRING'},
                    {'name':'res_trans_no','type':'STRING'},
                    {'name':'res_csc_trans_no','type':'STRING'},
                    {'name':'res_status','type':'STRING'},
                    {'name':'res_msg','type':'STRING'},
                    {'name':'res_amount','type':'FLOAT'},
                    {'name':'res_other_values','type':'STRING'},
                    {'name':'dverify_trans_no','type':'STRING'},
                    {'name':'dverify_status','type':'STRING'},
                    {'name':'dverify_msg','type':'STRING'},
                    {'name':'dverify_csc_trans_no','type':'STRING'},
                    {'name':'payment_status','type':'STRING'}
                ]
                    
        #Specifying the header column            
        header_list_csf= ['reference_no',
                        'application_no',
                        'trans_no',
                        'trans_date',
                        'vle_id',
                        'res_trans_no',
                        'res_csc_trans_no',
                        'res_status',
                        'res_msg',
                        'res_amount',
                        'res_other_values',
                        'dverify_trans_no',
                        'dverify_status',
                        'dverify_msg',
                        'dverify_csc_trans_no',
                        'payment_status'
                        ]
        
        list1_csf= ['reference_no',
                        'application_no',
                        'trans_no',
                        'vle_id',
                        'res_trans_no',
                        'res_csc_trans_no',
                        'res_status',
                        'res_msg',
                        'res_other_values',
                        'dverify_trans_no',
                        'dverify_status',
                        'dverify_msg',
                        'dverify_csc_trans_no',
                        'payment_status']
        list2_csf=['res_amount']
        

        print('CSF PAYMENT_REPORT.xls')
        # Reading data from excel to dataframe            
        df_csf = pd.read_excel('CSF_PAYMENT_REPORT.xls',skiprows=1,names=header_list_csf,header=None,parse_dates = (['trans_date']))  
        
        df_csf[list1_csf]=df_csf[list1_csf].astype(str)
        df_csf[list2_csf]=df_csf[list2_csf].astype(float)
        
        df_csf.to_gbq(destination_table='sm_recon.ts_pan_card_csf_payment_report_log', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema_csf)
        print("Data moved to ts_pan_card_csf_payment_report_log table")
        df.to_gbq(destination_table='prod_sm_recon.prod_pan_card_csf_payment_report_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_csf)
        print("Data moved to prod_pan_card_csf_payment_report_log table")
        print("---------------------------------------------------------")
        #---------------------------------------------------------------------------------------------------------------------
        #Loading CSF_PAYMENT_REPORT (1).xls into the database
        #---------------------------------------------------------------------------------------------------------------------
        print("Data movement started to ts_pan_card_csf_payment_report_1_log table")
        schema_csf1= [{'name':'reference_no','type':'STRING'},
                    {'name':'application_no','type':'STRING'},
                    {'name':'trans_no','type':'STRING'},
                    {'name':'trans_date','type':'DATETIME'},
                    {'name':'vle_id','type':'STRING'},
                    {'name':'res_trans_no','type':'STRING'},
                    {'name':'res_csc_trans_no','type':'STRING'},
                    {'name':'res_status','type':'STRING'},
                    {'name':'res_msg','type':'STRING'},
                    {'name':'res_amount','type':'FLOAT'},
                    {'name':'res_other_values','type':'STRING'},
                    {'name':'dverify_trans_no','type':'STRING'},
                    {'name':'dverify_status','type':'STRING'},
                    {'name':'dverify_msg','type':'STRING'},
                    {'name':'dverify_csc_trans_no','type':'STRING'},
                    {'name':'payment_status','type':'STRING'}
                ]
                    
        #Specifying the header column            
        header_list_csf1 = ['reference_no',
                        'application_no',
                        'trans_no',
                        'trans_date',
                        'vle_id',
                        'res_trans_no',
                        'res_csc_trans_no',
                        'res_status',
                        'res_msg',
                        'res_amount',
                        'res_other_values',
                        'dverify_trans_no',
                        'dverify_status',
                        'dverify_msg',
                        'dverify_csc_trans_no',
                        'payment_status'
                        ]
        
        list1_csf1= ['reference_no',
                        'application_no',
                        'trans_no',
                        'vle_id',
                        'res_trans_no',
                        'res_csc_trans_no',
                        'res_status',
                        'res_msg',
                        'res_other_values',
                        'dverify_trans_no',
                        'dverify_status',
                        'dverify_msg',
                        'dverify_csc_trans_no',
                        'payment_status']
        list2_csf1=['res_amount']
        

        print('CSF_PAYMENT_REPORT (1).xls')
        # Reading data from excel to dataframe            
        df_csf1 = pd.read_excel('CSF_PAYMENT_REPORT (1).xls',skiprows=1,names=header_list_csf1,header=None,parse_dates = (['trans_date']))  
        
        df_csf1[list1_csf1]=df_csf1[list1_csf1].astype(str)
        df_csf1[list2_csf1]=df_csf1[list2_csf1].astype(float)
        
        df_csf1.to_gbq(destination_table='sm_recon.ts_pan_card_csf_payment_report_1_log', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema_csf1)
        print("Data moved to ts_pan_card_csf_payment_report_1_log table")
        df.to_gbq(destination_table='prod_sm_recon.prod_pan_card_csf_payment_report_1_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_csf1)
        print("Data moved to prod_pan_card_csf_payment_report_1_log table")
        print("---------------------------------------------------------")
        #---------------------------------------------------------------------------------------------------------------------
        #Loading PAYMENT_REPORT (1).xls into the database
        #---------------------------------------------------------------------------------------------------------------------
        print("Data movement started to ts_pan_card_payment_report_1_log table")
        schema1 = [{'name':'reference_no','type':'STRING'},
                    {'name':'application_no','type':'STRING'},
                    {'name':'trans_no','type':'STRING'},
                    {'name':'trans_date','type':'DATETIME'},
                    {'name':'vle_id','type':'STRING'},
                    {'name':'res_trans_no','type':'STRING'},
                    {'name':'res_csc_trans_no','type':'STRING'},
                    {'name':'res_status','type':'STRING'},
                    {'name':'res_msg','type':'STRING'},
                    {'name':'res_amount','type':'FLOAT'},
                    {'name':'res_other_values','type':'STRING'},
                    {'name':'dverify_trans_no','type':'STRING'},
                    {'name':'dverify_status','type':'STRING'},
                    {'name':'dverify_msg','type':'STRING'},
                    {'name':'dverify_csc_trans_no','type':'STRING'},
                    {'name':'payment_status','type':'STRING'}
                ]
                    
        #Specifying the header column            
        header_list1 = ['reference_no',
                        'application_no',
                        'trans_no',
                        'trans_date',
                        'vle_id',
                        'res_trans_no',
                        'res_csc_trans_no',
                        'res_status',
                        'res_msg',
                        'res_amount',
                        'res_other_values',
                        'dverify_trans_no',
                        'dverify_status',
                        'dverify_msg',
                        'dverify_csc_trans_no',
                        'payment_status'
                        ]
        
        list1_1= ['reference_no',
                        'application_no',
                        'trans_no',
                        'vle_id',
                        'res_trans_no',
                        'res_csc_trans_no',
                        'res_status',
                        'res_msg',
                        'res_other_values',
                        'dverify_trans_no',
                        'dverify_status',
                        'dverify_msg',
                        'dverify_csc_trans_no',
                        'payment_status']
        list2_1=['res_amount']
        

        print('PAYMENT_REPORT (1).xls')
        # Reading data from excel to dataframe            
        df1 = pd.read_excel('PAYMENT_REPORT (1).xls',skiprows=1,names=header_list1,header=None,parse_dates = (['trans_date']))  
        
        df1[list1_1]=df1[list1_1].astype(str)
        df1[list2_1]=df1[list2_1].astype(float)
        
        df1.to_gbq(destination_table='sm_recon.ts_pan_card_payment_report_1_log', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema1)
        print("Data moved to ts_pan_card_payment_report_1_log table")
        df.to_gbq(destination_table='prod_sm_recon.prod_pan_card_payment_report_1_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema1)
        print("Data moved to prod_pan_card_payment_report_1_log table")
        print("---------------------------------------------------------")
        

        #---------------------------------------------------------------------------------------------------------------------
        #--PANCARD_Wallet_Debit_vs_Spice_vs_UTILogs
        #---------------------------------------------------------------------------------------------------------------------
        

        sql_query="""
        select Transaction_Date,Transaction_Id,Wallet_Id,SDL_Trans_Amount_DEBIT,SDL_Trans_Amount_CREDIT,Net_Wallet_Amt
        ,Client_Id,Pancard_Trans_Amnt,Pancard_Trans_Status,Net_Wallet_Amt-Pancard_Trans_Amnt as DIFF_WALLET_vs_DETAILED_LOG,
        Res_Status, 
        coalesce(Uti_Success_Res_Amount,0) AS Uti_Success_Res_Amount,
        Pancard_Trans_Amnt-Uti_Success_Res_Amount as DIFF_PANCARD_DETAIL_AMNT_vs_UTI_SUCCESS,
        failure_res_trans_no as UTI_FAILED_TRANS_NO
        from 
        (select Transaction_Date,Transaction_Id,Wallet_Id,SDL_Trans_Amount_DEBIT,SDL_Trans_Amount_CREDIT,SDL_Trans_Amount_DEBIT-SDL_Trans_Amount_CREDIT as Net_Wallet_Amt
        ,Client_Id,trans_amt as PANCARD_TRANS_AMNT,status as PANCARD_TRANS_Status,
        res_status as Res_Status,res_amount as UTI_Success_Res_Amount,failure_res_trans_no
        from 
        (
        select date(trans_date) as Transaction_Date ,trans_id as Transaction_Id,wallet_id as Wallet_Id,sum(trans_amt) as SDL_Trans_Amount_DEBIT,0 as SDL_Trans_Amount_CREDIT from prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='DEBIT'  group by Transaction_Date,Transaction_Id,wallet_id
        UNION ALL
        select date(trans_date) as Transaction_Date ,trans_id as Transaction_Id,wallet_id as Wallet_Id,0 as SDL_Trans_Amount_DEBIT,sum(trans_amt) as SDL_Trans_Amount_CREDIT from prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='CREDIT'  group by Transaction_Date,Transaction_Id,wallet_id
        )
        LEFT OUTER JOIN
        (
            select client_id,trans_amt,status,spice_trans_id from prod_dwh.b2b_pan_card where date(log_date_time)= "2022-09-13" and status='DEDUCTED'
            )
            ON spice_trans_id= Transaction_Id
            LEFT OUTER JOIN
            (
            select date(trans_date) as UTI_Sucess_trans_date,res_csc_trans_no,res_trans_no,vle_id,res_amount,res_status from `sm_recon.ts_pan_card_payment_report_log` where date(trans_date)="2022-09-13" 
            UNION ALL
            select date(trans_date) as UTI_Sucess_trans_date,res_csc_trans_no,res_trans_no,vle_id,res_amount,res_status from `sm_recon.ts_pan_card_csf_payment_report_log` where date(trans_date)="2022-09-13" 
            )
            ON Transaction_Id=res_csc_trans_no
            LEFT OUTER JOIN
            (
            select date(trans_date) as UTI_Failure_trans_date,res_csc_trans_no as failure_trans_id,res_trans_no as failure_res_trans_no,vle_id as failure_vle_id,res_amount as failure_res_amount,res_status as failure_res_status from `sm_recon.ts_pan_card_payment_report_1_log` where date(trans_date)="2022-09-13" 
            UNION ALL
            select date(trans_date) as UTI_Failure_trans_date,res_csc_trans_no,res_trans_no,vle_id,res_amount,res_status from `sm_recon.ts_pan_card_csf_payment_report_1_log` where date(trans_date)="2022-09-13"

            )
            ON failure_trans_id=Transaction_Id
            )
        
        """
        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_pancard_wallet_debit_vs_spice_vs_utilogs', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_pancard_wallet_debit_vs_spice_vs_utilogs', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()


        #---------------------------------------------------------------------------------------------------------------------
        #--PANCARD_Limit_Detail
        #---------------------------------------------------------------------------------------------------------------------
        

        sql_query="""
        select '2022-09-13' as recon_date,"Pan Card" as Service,"Utiitsl" as Aggregator,Min_Amount_Transaction_Id,MIN_TRANSACTION_AMOUNT,Max_Amount_Transaction_Id,MAX_TRANSACTION_AMOUNT,Number_of_Transaction_Agent_Wise,Transaction_Amt_Limit_Per_Day from 
        (
        select trans_id as Min_Amount_Transaction_Id,trans_amt as MIN_TRANSACTION_AMOUNT  from
        (
            select * from prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='DEBIT' and trans_amt=(select MIN(trans_amt) from prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='DEBIT' ) limit 1
        )
        )as tw_t1,
        (
        select trans_id as Max_Amount_Transaction_Id,trans_amt as MAX_TRANSACTION_AMOUNT,trans_date from
        (
            select * from prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='DEBIT' and trans_amt=(select MAX(trans_amt) from prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='DEBIT' ) limit 1
        )
        )as tw_t2,
        (
        select Max(Number_of_Clients) as Number_of_Transaction_Agent_Wise from
        (
        select count(*) as Number_of_Clients from prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='DEBIT' group by wallet_id
        )

        )as tw_t3,
        (
        select max(amount_total) as Transaction_Amt_Limit_Per_Day from 
        (
        select sum(trans_amt) as amount_total,wallet_id from  prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='DEBIT' group by wallet_id
        )
        ) as tw_t4
        
        """
        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_pancard_limit_detail', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_pancard_limit_detail', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

        #---------------------------------------------------------------------------------------------------------------------
        #--Internal File Summary -PANCARD_Wallet_Trans_Summary
        #---------------------------------------------------------------------------------------------------------------------
        

        sql_query="""
        select sum(trans_amt) as SumOf_Trans_Amount,trans_type, DATE(trans_date) as Trans_Date,comments as Comments from prod_dwh.wallet_trans  where  date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='DEBIT'
        group by DATE(trans_date) ,comments,trans_type
        
        """
        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_pancard_wallet_trans_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_pancard_wallet_trans_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

         #---------------------------------------------------------------------------------------------------------------------
        #--Internal File Summary -PANCARD_DETAIL_LOG_Summary
        #---------------------------------------------------------------------------------------------------------------------
        

        sql_query="""
         select date(log_date_time) as Log_Date,trans_mode,sum(trans_amt) as Trans_Amnt,status from prod_dwh.b2b_pan_card where date(log_date_time)= "2022-09-13" group by Log_Date,trans_mode,status
        
        """
        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_pancard_detail_log_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_pancard_detail_log_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

         #---------------------------------------------------------------------------------------------------------------------
        #--External File Summary -PANCARD_PAYMENT_REPORT_SUMMARY
        #---------------------------------------------------------------------------------------------------------------------
        

        sql_query="""
         select date(trans_date) as trans_date,res_status,sum(res_amount) as res_amount from `sm_recon.ts_pan_card_payment_report_log` where date(trans_date)="2022-09-13"  group by trans_date,res_status
        """
        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_pancard_payment_report_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_pancard_payment_report_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

        #---------------------------------------------------------------------------------------------------------------------
        #--External File Summary -PANCARD_CSF_PAYMENT_REPORT_SUMMARY
        #---------------------------------------------------------------------------------------------------------------------
        

        sql_query="""
         select date(trans_date) as trans_date,res_status,sum(res_amount) as res_amount 
         from `sm_recon.ts_pan_card_csf_payment_report_log`  where date(trans_date)="2022-09-13"  group by trans_date,res_status

        """
        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_pancard_csf_payment_report_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_pancard_csf_payment_report_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

         #---------------------------------------------------------------------------------------------------------------------
        #--External File Summary -PANCARD_PAYMENT_REPORT_1_SUMMARY
        #---------------------------------------------------------------------------------------------------------------------
        

        sql_query="""
         select date(trans_date) as trans_date,payment_status,count(*) as Record_Count from `sm_recon.ts_pan_card_payment_report_1_log` where payment_status='Payment Failure' and date(trans_date)="2022-09-13" group by trans_date,payment_status
        UNION ALL
        select date(trans_date) as trans_date,payment_status,count(*) as Record_Count from `sm_recon.ts_pan_card_payment_report_1_log` where payment_status='Payment Refunded due to Incomplete Application' and date(trans_date)="2022-09-13" group by trans_date,payment_status

        """
        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_pancard_payment_report_1_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_pancard_payment_report_1_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

         #---------------------------------------------------------------------------------------------------------------------
        #--External File Summary -PANCARD_CSF_PAYMENT_REPORT_1_SUMMARY
        #---------------------------------------------------------------------------------------------------------------------
        

        sql_query="""
         select date(trans_date) as trans_date,payment_status,count(*) as Record_Count from  `sm_recon.ts_pan_card_csf_payment_report_1_log` where payment_status='Payment Failure' and date(trans_date)="2022-09-13" group by trans_date,payment_status
        UNION ALL
        select date(trans_date) as trans_date,payment_status,count(*) as Record_Count from `sm_recon.ts_pan_card_csf_payment_report_1_log` where payment_status='Payment Refunded due to Incomplete Application' and date(trans_date)="2022-09-13" group by trans_date,payment_status

        """
        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_pancard_csf_payment_report_1_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_pancard_csf_payment_report_1_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

         #---------------------------------------------------------------------------------------------------------------------
        #--External File Summary -PANCARD_RECON_TRACKER
        #---------------------------------------------------------------------------------------------------------------------
        

        sql_query="""
         select Transaction_Date,Count_Pan_NewAPL_SDL_Successful,Amount_PAN_NewAPL_Deducted_From_Wallet_SDL,Count_Pan_ChangeAPL_SDL_Successful,Amount_PAN_ChangeAPL_Deducted_From_Wallet_SDL, Count_Pan_New_UTI_Successful ,Txn_Amount_PAN_New_UTI_Successful,Count_Pan_Change_UTI_Successful,  Txn_Amount_PAN_Change_UTI_Successful,Diff_Count_SDL_vs_UTI_NewAPL,Diff_Amount_SDL_vs_UTI_NewAPL,
        Diff_Count_SDL_vs_UTI_ChangedAPL,Diff_Amount_SDL_vs_UTI_ChangedAPL, Count_of_Txns_to_be_settled_New,
        Amount_of_Txns_to_be_settled_New,Count_of_Txns_to_be_settled_Changed,Amount_of_Txns_to_be_settled_Changed,
        SDL_Upfront_Commission_New,SDL_Upfront_Commission_Changed
        TDS_at_5Percent_On_SDL_Commission_New,TDS_at_5Percent_On_SDL_Commission_Changed,
        Amount_of_Txns_to_be_settled_New-SDL_Upfront_Commission_New+TDS_at_5Percent_On_SDL_Commission_New as Net_Amount_to_be_settled_with_UTI_New,
        Amount_of_Txns_to_be_settled_Changed-SDL_Upfront_Commission_Changed+TDS_at_5Percent_On_SDL_Commission_Changed as Net_Amount_to_be_settled_with_UTI_Changed
        from 
        (select Transaction_Date,Count_Pan_NewAPL_SDL_Successful,Amount_PAN_NewAPL_Deducted_From_Wallet_SDL,Count_Pan_ChangeAPL_SDL_Successful,Amount_PAN_ChangeAPL_Deducted_From_Wallet_SDL, Count_Pan_New_UTI_Successful ,Txn_Amount_PAN_New_UTI_Successful,Count_Pan_Change_UTI_Successful,  Txn_Amount_PAN_Change_UTI_Successful,Diff_Count_SDL_vs_UTI_NewAPL,Diff_Amount_SDL_vs_UTI_NewAPL,
        Diff_Count_SDL_vs_UTI_ChangedAPL,Diff_Amount_SDL_vs_UTI_ChangedAPL, Count_of_Txns_to_be_settled_New,
        Amount_of_Txns_to_be_settled_New,Count_of_Txns_to_be_settled_Changed,Amount_of_Txns_to_be_settled_Changed,
        SDL_Upfront_Commission_New,SDL_Upfront_Commission_Changed,
        0.05* SDL_Upfront_Commission_New  as TDS_at_5Percent_On_SDL_Commission_New,
        0.05* SDL_Upfront_Commission_Changed  as TDS_at_5Percent_On_SDL_Commission_Changed
        from 
        (select Transaction_Date,Count_Pan_NewAPL_SDL_Successful,Amount_PAN_NewAPL_Deducted_From_Wallet_SDL,Count_Pan_ChangeAPL_SDL_Successful,Amount_PAN_ChangeAPL_Deducted_From_Wallet_SDL, Count_Pan_New_UTI_Successful ,Txn_Amount_PAN_New_UTI_Successful,Count_Pan_Change_UTI_Successful,  Txn_Amount_PAN_Change_UTI_Successful,Diff_Count_SDL_vs_UTI_NewAPL,Diff_Amount_SDL_vs_UTI_NewAPL,
        Diff_Count_SDL_vs_UTI_ChangedAPL,Diff_Amount_SDL_vs_UTI_ChangedAPL, Count_of_Txns_to_be_settled_New,
        Amount_of_Txns_to_be_settled_New,Count_of_Txns_to_be_settled_Changed,Amount_of_Txns_to_be_settled_Changed,
        Count_of_Txns_to_be_settled_New*14.5 as SDL_Upfront_Commission_New,
        Count_of_Txns_to_be_settled_Changed*14.5 as SDL_Upfront_Commission_Changed
        from 

        (select Transaction_Date,Count_Pan_NewAPL_SDL_Successful,Amount_PAN_NewAPL_Deducted_From_Wallet_SDL,Count_Pan_ChangeAPL_SDL_Successful,Amount_PAN_ChangeAPL_Deducted_From_Wallet_SDL, Count_Pan_New_UTI_Successful ,Txn_Amount_PAN_New_UTI_Successful,Count_Pan_Change_UTI_Successful,  Txn_Amount_PAN_Change_UTI_Successful,
        Count_Pan_NewAPL_SDL_Successful-Count_Pan_New_UTI_Successful as Diff_Count_SDL_vs_UTI_NewAPL,
        Amount_PAN_NewAPL_Deducted_From_Wallet_SDL-Txn_Amount_PAN_New_UTI_Successful as Diff_Amount_SDL_vs_UTI_NewAPL,
        Count_Pan_ChangeAPL_SDL_Successful-Count_Pan_Change_UTI_Successful as Diff_Count_SDL_vs_UTI_ChangedAPL,	
        Amount_PAN_ChangeAPL_Deducted_From_Wallet_SDL-Txn_Amount_PAN_Change_UTI_Successful as Diff_Amount_SDL_vs_UTI_ChangedAPL,
        Count_Pan_New_UTI_Successful	as Count_of_Txns_to_be_settled_New,
        Txn_Amount_PAN_New_UTI_Successful as Amount_of_Txns_to_be_settled_New, 
        Count_Pan_Change_UTI_Successful as Count_of_Txns_to_be_settled_Changed,
        Txn_Amount_PAN_Change_UTI_Successful as Amount_of_Txns_to_be_settled_Changed 	
        from 
        (
        select sum(SDL_Trans_Amount_DEBIT-SDL_Trans_Amount_CREDIT)  as Amount_PAN_NewAPL_Deducted_From_Wallet_SDL
        ,count(*) as Count_Pan_NewAPL_SDL_Successful,Transaction_Date from 
        (
        select res_csc_trans_no,Transaction_Id,SDL_Trans_Amount_DEBIT,SDL_Trans_Amount_CREDIT,Transaction_Date
        from 
        (select date(trans_date) as Transaction_Date ,trans_id as Transaction_Id,wallet_id as Wallet_Id,sum(trans_amt) as SDL_Trans_Amount_DEBIT,0 as SDL_Trans_Amount_CREDIT from prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='DEBIT'  group by Transaction_Date,Transaction_Id,wallet_id
        UNION ALL
        select date(trans_date) as Transaction_Date ,trans_id as Transaction_Id,wallet_id as Wallet_Id,0 as SDL_Trans_Amount_DEBIT,sum(trans_amt) as SDL_Trans_Amount_CREDIT from prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='CREDIT'  group by Transaction_Date,Transaction_Id,wallet_id
        )
        LEFT OUTER JOIN
        (
        select date(trans_date) as UTI_Sucess_trans_date,res_csc_trans_no,res_trans_no,vle_id,res_amount,res_status from `sm_recon.ts_pan_card_payment_report_log` where date(trans_date)="2022-09-13"  
        UNION ALL
        select date(trans_date) as UTI_Failure_trans_date,res_csc_trans_no as failure_trans_id,res_trans_no as failure_res_trans_no,vle_id as failure_vle_id,res_amount as failure_res_amount,res_status as failure_res_status from `sm_recon.ts_pan_card_payment_report_1_log` where date(trans_date)="2022-09-13" 
        )
        ON Transaction_Id=res_csc_trans_no
        where res_csc_trans_no is not null
        ) group by Transaction_Date
        ) as B_C,
        (
        select sum(SDL_Trans_Amount_DEBIT-SDL_Trans_Amount_CREDIT)  as Amount_PAN_ChangeAPL_Deducted_From_Wallet_SDL
        ,count(*) as Count_Pan_ChangeAPL_SDL_Successful from 
        (
        select res_csc_trans_no,Transaction_Id,SDL_Trans_Amount_DEBIT,SDL_Trans_Amount_CREDIT
        from 
        (select date(trans_date) as Transaction_Date ,trans_id as Transaction_Id,wallet_id as Wallet_Id,sum(trans_amt) as SDL_Trans_Amount_DEBIT,0 as SDL_Trans_Amount_CREDIT from prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='DEBIT'  group by Transaction_Date,Transaction_Id,wallet_id
        UNION ALL
        select date(trans_date) as Transaction_Date ,trans_id as Transaction_Id,wallet_id as Wallet_Id,0 as SDL_Trans_Amount_DEBIT,sum(trans_amt) as SDL_Trans_Amount_CREDIT from prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='CREDIT'  group by Transaction_Date,Transaction_Id,wallet_id
        )
        LEFT OUTER JOIN
        (
        select date(trans_date) as UTI_Sucess_trans_date,res_csc_trans_no,res_trans_no,vle_id,res_amount,res_status from `sm_recon.ts_pan_card_csf_payment_report_log` where date(trans_date)="2022-09-13"   
        UNION ALL
        select date(trans_date) as UTI_Failure_trans_date,res_csc_trans_no,res_trans_no,vle_id,res_amount,res_status from `sm_recon.ts_pan_card_csf_payment_report_1_log` where date(trans_date)="2022-09-13"
        )
        ON Transaction_Id=res_csc_trans_no
        where res_csc_trans_no is not null
        )
        )as D_E,
        (
        select sum(SDL_Trans_Amount_DEBIT-SDL_Trans_Amount_CREDIT)  as Txn_Amount_PAN_New_UTI_Successful
        ,count(*) as Count_Pan_New_UTI_Successful from 
        (
        select res_csc_trans_no,Transaction_Id,SDL_Trans_Amount_DEBIT,SDL_Trans_Amount_CREDIT,Transaction_Date
        from 
        (select date(trans_date) as Transaction_Date ,trans_id as Transaction_Id,wallet_id as Wallet_Id,sum(trans_amt) as SDL_Trans_Amount_DEBIT,0 as SDL_Trans_Amount_CREDIT from prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='DEBIT'  group by Transaction_Date,Transaction_Id,wallet_id
        UNION ALL
        select date(trans_date) as Transaction_Date ,trans_id as Transaction_Id,wallet_id as Wallet_Id,0 as SDL_Trans_Amount_DEBIT,sum(trans_amt) as SDL_Trans_Amount_CREDIT from prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='CREDIT'  group by Transaction_Date,Transaction_Id,wallet_id
        )
        LEFT OUTER JOIN
        (
        select client_id,trans_amt,status,spice_trans_id from prod_dwh.b2b_pan_card where date(log_date_time)= "2022-09-13" and status='DEDUCTED'
        )
        ON spice_trans_id= Transaction_Id
        LEFT OUTER JOIN
        (
        select date(trans_date) as UTI_Sucess_trans_date,res_csc_trans_no,res_trans_no,vle_id,res_amount,res_status from `sm_recon.ts_pan_card_payment_report_log` where date(trans_date)="2022-09-13" 
        )
        ON Transaction_Id=res_csc_trans_no
        where res_csc_trans_no is not null
        ) group by Transaction_Date
        ) as F_G,
        (

        select sum(SDL_Trans_Amount_DEBIT-SDL_Trans_Amount_CREDIT)  as Txn_Amount_PAN_Change_UTI_Successful
        ,count(*) as Count_Pan_Change_UTI_Successful from 
        (
        select res_csc_trans_no,Transaction_Id,SDL_Trans_Amount_DEBIT,SDL_Trans_Amount_CREDIT,Transaction_Date
        from 
        (select date(trans_date) as Transaction_Date ,trans_id as Transaction_Id,wallet_id as Wallet_Id,sum(trans_amt) as SDL_Trans_Amount_DEBIT,0 as SDL_Trans_Amount_CREDIT from prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='DEBIT'  group by Transaction_Date,Transaction_Id,wallet_id
        UNION ALL
        select date(trans_date) as Transaction_Date ,trans_id as Transaction_Id,wallet_id as Wallet_Id,0 as SDL_Trans_Amount_DEBIT,sum(trans_amt) as SDL_Trans_Amount_CREDIT from prod_dwh.wallet_trans  where date(trans_date)="2022-09-13" and comments in ('Pancard Transaction','Pancard Transaction Reversal') and trans_type='CREDIT'  group by Transaction_Date,Transaction_Id,wallet_id
        )
        LEFT OUTER JOIN
        (
        select client_id,trans_amt,status,spice_trans_id from prod_dwh.b2b_pan_card where date(log_date_time)= "2022-09-13" and status='DEDUCTED'
        )
        ON spice_trans_id= Transaction_Id
        LEFT OUTER JOIN
        (
        select date(trans_date) as UTI_Failure_trans_date,res_csc_trans_no,res_trans_no,vle_id,res_amount,res_status from `sm_recon.ts_pan_card_csf_payment_report_log` where date(trans_date)="2022-09-13"

        )
        ON Transaction_Id=res_csc_trans_no
        where res_csc_trans_no is not null
        ) group by Transaction_Date

        ) as H_I
        )
        ))
        """
        job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_pancard_recon_tracker', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
        job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_pancard_recon_tracker', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

        query_job = client.query(sql_query, job_config=job_config)
        query_job = client.query(sql_query, job_config=job_config2)

        results = query_job.result()

        print("---------------------------------------------------------------")
		
	   
        print('Recon Success for {} at {}'.format(dates, times))
        with open('/home/sdlreco/crons/pan_card_process/stat/stat-'+str(dates)+'.txt', 'w') as f:
            f.write('1')
            f.close()

	#driver

import sys
sys.path.insert(0, '/home/sdlreco/crons/smarten/')
import payload as smarten

pan_card_process = []

with open('/home/sdlreco/crons/pan_card_process/stat/stat-'+str(dates)+'.txt', 'r') as f:
    lines = f.read().splitlines()
    f.close()
if('1' in lines):
    print('Tried at {}, but reco already done !!'.format(times))
else:
    main()
    for ids in pan_card_process:

        smarten.payload(ids)
    print('Refreshed : pan_card_process')
    


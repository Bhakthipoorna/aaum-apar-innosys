def FootageAvailability(ServerAddress,UserName,Password,DatabaseName,df):    
    
    import pandas as pd
    import numpy as np
    from datetime import datetime
    import pymssql
    
    df=df[['Ticket Id','McuId','Transaction date','Transaction time']]
    df=df[~df['McuId'].isnull()]
    #df['Transaction time']=pd.to_datetime(df['Transaction time'])
    #df['Transaction time']=df['Transaction time'].dt.time
    df['transaction_date_time'] = df.apply(lambda r : pd.datetime.combine(r['Transaction date'],r['Transaction time']),1)
    
    df['McuId']=df['McuId'].astype('object')
    df['Transaction date']=df['Transaction date'].astype('object')
    df['Transaction time']=df['Transaction time'].astype('object')
    df['transaction_date_time']=df['transaction_date_time'].astype('object')
    df.rename(columns={'Transaction date':'Transaction_date','Transaction time':'Transaction_time'},inplace=True)
    df=df.reset_index()
    
    conn = pymssql.connect(server=ServerAddress, user=UserName, password=Password, database=DatabaseName)
    cur = conn.cursor()

    conn2=pymssql.connect(host='188.42.97.40',user='aaum_db_usr',password='nfm!DBj!Pu#hZmgThxAoZe$E',database='AAUM_Analytics_DB')
    cur2=conn2.cursor()


    def fetch_and_transform(McuId,Transaction_date,transaction_date_time):

        df_video=pd.read_sql("select VboxId,CameraId,FileCount,RemoteFileCount,AtTime from V3tblCameraRecordingCount where VboxId ='{}' and CAST(AtTime AS DATE)='{}'".format(McuId,Transaction_date),conn)
        df_current_status=pd.read_sql("select count(*) from lbtEvents Where McuId='{}' AND (Msg='NC-30' or Msg='NC-30 (Mains Power Failure)') AND ClosedOn IS NULL".format(McuId),conn)

        df_video=df_video[df_video['CameraId'].isin(['0702','0702R'])]
        df_video=df_video.sort_values('AtTime')
        df_video.drop_duplicates(['VboxId','CameraId'],keep='last',inplace=True)
        df_video['FileCount'].fillna(0,inplace=True)
        df_video['RemoteFileCount'].fillna(0,inplace=True)
        if len(df_video)>1:
            df_video=df_video.groupby(['VboxId']).sum()
            df_video=df_video.reset_index()
        else:
            pass

        df_video['TotalFile']=df_video['FileCount']+df_video['RemoteFileCount']
        df_video['Availability']=((df_video['TotalFile']/1000)*100).round(2)
        df_video['Availability']=df_video['Availability'].clip(0,100)
        availability = df_video['Availability'].sum()

        if df_current_status.iloc[0][0]==0:
            Current_status='Online'
        else:
            Current_status='Offline'

        if availability>99:
            df_output=pd.DataFrame([[TicketId,McuId,Transaction_date,Transaction_time,'Available',np.nan,Current_status]],columns=['TicketId','McuId','Transaction Date','Transaction Time','Video Status','Reason','Current Status'])
        elif availability>1 and availability<=99:
            df_events=pd.read_sql("select count(*) from lbtEvents Where McuId='{}' AND (Msg='NC-30' or Msg='NC-30 (Mains Power Failure)') AND ClosedOn>='{}' AND AtTime<='{}' ".format(McuId,transaction_date_time,transaction_date_time),conn)
            if df_events.iloc[0][0]!=0:
                df_output=pd.DataFrame([[TicketId,McuId,Transaction_date,Transaction_time,'Not Available',"Offline",Current_status]],columns=['TicketId','McuId','Transaction Date','Transaction Time','Video Status','Reason','Current Status'])
            else:


                df_cam_issues=pd.read_sql("select count(*) from Camera_Issues where McuId ='{}' and (OpenedOn<='{}' AND (ClosedOn>='{}' OR ClosedOn IS NULL)) AND CameraId='0702'".format(McuId,transaction_date_time,transaction_date_time),conn2)
                if df_cam_issues.iloc[0][0]!=0:
                    df_output=pd.DataFrame([[TicketId,McuId,Transaction_date,Transaction_time,'Not Available',"Camera Issue",Current_status]],columns=['TicketId','McuId','Transaction Date','Transaction Time','Video Status','Reason','Current Status'])
                else:

                    df_hdd_issues=pd.read_sql("select count(*) from HDD_Issues where McuId ='{}' and (OpenedOn<='{}' AND (ClosedOn>='{}' OR ClosedOn IS NULL))".format(McuId,transaction_date_time,transaction_date_time),conn2)
                    if df_hdd_issues.iloc[0][0]!=0:
                        df_output=pd.DataFrame([[TicketId,McuId,Transaction_date,Transaction_time,'Available','HDD Issue',Current_status]],columns=['TicketId','McuId','Transaction Date','Transaction Time','Video Status','Reason','Current Status'])
                    else:
                        df_output=pd.DataFrame([[TicketId,McuId,Transaction_date,Transaction_time,'Available','No HDD Issue',Current_status]],columns=['TicketId','McuId','Transaction Date','Transaction Time','Video Status','Reason','Current Status'])

        else:
            df_events=pd.read_sql("select count(*) from lbtEvents Where McuId='{}' AND (Msg='NC-30' or Msg='NC-30 (Mains Power Failure)') AND ClosedOn>='{}' AND AtTime<='{}' ".format(McuId,transaction_date_time,transaction_date_time),conn)
            if df_events.iloc[0][0]!=0:
                df_output=pd.DataFrame([[TicketId,McuId,Transaction_date,Transaction_time,'Not Available',"Offline",Current_status]],columns=['TicketId','McuId','Transaction Date','Transaction Time','Video Status','Reason','Current Status'])
            else:
                df_cam_issues=pd.read_sql("select count(*) from Camera_Issues where McuId ='{}' and (OpenedOn<='{}' AND (ClosedOn>='{}' OR ClosedOn IS NULL)) AND CameraId='0702'".format(McuId,transaction_date_time,transaction_date_time),conn2)
                if df_cam_issues.iloc[0][0]!=0:
                    df_output=pd.DataFrame([[TicketId,McuId,Transaction_date,Transaction_time,'Not Available',"Camera Issue",Current_status]],columns=['TicketId','McuId','Transaction Date','Transaction Time','Video Status','Reason','Current Status'])
                else:

                    df_hdd_issues=pd.read_sql("select count(*) from HDD_Issues where McuId ='{}' and (OpenedOn<='{}' AND (ClosedOn>='{}' OR ClosedOn IS NULL))".format(McuId,transaction_date_time,transaction_date_time),conn2)
                    if df_hdd_issues.iloc[0][0]!=0:
                        df_output=pd.DataFrame([[TicketId,McuId,Transaction_date,Transaction_time,'Not Available','HDD Issue',Current_status]],columns=['TicketId','McuId','Transaction Date','Transaction Time','Video Status','Reason','Current Status'])
                    else:
                        df_output=pd.DataFrame([[TicketId,McuId,Transaction_date,Transaction_time,'Not Available','Online - No HDD Issue',Current_status]],columns=['TicketId','McuId','Transaction Date','Transaction Time','Video Status','Reason','Current Status'])

        return df_output
    
    df_output1=pd.DataFrame()
    for i in range(0,len(df)):
        McuId=df['McuId'].iloc[i]
        McuId=int(McuId)
        McuId=str(McuId)
        Transaction_date=df['Transaction_date'].iloc[i]
        Transaction_time=df['Transaction_time'].iloc[i]
        transaction_date_time=df['transaction_date_time'].iloc[i]
        TicketId=df['Ticket Id'].iloc[i]
        print(i)
        df_output=fetch_and_transform(McuId,Transaction_date,transaction_date_time)
        df_output1=pd.concat([df_output1,df_output],ignore_index=False)
        
    conn.close()
    cur.close()
    
    conn2.close()
    cur2.close()
        
    return df_output1
df_output1=FootageAvailability(ServerAddress,UserName,Password,DatabaseName,df)
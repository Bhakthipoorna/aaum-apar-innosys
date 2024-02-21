def slarealtime(ServerAddress,DatabaseName,UserName,Password):
    import time
    import pandas as pd
    import pymssql
    import numpy as np
    import datetime
    import pytz 
    import warnings
    IST = pytz.timezone('Asia/Kolkata')
    warnings.filterwarnings('ignore')
    def fetch_realtime(ServerAddress,DatabaseName,UserName,Password):
        conn = pymssql.connect(server=ServerAddress, user=UserName, password=Password, database=DatabaseName)  
        cursor = conn.cursor()
        event = pd.read_sql("SELECT EventId,McuId,Msg,EventType,AtTime,OpenedOn,ClosedOn FROM lbtEvents WHERE (AtTime >=getdate()-1) AND ClosedOn IS NULL AND (EventType='Critical' or EventType='SOS') AND (Msg!='NC-30' AND Msg!='NC-30(Mains Power Failure)') ", conn)
        remarks = pd.read_sql("SELECT EventId,UpdatedON,Remarks FROM lbtEventsRemarks WHERE (UpdatedON >=getdate()-1) ",conn)
        sitemas = pd.read_sql("SELECT a.s_Name,a.McuId,a.s_Addr_State,b.BankName,b.BankCode from lbtSiteMas as a,lbtBankMas as b WHERE a.BankCode=b.BankCode AND a.qstatus='True' AND b.qstatus='True' ", conn)
        event=pd.merge(event,remarks,on='EventId',how='left')
        cursor.close
        conn.close()
        return event, sitemas

    def transform_realtime(Events,Sitemas):
        
        Events.drop_duplicates(['McuId','EventType','AtTime'],keep='first',inplace=True)
    
        SiteMaster2=Sitemas[(Sitemas['BankName']!='Test') & (Sitemas['BankName']!='TEST BED')]
        SiteMaster2['s_Addr_State']=((SiteMaster2['s_Addr_State'].str.lower()).str.title()).str.strip()
        bankname=SiteMaster2['BankName'][0]
        Events_df=pd.merge(Events,SiteMaster2,on='McuId',how='inner')
        df_sos=Events_df[Events_df['EventType']=="SOS"]
        df_critical=Events_df[Events_df['EventType']=="Critical"]
        Events_df['Acknowledgement']=np.where(Events_df['UpdatedON'].isnull(),'Not Acknowledged','Acknowledged')
        Events_acknowledged=Events_df[Events_df['Acknowledgement']=='Acknowledged']
        Events_not_acknowledged=Events_df[Events_df['Acknowledgement']=='Not Acknowledged']
        Events_acknowledged['UpdatedON']=pd.to_datetime(Events_acknowledged['UpdatedON'])
        Events_acknowledged['AcknowledgedAge']=(((((pd.to_datetime(Events_acknowledged['UpdatedON']))-(pd.to_datetime(Events_acknowledged['OpenedOn']))).astype('timedelta64[s]')))/60)/round(2)

        Events_df=pd.concat([Events_acknowledged, Events_not_acknowledged], ignore_index=True)     
    
        Events_df['CurrentTime']=(datetime.datetime.now(IST)).strftime("%Y-%m-%d %H:%M:%S")
        Events_df['AgeInMinutes']=(((((pd.to_datetime(Events_df['CurrentTime']))-(pd.to_datetime(Events_df['OpenedOn']))).astype('timedelta64[s]')))/60).round(2)
    

        conditionsOnAckTime = [
                    (Events_df['EventType'] =='SOS') & (Events_df['AgeInMinutes'] >=4) & (Events_df['AgeInMinutes']<5), 
                    (Events_df['EventType'] =='Critical') & (Events_df['AgeInMinutes'] >=8) & (Events_df['AgeInMinutes']<10),
                            ]
        valuesOnAck = ['TicketToAck','TicketToAck']
        Events_df['TicketsToAcknowledge'] = np.select(conditionsOnAckTime, valuesOnAck)

        conditionsOnCloseTime = [
                    (Events_df['EventType'] =='SOS') & (Events_df['AgeInMinutes'] >=24) & (Events_df['AgeInMinutes']<30),
                    (Events_df['EventType'] =='Critical') & (Events_df['AgeInMinutes'] >=48) & (Events_df['AgeInMinutes']<60) & (Events_df['Msg'] !='NC-30'),
                        ]
        valuesOnClose = ['TicketToClose','TicketToClose']
        Events_df['TicketsToClose'] = np.select(conditionsOnCloseTime, valuesOnClose)
        if DatabaseName=='InnoculateCMS':
            Events_hdfc_df=Events_df[Events_df['BankName']=='CMS-HDFC']
            Events_icici_df=Events_df[Events_df['BankName']=='CMS-ICICI']
            df_hdfc_sos=Events_hdfc_df[Events_hdfc_df['EventType']=="SOS"]
            df_hdfc_critical=Events_hdfc_df[Events_hdfc_df['EventType']=="Critical"]
            df_icici_sos=Events_icici_df[Events_icici_df['EventType']=="SOS"]
            df_icici_critical=Events_icici_df[Events_icici_df['EventType']=="Critical"]
            sos_hdfc_acknowlegdeged=Events_df[(Events_df['EventType']=='SOS') & (Events_df['Acknowledgement']=='Acknowledged') & (Events_df['BankName']=='CMS-ICICI')]
            sos_icici_acknowlegdeged=Events_df[(Events_df['EventType']=='SOS') & (Events_df['Acknowledgement']=='Acknowledged') & (Events_df['BankName']=='CMS-ICICI')]
            sos_hdfc_not_acknowlegdeged=Events_df[(Events_df['EventType']=='SOS') & (Events_df['Acknowledgement']=='Not Acknowledged') & (Events_df['BankName']=='CMS-HDFC')]
            sos_icici_not_acknowlegdeged=Events_df[(Events_df['EventType']=='SOS') & (Events_df['Acknowledgement']=='Not Acknowledged') & (Events_df['BankName']=='CMS-ICICI')]
            critical_hdfc_acknowlegdeged=Events_df[(Events_df['EventType']=='Critical') & (Events_df['Acknowledgement']=='Acknowledged') & (Events_df['BankName']=='CMS-HDFC')]
            critical_icici_acknowlegdeged=Events_df[(Events_df['EventType']=='Critical') & (Events_df['Acknowledgement']=='Acknowledged') & (Events_df['BankName']=='CMS-ICICI')]
            critical_hdfc_not_acknowlegdeged=Events_df[(Events_df['EventType']=='Critical') & (Events_df['Acknowledgement']=='Not Acknowledged') & (Events_df['BankName']=='CMS-HDFC')]
            critical_icici_not_acknowlegdeged=Events_df[(Events_df['EventType']=='Critical') & (Events_df['Acknowledgement']=='Not Acknowledged') & (Events_df['BankName']=='CMS-ICICI')]
            sos_hdfc_ack_time_vio=Events_df[(Events_df['EventType']=='SOS') & (Events_df['Acknowledgement']=='Acknowledged') & (Events_df['BankName']=='CMS-HDFC') & (Events_df['AcknowledgedAge']>5)]
            sos_icici_ack_time_vio=Events_df[(Events_df['EventType']=='SOS') & (Events_df['Acknowledgement']=='Acknowledged') & (Events_df['BankName']=='CMS-ICICI') & (Events_df['AcknowledgedAge']>5)]
            critical_hdfc_ack_time_vio=Events_df[(Events_df['EventType']=='Critical') & (Events_df['Acknowledgement']=='Acknowledged') & (Events_df['BankName']=='CMS-HDFC') & (Events_df['AcknowledgedAge']>10)]
            critical_icici_ack_time_vio=Events_df[(Events_df['EventType']=='Critical') & (Events_df['Acknowledgement']=='Acknowledged') & (Events_df['BankName']=='CMS-ICICI') & (Events_df['AcknowledgedAge']>10)]
            Events_hdfc_df=Events_hdfc_df[['BankName','EventId','McuId','s_Name','EventType','Msg','OpenedOn','UpdatedON','Acknowledgement','AgeInMinutes','AcknowledgedAge','TicketsToAcknowledge','TicketsToClose']]
            Events_icici_df=Events_icici_df[['BankName','EventId','McuId','s_Name','EventType','Msg','OpenedOn','UpdatedON','Acknowledgement','AgeInMinutes','AcknowledgedAge','TicketsToAcknowledge','TicketsToClose']]
            df_hdfc_SOSTicketsToClose=Events_hdfc_df[(Events_hdfc_df['TicketsToClose']=='TicketToClose') & (Events_hdfc_df['EventType']=='SOS')]
            df_icici_SOSTicketsToClose=Events_icici_df[(['TicketsToClose']=='TicketToClose') & (Events_icici_df['EventType']=='SOS')]
            df_hdfc_CriticalTicketsToClose=Events_hdfc_df[(Events_hdfc_df['TicketsToClose']=='TicketToClose') & (Events_df['EventType']=='Critical')]
            df_icici_CriticalTicketsToClose=Events_icici_df[(Events_icici_df['TicketsToClose']=='TicketToClose') & (Events_icici_df['EventType']=='Critical')]
            df_hdfc_CriticalTicketToAck=Events_hdfc_df[(Events_hdfc_df['TicketsToAcknowledge']=='TicketToAck') & (Events_hdfc_df['EventType']=='Critical')]
            df_icici_CriticalTicketToAck=Events_icici_df[(Events_icici_df['TicketsToAcknowledge']=='TicketToAck') & (Events_icici_df['EventType']=='Critical')]
            df_hdfc_SOSTicketToAck=Events_hdfc_df[(Events_hdfc_df['TicketsToAcknowledge']=='TicketToAck') & (Events_hdfc_df['EventType']=='SOS')]
            df_icici_SOSTicketToAck=Events_icici_df[(Events_icici_df['TicketsToAcknowledge']=='TicketToAck') & (Events_icici_df['EventType']=='SOS')]
            df_hdfc_tickets_near_viloation=Events_hdfc_df[(Events_hdfc_df['TicketsToAcknowledge']!='0') | (Events_hdfc_df['TicketsToClose']!='0')]
            df_icici_tickets_near_viloation=Events_icici_df[(Events_icici_df['TicketsToAcknowledge']!='0') | (Events_icici_df['TicketsToClose']!='0')]
            df_hdfc_tickets_near_viloation=df_hdfc_tickets_near_viloation[['BankName','s_Name','McuId','EventId','EventType','Msg','OpenedOn','AgeInMinutes','TicketsToAcknowledge','TicketsToClose']]
            df_icici_tickets_near_viloation=df_icici_tickets_near_viloation[['BankName','s_Name','McuId','EventId','EventType','Msg','OpenedOn','AgeInMinutes','TicketsToAcknowledge','TicketsToClose']]            
            df_hdfc_tickets_near_viloation['TicketsRequiredAction']=df_hdfc_tickets_near_viloation['TicketsToAcknowledge']
            df_hdfc_tickets_near_viloation['TicketsRequiredAction'].replace('0',"TicketToClose",inplace=True)
            
            df_hdfc_tickets_near_viloation=df_hdfc_tickets_near_viloation[['BankName','s_Name','McuId','EventId','EventType','Msg','OpenedOn','AgeInMinutes','TicketsRequiredAction']]
            df_icici_tickets_near_viloation['TicketsRequiredAction']=df_icici_tickets_near_viloation['TicketsToAcknowledge']
            df_icici_tickets_near_viloation['TicketsRequiredAction'].replace('0',"TicketToClose",inplace=True)
            df_icici_tickets_near_viloation=df_icici_tickets_near_viloation[['BankName','s_Name','McuId','EventId','EventType','Msg','OpenedOn','AgeInMinutes','TicketsRequiredAction']]
            df_tickets_near_viloation=pd.merge(df_hdfc_tickets_near_viloation,df_icici_tickets_near_viloation,how='outer') 

            try:
                sos_hdfc_to_close=len(df_hdfc_SOSTicketsToClose)
            except ZeroDivisionError:
                sos_hdfc_to_close=0
            try:
                critical_hdfc_to_close=len(df_hdfc_CriticalTicketsToClose)
            except ZeroDivisionError:
                critical_hdfc_to_close=0
            try:
                critical_hdfc_to_ack=len(df_hdfc_CriticalTicketToAck)
            except ZeroDivisionError:
                critical_hdfc_to_ack=0
            try:
                sos_hdfc_to_ack=len(df_hdfc_SOSTicketToAck)
            except ZeroDivisionError:
                sos_hdfc_to_ack=0
    
            try:
                sos_icici_to_close=len(df_icici_SOSTicketsToClose)
            except ZeroDivisionError:
                sos_icici_to_close=0
            try:
                critical_icici_to_close=len(df_icici_CriticalTicketsToClose)
            except ZeroDivisionError:
                critical_icici_to_close=0
            try:
                critical_icici_to_ack=len(df_icici_CriticalTicketToAck)
            except ZeroDivisionError:
                critical_icici_to_ack=0
            try:
                sos_icici_to_ack=len(df_icici_SOSTicketToAck)
            except ZeroDivisionError:
                sos_icici_to_ack=0
    
            df_columns=['TotalTickets','Acknowledged','NotAcknowledged','ViolatedAckTime','NearingAckViolation','NearingCloingViolation']
            df_indexes=['Critical','SOS']
            df_hdfc_count=(pd.DataFrame([[len(df_hdfc_critical),len(critical_hdfc_acknowlegdeged),len(critical_hdfc_not_acknowlegdeged),len(critical_hdfc_ack_time_vio),critical_hdfc_to_ack,critical_hdfc_to_close],[len(df_hdfc_sos),len(sos_hdfc_acknowlegdeged),len(sos_hdfc_not_acknowlegdeged),len(sos_hdfc_ack_time_vio),sos_hdfc_to_ack,sos_hdfc_to_close]],columns=df_columns,index=df_indexes)).round(2)
            df_hdfc_count['EventType']=['Critical','SOS']
            df_hdfc_count.reset_index(drop=True, inplace=True)
            df_hdfc_count['BankName']='CMS-HDFC'
            df_hdfc_count=df_hdfc_count[['BankName','EventType','TotalTickets','Acknowledged','NotAcknowledged','ViolatedAckTime','NearingAckViolation','NearingCloingViolation']]
            df_icici_count=(pd.DataFrame([[len(df_icici_critical),len(critical_icici_acknowlegdeged),len(critical_icici_not_acknowlegdeged),len(critical_icici_ack_time_vio),critical_icici_to_ack,critical_icici_to_close],[len(df_icici_sos),len(sos_icici_acknowlegdeged),len(sos_icici_not_acknowlegdeged),len(sos_icici_ack_time_vio),sos_icici_to_ack,sos_icici_to_close]],columns=df_columns,index=df_indexes)).round(2)
            df_icici_count['EventType']=['Critical','SOS']
            df_icici_count.reset_index(drop=True, inplace=True)
            df_icici_count['BankName']='CMS-ICICI'
            df_icici_count=df_icici_count[['BankName','EventType','TotalTickets','Acknowledged','NotAcknowledged','ViolatedAckTime','NearingAckViolation','NearingCloingViolation']]
            df_count=pd.merge(df_hdfc_count,df_icici_count,how='outer') 
            return df_tickets_near_viloation,df_count,bankname 
                                               
        else:
            sos_acknowlegdeged=Events_df[(Events_df['EventType']=='SOS') & (Events_df['Acknowledgement']=='Acknowledged')]
            sos_not_acknowlegdeged=Events_df[(Events_df['EventType']=='SOS') & (Events_df['Acknowledgement']=='Not Acknowledged')]
            critical_acknowlegdeged=Events_df[(Events_df['EventType']=='Critical') & (Events_df['Acknowledgement']=='Acknowledged')]
            critical_not_acknowlegdeged=Events_df[(Events_df['EventType']=='Critical') & (Events_df['Acknowledgement']=='Not Acknowledged')]

            sos_ack_time_vio=Events_df[(Events_df['EventType']=='SOS') & (Events_df['Acknowledgement']=='Acknowledged') & (Events_df['AcknowledgedAge']>5)]
            critical_ack_time_vio=Events_df[(Events_df['EventType']=='Critical') & (Events_df['Acknowledgement']=='Acknowledged') & (Events_df['AcknowledgedAge']>10)]

            Events_df=Events_df[['BankName','EventId','McuId','s_Name','EventType','Msg','OpenedOn','UpdatedON','Acknowledgement','AgeInMinutes','AcknowledgedAge','TicketsToAcknowledge','TicketsToClose']]
 
            df_SOSTicketsToClose=Events_df[(Events_df['TicketsToClose']=='TicketToClose') & (Events_df['EventType']=='SOS')]
            df_CriticalTicketsToClose=Events_df[(Events_df['TicketsToClose']=='TicketToClose') & (Events_df['EventType']=='Critical')]
            df_CriticalTicketToAck=Events_df[(Events_df['TicketsToAcknowledge']=='TicketToAck') & (Events_df['EventType']=='Critical')]
            df_SOSTicketToAck=Events_df[(Events_df['TicketsToAcknowledge']=='TicketToAck') & (Events_df['EventType']=='SOS')]
            df_tickets_near_viloation=Events_df[(Events_df['TicketsToAcknowledge']!='0') | (Events_df['TicketsToClose']!='0')]
            df_tickets_near_viloation=df_tickets_near_viloation[['BankName','s_Name','McuId','EventId','EventType','Msg','OpenedOn','AgeInMinutes','TicketsToAcknowledge','TicketsToClose']]
            df_tickets_near_viloation['TicketsRequiredAction']=df_tickets_near_viloation['TicketsToAcknowledge']
            df_tickets_near_viloation['TicketsRequiredAction'].replace('0',"TicketToClose",inplace=True)
            df_tickets_near_viloation=df_tickets_near_viloation[['BankName','s_Name','McuId','EventId','EventType','Msg','OpenedOn','AgeInMinutes','TicketsRequiredAction']]
        
            try:
                sos_to_close=len(df_SOSTicketsToClose)
            except ZeroDivisionError:
                sos_to_close=0
            try:
                critical_to_close=len(df_CriticalTicketsToClose)
            except ZeroDivisionError:
                critical_to_close=0
            try:
                critical_to_ack=len(df_CriticalTicketToAck)
            except ZeroDivisionError:
                critical_to_ack=0
            try:
                sos_to_ack=len(df_SOSTicketToAck)
            except ZeroDivisionError:
                sos_to_ack=0
            df_columns=['TotalTickets','Acknowledged','NotAcknowledged','ViolatedAckTime','NearingAckViolation','NearingCloingViolation']
            df_indexes=['Critical','SOS']
            df_count=(pd.DataFrame([[len(df_critical),len(critical_acknowlegdeged),len(critical_not_acknowlegdeged),len(critical_ack_time_vio),critical_to_ack,critical_to_close],[len(df_sos),len(sos_acknowlegdeged),len(sos_not_acknowlegdeged),len(sos_ack_time_vio),sos_to_ack,sos_to_close]],columns=df_columns,index=df_indexes)).round(2)
            df_count['EventType']=['Critical','SOS']
            df_count.reset_index(drop=True, inplace=True)
            df_count['BankName']=bankname
            df_count=df_count[['BankName','EventType','TotalTickets','Acknowledged','NotAcknowledged','ViolatedAckTime','NearingAckViolation','NearingCloingViolation']]
            return df_tickets_near_viloation,df_count, bankname
        return df_tickets_near_viloation,df_count,bankname                                    
     
    def pushdata(df_tickets_near_viloation,df_count,bankname):
        conn2=pymssql.connect(host='188.42.97.40',user='aaum_db_usr',password='nfm!DBj!Pu#hZmgThxAoZe$E',database='AAUM_Analytics_DB')
        cur2=conn2.cursor()
        if DatabaseName=='InnoculateCMS':
            clear_data1="DELETE FROM RealTimeSlaViolatedTicket WHERE BankName = 'CMS-HDFC' "
            cur2.execute(clear_data1)
            
            clear_data2="DELETE FROM Realtimeticketscount WHERE BankName = 'CMS-HDFC' "
            cur2.execute(clear_data2)
            clear_data3="DELETE FROM RealTimeSlaViolatedTicket WHERE BankName = 'CMS-ICICI' "
            cur2.execute(clear_data3)
            clear_data4="DELETE FROM Realtimeticketscount WHERE BankName = 'CMS-ICICI' "
            cur2.execute(clear_data4)
            query4 = "INSERT INTO dbo.RealTimeSlaViolatedTicket VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            realtime_vio_data = tuple(map(tuple, df_tickets_near_viloation.values))
            cur2.executemany(query4, realtime_vio_data)
            query4 = "INSERT INTO dbo.Realtimeticketscount VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            realtime_tickets_data = tuple(map(tuple, df_count.values))
            cur2.executemany(query4, realtime_tickets_data)
            cur2.close()
            conn2.commit()
            conn2.close() 
        else:
            clear_data1="DELETE FROM RealTimeSlaViolatedTicket WHERE BankName = '{}' ".format(bankname)
            cur2.execute(clear_data1)
            clear_data2="DELETE FROM Realtimeticketscount WHERE BankName = '{}' ".format(bankname)
            cur2.execute(clear_data2)
            query4 = "INSERT INTO dbo.RealTimeSlaViolatedTicket VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            realtime_vio_data = tuple(map(tuple, df_tickets_near_viloation.values))
            cur2.executemany(query4, realtime_vio_data)
            query4 = "INSERT INTO dbo.Realtimeticketscount VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            realtime_tickets_data = tuple(map(tuple, df_count.values))
            cur2.executemany(query4, realtime_tickets_data)
            cur2.close()
            conn2.commit()
            conn2.close()        
     
      
    Events, Sitemas = fetch_realtime(ServerAddress,DatabaseName,UserName,Password)
    df_tickets_near_viloation,df_count,bankname= transform_realtime(Events,Sitemas) 
    pushdata(df_tickets_near_viloation,df_count,bankname)
    
    return df_tickets_near_viloation,df_count
df_tickets_near_viloation,df_count= slarealtime(ServerAddress,DatabaseName,UserName,Password)

def slaviolation(ServerAddress,UserName,Password,DatabaseName):
    import pytz
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta
    IST = pytz.timezone('Asia/Kolkata')
    import pandas as pd
    import pymssql
    import numpy as np
    import warnings
    warnings.filterwarnings('ignore')
    def fetch(ServerAddress,UserName,Password,DatabaseName):
        today=datetime.now(IST).date().strftime("%Y-%m-%d")
        yesterday=datetime.strftime(datetime.now(IST) - timedelta(1), '%Y-%m-%d')
        conn = pymssql.connect(server=ServerAddress,user=UserName, password=Password, database=DatabaseName)  
        cursor = conn.cursor() 
        events = pd.read_sql("SELECT EventId,McuId,Msg,EventType,AtTime,OpenedOn,ClosedOn FROM lbtEvents WHERE OpenedOn BETWEEN '{}' AND '{}' AND (EventType='Critical' or EventType='SOS') AND (Msg!='NC-30' AND Msg!='NC-30 (Mains Power Failure)' AND  LOWER(ClosedBy)!='system' AND (Msg!='Movement in Lobby for more than 3 Mins')) ".format(yesterday,today),conn)
        remarks = pd.read_sql("select EventId,UpdatedON,Remarks from lbtEventsRemarks  where UpdatedON BETWEEN '{}' AND '{}' AND (TktStatus=1) ".format(yesterday,today), conn)
        remarks = remarks.sort_values(['EventId','UpdatedON'])
        remarks = remarks.drop_duplicates(['EventId'],keep='first')
        events=pd.merge(events,remarks,on='EventId',how='left')
        siteMas=pd.read_sql("select a.s_Name,a.McuId,a.s_Addr_State,b.BankName,a.BankCode from lbtSiteMas as a, lbtBankMas as b WHERE a.BankCode=b.BankCode AND a.qstatus='True' AND b.qstatus='True' AND a.s_Status=1 ",conn)
        cursor.close
        conn.close()
        return events,siteMas


    def transformation(events,SiteMas):

        if len(events)==0:
            df_Events = pd.DataFrame(columns=['BankName','s_Name','McuId','EventType','AtTime','OpenedOn','UpdatedON','ClosedOn','AckPenalty','ClosePenalty'])
            df_Violated_Tickets=pd.DataFrame(columns=['BankName','s_Name','TotalTickets','TotalSOSTickets','TotalCriticalTickets','SOSAckViolation','SOSCloseViolations','CriticalAckVioloation','CriticalCloseViolations'])
            SiteMaster2=SiteMas[(SiteMas['BankName']!='Test') & (SiteMas['s_Name']!='TEST BED')]
            SiteMaster2['s_Addr_State']=((SiteMaster2['s_Addr_State'].str.lower()).str.title()).str.strip()
            
            df_sla_violation= pd.DataFrame(columns=['Date','BankName','s_Name','McuId','EventId','EventType','AtTime','OpenedOn','UpdatedON','ClosedOn','AckPenalty','ClosePenalty'])

        else:  
            events.drop_duplicates(['McuId','Msg','AtTime'],keep='first',inplace=True)

            SiteMaster2=SiteMas[(SiteMas['BankName']!='Test') & (SiteMas['s_Name']!='TEST BED')]
            SiteMaster2['s_Addr_State']=((SiteMaster2['s_Addr_State'].str.lower()).str.title()).str.strip()

            Events=pd.merge(events,SiteMaster2,on='McuId',how='inner')
            Events['ClosedOn']=pd.to_datetime(Events['ClosedOn'])
            Events['OpenedOn']=pd.to_datetime(Events['OpenedOn'])
            Events['UpdatedON']=pd.to_datetime(Events['UpdatedON'])
            Events['TicketAge']=(Events['ClosedOn']-Events['OpenedOn']).astype('timedelta64[m]')
            Events["TicketAge"].fillna((datetime.now()-Events['OpenedOn']).astype('timedelta64[m]'),inplace=True)

            Events['Acknowledgement']=np.where(Events['UpdatedON'].isnull(),'Not Acknowledged','Acknowledged')
            Events_Notacknowledged=Events[Events['Acknowledgement']=='Not Acknowledged']
            Events_acknowledged=Events[Events['Acknowledgement']=='Acknowledged']
            Events_acknowledged['UpdatedON']=pd.to_datetime(Events_acknowledged['UpdatedON'])
            Events_acknowledged['AcknowledgedAge']=(Events_acknowledged['UpdatedON']-Events_acknowledged['OpenedOn']).astype('timedelta64[m]')
            df_Events=pd.concat([Events_acknowledged, Events_Notacknowledged], ignore_index=True)


            conditionsOnAck = [((df_Events['EventType'] =='SOS') & (df_Events['Acknowledgement']=='Not Acknowledged') & (df_Events['TicketAge'] >= 5)),((df_Events['EventType'] =='SOS') & (df_Events['Acknowledgement']=='Acknowledged') & (df_Events['AcknowledgedAge'] >=5)) |((df_Events['EventType'] =='SOS') & (df_Events['Acknowledgement']=='Not Acknowledged') & (df_Events['TicketAge'] >= 5)),((df_Events['EventType'] =='Critical') & (df_Events['Acknowledgement']=='Not Acknowledged') & (df_Events['TicketAge'] >= 10)),((df_Events['EventType'] =='Critical') & (df_Events['Acknowledgement']=='Acknowledged') & (df_Events['AcknowledgedAge'] >=10)) |((df_Events['EventType'] =='Critical') & (df_Events['Acknowledgement']=='Not Acknowledged') & (df_Events['TicketAge'] >= 10))]
            valuesOnAck = ['SOSNoAckVio', 'SOSAckViolations','CriticalNoAckVio','CriticalAckViolations']
            df_Events['AckPenalty'] = np.select(conditionsOnAck, valuesOnAck)

            conditionsOnClose = [(df_Events['EventType'] =='SOS') & (df_Events['TicketAge'] >=30) ,(df_Events['EventType'] =='Critical') & (df_Events['TicketAge'] >=60)]

            valuesOnClose = ['SOSCloseViolations','CriticalCloseViolations']
            df_Events['ClosePenalty'] = np.select(conditionsOnClose, valuesOnClose)

            df_sla_violation=df_Events[['BankName','s_Name','McuId','EventId','EventType','AtTime','OpenedOn','UpdatedON','ClosedOn','AckPenalty','ClosePenalty']]
            df_sla_violation=df_sla_violation[(df_sla_violation['AckPenalty']!='0') | (df_sla_violation['ClosePenalty']!='0')]
            df_sla_violation['Date']=pd.to_datetime(datetime.strftime(datetime.now(IST) - timedelta(1), '%Y-%m-%d'))
            
            df_sla_violation=df_sla_violation[['Date','BankName','s_Name','McuId','EventId','EventType','AtTime','OpenedOn','UpdatedON','ClosedOn','AckPenalty','ClosePenalty']]

    
            df_events=df_Events[['BankName','s_Name','McuId','EventType','AckPenalty','ClosePenalty']]
            df_sos=df_events[df_events['EventType']=='SOS']
            df_critical=df_events[df_events['EventType']=='Critical']
    
            df_sos_tickets=df_sos.groupby(['BankName','s_Name']).count().reset_index()
            df_sos_tickets.rename(columns={'McuId': 'TotalSOSTickets'},inplace=True)
            df_sos_tickets=df_sos_tickets[['BankName','s_Name','TotalSOSTickets']]
    
            df_critical_tickets=df_critical.groupby(['BankName','s_Name']).count().reset_index()
            df_critical_tickets.rename(columns={'McuId': 'TotalCriticalTickets'},inplace=True)
            df_critical_tickets=df_critical_tickets[['BankName','s_Name','TotalCriticalTickets']]

            df_total_tickets=pd.merge(df_sos_tickets,df_critical_tickets,on=['BankName','s_Name'],how='outer').fillna(0)
            df_bankwise=df_events.sort_values('BankName')
            df_close_vio=pd.crosstab(df_bankwise['s_Name'],df_bankwise['ClosePenalty'])
            close_vio_column_names=['SOSCloseViolations','CriticalCloseViolations']
            for i in close_vio_column_names:
                if i not in df_close_vio:
                    df_close_vio[i]=0
            df_Ack_vio=pd.crosstab(df_bankwise['s_Name'],df_bankwise['AckPenalty'])
            ack_vio_column_names=['SOSNoAckVio','SOSAckViolations','CriticalNoAckVio','CriticalAckViolations']
            for i in ack_vio_column_names:
                if i not in df_Ack_vio:
                    df_Ack_vio[i]=0
            
            df_Ack_vio['SOSAckViolation']=df_Ack_vio['SOSAckViolations']+df_Ack_vio['SOSNoAckVio']
            df_Ack_vio['CriticalAckVioloation']=df_Ack_vio['CriticalAckViolations']+df_Ack_vio['CriticalNoAckVio']
            df_Ack_vio.drop(['CriticalAckViolations','CriticalNoAckVio','SOSAckViolations','SOSNoAckVio'],axis=1,inplace=True)
            df_Violation=pd.merge(df_Ack_vio,df_close_vio,on=['s_Name'],how='inner').reset_index()
            df_Violation.drop(['0_x','0_y'],axis=1,inplace=True)
            df_SOS_Violation=df_Violation[['s_Name','SOSAckViolation','SOSCloseViolations']]
            df_Critical_Violation=df_Violation[['s_Name','CriticalAckVioloation','CriticalCloseViolations']]
            df_All_Violation=pd.merge(df_SOS_Violation,df_Critical_Violation,on=['s_Name'],how='outer').fillna(0)
            df_Violated_Tickets=pd.merge(df_total_tickets,df_All_Violation,on=['s_Name'],how='left')
            df_Violated_Tickets['TotalTickets']=df_Violated_Tickets['TotalSOSTickets']+df_Violated_Tickets['TotalCriticalTickets']
            df_Violated_Tickets=df_Violated_Tickets[['BankName','s_Name','TotalTickets','TotalSOSTickets','TotalCriticalTickets','SOSAckViolation','SOSCloseViolations','CriticalAckVioloation','CriticalCloseViolations']]
        return df_Violated_Tickets,SiteMaster2,df_sla_violation

    def output(df_violated_Tickets,sitemas2,df_sla_violation):
        if len(df_violated_Tickets)==0:
            today=datetime.now(IST).date().strftime("%Y-%m-%d")
            yesterday=pd.to_datetime(datetime.strftime(datetime.now(IST) - timedelta(1), '%Y-%m-%d'))
            bank=sitemas2['BankName'][0]
            
            df_monitoring_sla_violation_bankwise =pd.DataFrame(columns=['Date','BankName','TotalTickets','TotalSOSTickets','TotalCriticalTickets','SOSAckViolation','SOSCloseViolations','CriticalAckVioloation','CriticalCloseViolations'])
            df_monitoring_sla_violation_bankwise=df_monitoring_sla_violation_bankwise.append({'Date':yesterday,'BankName':bank,'TotalTickets':0,'TotalSOSTickets':0,'TotalCriticalTickets':0,'SOSAckViolation':0,'SOSCloseViolations':0,'CriticalAckVioloation':0,'CriticalCloseViolations':0},ignore_index=True)
            df_monitoring_sla_violation_bankwise[['TotalTickets','TotalSOSTickets','TotalCriticalTickets','SOSAckViolation','SOSCloseViolations','CriticalAckVioloation','CriticalCloseViolations']]=df_monitoring_sla_violation_bankwise[['TotalTickets','TotalSOSTickets','TotalCriticalTickets','SOSAckViolation','SOSCloseViolations','CriticalAckVioloation','CriticalCloseViolations']].astype('int64')
            df_monitoring_sla_violation_sitewise1 =pd.DataFrame(columns=['Date','BankName','s_Name','TotalTickets','TotalSOSTickets','TotalCriticalTickets','SOSAckViolation','SOSCloseViolations','CriticalAckVioloation','CriticalCloseViolations'])
            df_monitoring_sla_violation_sitewise=pd.merge(df_monitoring_sla_violation_sitewise1,sitemas2,left_on=['s_Name'],right_on=['s_Name'],how='outer')
            df_monitoring_sla_violation_sitewise.rename(columns={'BankName_y':'BankName','s_Name':'SiteName'},inplace=True)
            df_monitoring_sla_violation_sitewise.drop(['BankName_x','BankCode','s_Addr_State','McuId'],axis=1,inplace=True)
            df_monitoring_sla_violation_sitewise =df_monitoring_sla_violation_sitewise[['Date','BankName','SiteName','TotalTickets','TotalSOSTickets','TotalCriticalTickets','SOSAckViolation','SOSCloseViolations','CriticalAckVioloation','CriticalCloseViolations']]
            df_monitoring_sla_violation_sitewise['Date']=yesterday
            df_monitoring_sla_violation_sitewise[['TotalTickets','TotalSOSTickets','TotalCriticalTickets','SOSAckViolation','SOSCloseViolations','CriticalAckVioloation','CriticalCloseViolations']]=0
        
        else:   
            df_monitoring_sla_violation_bankwise=df_violated_Tickets.groupby('BankName').sum().reset_index()
            df_monitoring_sla_violation_bankwise['Date']=datetime.strftime(datetime.now(IST) - timedelta(1), '%Y-%m-%d')
            df_monitoring_sla_violation_bankwise['Date']=pd.to_datetime(df_monitoring_sla_violation_bankwise['Date'])
            df_monitoring_sla_violation_bankwise=df_monitoring_sla_violation_bankwise[['Date','BankName','TotalTickets','TotalSOSTickets','TotalCriticalTickets','SOSAckViolation','SOSCloseViolations','CriticalAckVioloation','CriticalCloseViolations']]
            df_monitoring_sla_violation_bankwise[['TotalTickets','TotalSOSTickets','TotalCriticalTickets','SOSAckViolation','SOSCloseViolations','CriticalAckVioloation','CriticalCloseViolations']]=df_monitoring_sla_violation_bankwise[['TotalTickets','TotalSOSTickets','TotalCriticalTickets','SOSAckViolation','SOSCloseViolations','CriticalAckVioloation','CriticalCloseViolations']].astype('int64')
            df_monitoring_sla_violation_sitewise=df_violated_Tickets.groupby('s_Name').sum().reset_index()
            df_monitoring_sla_violation_sitewise=pd.merge(df_monitoring_sla_violation_sitewise,sitemas2,on='s_Name',how='outer')
            df_monitoring_sla_violation_sitewise['Date']=datetime.strftime(datetime.now(IST) - timedelta(1), '%Y-%m-%d')
            df_monitoring_sla_violation_sitewise['Date']=pd.to_datetime(df_monitoring_sla_violation_sitewise['Date'])
            df_monitoring_sla_violation_sitewise.drop(['s_Addr_State','s_Addr_State','BankCode'],axis=1,inplace=True)
            df_monitoring_sla_violation_sitewise.fillna(0,inplace=True)
            df_monitoring_sla_violation_sitewise=df_monitoring_sla_violation_sitewise[['Date','BankName','s_Name','TotalTickets','TotalSOSTickets','TotalCriticalTickets','SOSAckViolation','SOSCloseViolations','CriticalAckVioloation','CriticalCloseViolations']]
            df_sla_violation.rename(columns={'s_Name':'SiteName'},inplace=True)
            df_monitoring_sla_violation_sitewise.rename(columns={'s_Name':'SiteName'},inplace=True)
            df_monitoring_sla_violation_sitewise[['TotalTickets','TotalSOSTickets','TotalCriticalTickets','SOSAckViolation','SOSCloseViolations','CriticalAckVioloation','CriticalCloseViolations']]=df_monitoring_sla_violation_sitewise[['TotalTickets','TotalSOSTickets','TotalCriticalTickets','SOSAckViolation','SOSCloseViolations','CriticalAckVioloation','CriticalCloseViolations']].astype('int64')
        return df_monitoring_sla_violation_bankwise,df_monitoring_sla_violation_sitewise,df_sla_violation
    
    def pushdata(df_monitoring_sla_violation_bankwise,df_monitoring_sla_violation_sitewise,df_sla_violation):
        from sqlalchemy import create_engine
        engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')        
        df_monitoring_sla_violation_bankwise.to_sql('SlaViolationBankwise',engine,if_exists='append',index=False)
        df_monitoring_sla_violation_sitewise.to_sql('SlaViolationSitewise',engine,if_exists='append',index=False)
        df_sla_violation.to_sql('TicketsViolated',engine,if_exists='append',index=False)
        
    
    events, SiteMas = fetch(ServerAddress,UserName,Password,DatabaseName)
    df_violated_Tickets,sitemas2,df_sla_violation = transformation(events,SiteMas)
    df_monitoring_sla_violation_bankwise,df_monitoring_sla_violation_sitewise,df_sla_violation=output(df_violated_Tickets,sitemas2,df_sla_violation)
    pushdata(df_monitoring_sla_violation_bankwise,df_monitoring_sla_violation_sitewise,df_sla_violation)
    return df_monitoring_sla_violation_bankwise,df_monitoring_sla_violation_sitewise,df_sla_violation

df_monitoring_sla_violation_Bankwise,df_monitoring_sla_violation_Sitewise,df_Sla_Violation=slaviolation(ServerAddress,UserName,Password,DatabaseName)   

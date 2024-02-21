ServerAddress='103.24.200.157'
UserName="aaum_support"
Password="aaum@123"
DatabaseName='InnoculateNCRLVB'

import pandas as pd
import pymssql
import numpy as np
import datetime
from datetime import datetime
import pytz
from pytz import timezone
import json
IST = pytz.timezone('Asia/Kolkata')

from datetime import datetime, timedelta
from datetime import date

Today=pd.to_datetime(datetime.strftime(datetime.now(IST), '%Y-%m-%d 00:00:00'))
prior2days=datetime.strftime(datetime.now(IST) - timedelta(2), '%Y-%m-%d 00:00:00')
prior30days=datetime.strftime(datetime.now(IST) - timedelta(30), '%Y-%m-%d 00:00:00')

conn = pymssql.connect(server=ServerAddress, user="aaum_support", password="aaum@123", database=DatabaseName)  
cursor = conn.cursor()

df_mcu_heartbeat=pd.read_sql("select * from McuHBDateTime ",conn)
siteMas=pd.read_sql("select a.s_Name,a.McuId,a.s_Addr_State,b.BankName,a.BankCode from lbtSiteMas as a, lbtBankMas as b WHERE a.BankCode=b.BankCode AND a.qstatus='True' AND b.qstatus='True' ",conn)
events = pd.read_sql("SELECT EventId,McuId,EventType,Msg,AtTime,OpenedOn,ClosedOn FROM lbtEvents WHERE ((ClosedON BETWEEN '{}' AND '{}') AND (OpenedOn < '{}') OR ClosedON IS NULL) AND (Msg='NC-30' or Msg='NC-30 (Mains Power Failure)') AND qStatus='True'".format(prior30days,Today,Today),conn)
df_sensor_item=pd.read_sql("select * from lbtSiteItemsMaster WHERE GroupNo='01' AND UnitNo='36' AND qStatus='True' ",conn)      
    


events.drop_duplicates(['McuId','EventType','AtTime'],inplace=True)

df_null_events=events[events['EventId'].isnull()]
events2=events.drop(df_null_events.index)
events_to_drop=events2[(events2['AtTime']>=Today) & (events2['ClosedOn'].isnull())]
events3=events2.drop(events_to_drop.index)
df_open_tickets=events3[events3['ClosedOn'].isnull()]
        
if DatabaseName=='InnoculateCMS':
    df_mcu_opentickets=pd.merge(df_open_tickets,df_mcu_heartbeat,left_on='McuId',right_on='McuID',how='left')
    df_mcu_opentickets=df_mcu_opentickets[df_mcu_opentickets['OpenedOn']<df_mcu_opentickets['GetDate']]
else:
    df_mcu_opentickets=pd.merge(df_open_tickets,df_mcu_heartbeat,on='McuId',how='left')
    df_mcu_opentickets=df_mcu_opentickets[df_mcu_opentickets['OpenedOn']<df_mcu_opentickets['ReceivedDate']]

df_mcu_opentickets=df_mcu_opentickets[['McuId','EventId','EventType','Msg','AtTime','OpenedOn','ClosedOn']]

events4=events3.merge(df_mcu_opentickets, how='left', indicator=True)
events4=events4[events4['_merge']=='left_only']
events4=events4[['McuId','EventId','EventType','Msg','AtTime','OpenedOn','ClosedOn']]

Events=events4.drop_duplicates(['McuId','EventType','AtTime'])

df_null=Events[Events['AtTime'].isnull()]
df_new_events=Events.drop(df_null.index)
        
df_new_events['first_day']=pd.to_datetime(prior30days)
df_new_events['last_day']=pd.to_datetime(Today)
df_new_events['ClosedOn']=df_new_events['ClosedOn'].fillna(df_new_events['last_day'])

df_new_events['ClosedOn'] =np.where(df_new_events['ClosedOn']>Today,df_new_events['last_day'],df_new_events['ClosedOn'])
df_new_events['ClosedOn']=pd.to_datetime(df_new_events['ClosedOn'], format='%Y-%m-%d %H:%M:%S')

df_new_events['AtTime']=np.where(df_new_events['AtTime']<prior30days,df_new_events['first_day'],df_new_events['AtTime'])
df_new_events['ClosedOn']=pd.to_datetime(df_new_events['ClosedOn'], format='%Y-%m-%d %H:%M:%S')

df_new_events['TicketAge']=(df_new_events['ClosedOn']-df_new_events['AtTime']).astype('timedelta64[m]')

df_new_events=pd.merge(siteMas[['BankName','s_Name','McuId']],df_new_events,how='left')
df_new_events_group=df_new_events.groupby(['BankName','s_Name','McuId']).sum()
df_new_events_group=df_new_events_group.reset_index()
df_new_events_group['OfflinePerc']=((df_new_events_group['TicketAge']/(1440*30))*100).round(2)
df_new_events_group['OnlinePerc']=100-df_new_events_group['OfflinePerc']
df_online=df_new_events_group[['BankName','s_Name','McuId','OnlinePerc']]
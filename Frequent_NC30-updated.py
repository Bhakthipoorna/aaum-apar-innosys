def nc30_pattern(ServerAddress,UserName,Password,DatabaseName):

	import pandas as pd
	import pymssql
	import numpy as np
	from datetime import datetime, timedelta
	import pytz
	from pytz import timezone
	IST = pytz.timezone('Asia/Kolkata')
	from sqlalchemy import create_engine
	import warnings
	warnings.filterwarnings('ignore')

	def fetch(ServerAddress,UserName,Password,DatabaseName):

		today=datetime.now(IST).date().strftime("%Y-%m-%d")
		prior3days=datetime.strftime(datetime.now(IST) - timedelta(3), '%Y-%m-%d')
		prior7days=datetime.strftime(datetime.now(IST) - timedelta(7), '%Y-%m-%d')
		prior15days=datetime.strftime(datetime.now(IST) - timedelta(15), '%Y-%m-%d')
		conn = pymssql.connect(server=ServerAddress, user=UserName, password=Password, database=DatabaseName)  
		cursor = conn.cursor()
		events = pd.read_sql("SELECT EventId,McuId,Msg,AtTime,OpenedOn,ClosedOn FROM lbtEvents WHERE AtTime BETWEEN '{}' AND '{}' AND ClosedOn <'{}' AND (Msg='NC-30' or Msg='NC-30 (Mains Power Failure)') ".format(prior15days,today,today),conn)
		df_sitemas=pd.read_sql("select a.s_Name,a.McuId,a.Status,b.BankName from lbtSiteMas as a, lbtBankMas as b WHERE a.BankCode=b.BankCode AND a.s_Status=1 AND a.qstatus='True' AND b.qstatus='True' ",conn)
		cursor.close()
		conn.close()
		return events,df_sitemas,today,prior3days,prior7days


	def transformation(events,df_sitemas,today,prior3days,prior7days):

		if len(events)>=1:

			events=pd.merge(df_sitemas[['BankName','s_Name','McuId','Status']],events,on='McuId',how='left')
			events.drop_duplicates(['McuId','Msg','AtTime'],inplace=True)
			null_event=events[(events['EventId'].isnull()) & (events['AtTime'].isnull())]
			Events=events.drop(null_event.index)
			Events=Events[['BankName','s_Name','McuId','Status','EventId','Msg','AtTime','OpenedOn','ClosedOn']]

			Events['OpenedDate']=Events['AtTime'].dt.date
			Events['ClosedOn']=pd.to_datetime(Events['ClosedOn'])
			Events['AtTime']=pd.to_datetime(Events['AtTime'])
			Events['Time_online']=Events['ClosedOn'].dt.strftime('%H:%M')
			Events['Time_offline']=Events['AtTime'].dt.strftime('%H:%M')
			Events['Hour_online']=Events['ClosedOn'].dt.strftime('%H')
			Events['Hour_offline']=Events['AtTime'].dt.strftime('%H')
			Events['Offline_Week_day']=Events['AtTime'].dt.day_name()
			Events['Online_Week_day']=Events['ClosedOn'].dt.day_name()
			Events['Offline_Week_day'].replace({0:'Monday',1:'Tuesday',2:'Wednesday',3:'Thursday',4:'Friday',5:'Saturday',6:'Sunday'},inplace=True)
			Events['Online_Week_day'].replace({0:'Monday',1:'Tuesday',2:'Wednesday',3:'Thursday',4:'Friday',5:'Saturday',6:'Sunday'},inplace=True)
			df_sitemas.rename(columns={'s_Name':'SiteName'},inplace=True)
			Events.rename(columns={'s_Name':'SiteName'},inplace=True)


			df_offline_allday=Events.groupby(['BankName','SiteName','McuId','Time_offline']).size().reset_index(name="count_15days")
			df_offline_allday=df_offline_allday[df_offline_allday['count_15days']>=10]
			df_online_allday=Events.groupby(['BankName','SiteName','McuId','Time_online']).size().reset_index(name="count_15days")
			df_online_allday=df_online_allday[df_online_allday['count_15days']>=10]
			attime_last_7days=Events[Events['AtTime']>=prior7days]
			df_offline_last7days=attime_last_7days.groupby(['BankName','SiteName','McuId','Time_offline']).size().reset_index(name="count_7days")
			df_offline_last7days=df_offline_last7days[df_offline_last7days['count_7days']>=5]
			closedon_last_7days=Events[Events['ClosedOn']>=prior7days]
			df_online_last7days=closedon_last_7days.groupby(['BankName','SiteName','McuId','Time_online']).size().reset_index(name="count_7days")
			df_online_last7days=df_online_last7days[df_online_last7days['count_7days']>=5]
			attime_last_3days=Events[Events['AtTime']>=prior3days]
			df_offline_last3days=attime_last_3days.groupby(['BankName','SiteName','McuId','Time_offline']).size().reset_index(name="count_3days")
			df_offline_last3days=df_offline_last3days[df_offline_last3days['count_3days']>=2]
			closedon_last_3days=Events[Events['ClosedOn']>=prior3days]
			df_online_last3days=closedon_last_3days.groupby(['BankName','SiteName','McuId','Time_online']).size().reset_index(name="count_3days")
			df_online_last3days=df_online_last3days[df_online_last3days['count_3days']>=2]

			df_Time_offline=pd.merge(df_offline_last3days,df_offline_last7days,on=['BankName','SiteName','McuId','Time_offline']).merge(df_offline_allday,on=['BankName','SiteName','McuId','Time_offline'])
			df_Time_online=pd.merge(df_online_last3days,df_online_last7days,on=['BankName','SiteName','McuId','Time_online']).merge(df_online_allday,on=['BankName','SiteName','McuId','Time_online'])


			df_hour_offline_allday=Events.groupby(['BankName','SiteName','McuId','Hour_offline']).size().reset_index(name="count_15days")
			df_hour_offline_allday=df_hour_offline_allday[df_hour_offline_allday['count_15days']>=10]
			df_hour_online_allday=Events.groupby(['BankName','SiteName','McuId','Hour_online']).size().reset_index(name="count_15days")
			df_hour_online_allday=df_hour_online_allday[df_hour_online_allday['count_15days']>=10]
			df_hour_offline_last7days=attime_last_7days.groupby(['BankName','SiteName','McuId','Hour_offline']).size().reset_index(name="count_7days")
			df_hour_offline_last7days=df_hour_offline_last7days[df_hour_offline_last7days['count_7days']>=5]
			df_hour_online_last7days=closedon_last_7days.groupby(['BankName','SiteName','McuId','Hour_online']).size().reset_index(name="count_7days")
			df_hour_online_last7days=df_hour_online_last7days[df_hour_online_last7days['count_7days']>=5]
			df_hour_offline_last3days=attime_last_3days.groupby(['BankName','SiteName','McuId','Hour_offline']).size().reset_index(name="count_3days")
			df_hour_offline_last3days=df_hour_offline_last3days[df_hour_offline_last3days['count_3days']>=2]
			df_hour_online_last3days=closedon_last_3days.groupby(['BankName','SiteName','McuId','Hour_online']).size().reset_index(name="count_3days")
			df_hour_online_last3days=df_hour_online_last3days[df_hour_online_last3days['count_3days']>=2]

			df_hour_offline=pd.merge(df_hour_offline_last3days,df_hour_offline_last7days,on=['BankName','SiteName','McuId','Hour_offline']).merge(df_hour_offline_allday,on=['BankName','SiteName','McuId','Hour_offline'])
			df_hour_online=pd.merge(df_hour_online_last3days,df_hour_online_last7days,on=['BankName','SiteName','McuId','Hour_online']).merge(df_hour_online_allday,on=['BankName','SiteName','McuId','Hour_online'])

			Events_both=Events.groupby(['BankName','SiteName','McuId','Time_offline','Time_online']).size().reset_index(name="count_15days")
			Events_both=Events_both[Events_both['count_15days']>=10]
			Events_7days_both=attime_last_7days.groupby(['BankName','SiteName','McuId','Time_offline','Time_online']).size().reset_index(name="count_7days")
			Events_7days_both=Events_7days_both[Events_7days_both['count_7days']>=5]
			Events_3days_both=attime_last_3days.groupby(['BankName','SiteName','McuId','Time_offline','Time_online']).size().reset_index(name="count_3days")
			Events_3days_both=Events_3days_both[Events_3days_both['count_3days']>=2]

			df_both=pd.merge(Events_3days_both,Events_7days_both,on=['BankName','SiteName','McuId','Time_offline','Time_online']).merge(Events_both,on=['BankName','McuId','SiteName','Time_offline','Time_online'])
			df_both['Date']=today
			first_column = df_both.pop('Date')
			df_both.insert(0, 'Date', first_column)
			df_both=df_both[['Date','BankName','SiteName','McuId','Time_offline','Time_online','count_3days','count_7days','count_15days']]
			df_both['Remark']="Offline-Online"       

			df_only_closedon=df_Time_online[['BankName','McuId','Time_online']].merge(df_both[['BankName','McuId','Time_online']], how='left', indicator=True)
			df_only_closedon=df_only_closedon[df_only_closedon['_merge']=='left_only']
			df_only_closedon=df_only_closedon.iloc[:,df_only_closedon.columns!='_merge']
			df_Time_online=pd.merge(df_only_closedon,df_Time_online,on=['BankName','McuId','Time_online'])
			df_Time_online['Date']=today
			first_column = df_Time_online.pop('Date')
			df_Time_online.insert(0, 'Date', first_column)
			df_Time_online=df_Time_online[['Date','BankName','SiteName','McuId','Time_online','count_3days','count_7days','count_15days']]
			df_Time_online.rename(columns={'SiteName':'SiteName'},inplace=True)
			df_Time_online['Remark']="Online"

			df_only_attime=df_Time_offline[['BankName','McuId','Time_offline']].merge(df_both[['BankName','McuId','Time_offline']], how='left', indicator=True)
			df_only_attime=df_only_attime[df_only_attime['_merge']=='left_only']
			df_only_attime=df_only_attime.iloc[:,df_only_attime.columns!='_merge']
			df_Time_offline=pd.merge(df_only_attime,df_Time_offline,on=['BankName','McuId','Time_offline'])
			df_Time_offline['Date']=today
			first_column4 = df_Time_offline.pop('Date')
			df_Time_offline.insert(0, 'Date', first_column4)
			df_Time_offline=df_Time_offline[['Date','BankName','SiteName','McuId','Time_offline','count_3days','count_7days','count_15days']]
			df_Time_offline['Remark']="Offline"

			Events_hour_both=Events.groupby(['BankName','SiteName','McuId','Hour_offline','Hour_online']).size().reset_index(name="count_15days")
			Events_hour_both=Events_hour_both[Events_hour_both['count_15days']>=10]
			Events_hour_7days_both=attime_last_7days.groupby(['BankName','SiteName','McuId','Hour_offline','Hour_online']).size().reset_index(name="count_7days")
			Events_hour_7days_both=Events_hour_7days_both[Events_hour_7days_both['count_7days']>=5]
			Events_hour_3days_both=attime_last_3days.groupby(['BankName','SiteName','McuId','Hour_offline','Hour_online']).size().reset_index(name="count_3days")
			Events_hour_3days_both=Events_hour_3days_both[Events_hour_3days_both['count_3days']>=2]

			df_hour_both=pd.merge(Events_hour_3days_both,Events_hour_7days_both,on=['BankName','SiteName','McuId','Hour_offline','Hour_online']).merge(Events_hour_both,on=['BankName','McuId','SiteName','Hour_offline','Hour_online'])
			df_hour_both['Date']=today
			first_hour_column = df_hour_both.pop('Date')
			df_hour_both.insert(0, 'Date', first_hour_column)
			df_hour_both=df_hour_both[['Date','BankName','SiteName','McuId','Hour_offline','Hour_online','count_3days','count_7days','count_15days']]
			df_hour_both['Remark']="Offline-Online"       

			df_hour_only_closedon=df_hour_online[['BankName','McuId','Hour_online']].merge(df_hour_both[['BankName','McuId','Hour_online']], how='left', indicator=True)
			df_hour_only_closedon=df_hour_only_closedon[df_hour_only_closedon['_merge']=='left_only']
			df_hour_only_closedon=df_hour_only_closedon.iloc[:,df_hour_only_closedon.columns!='_merge']
			df_hour_online=pd.merge(df_hour_only_closedon,df_hour_online,on=['BankName','McuId','Hour_online'])
			df_hour_online['Date']=today
			first_column2 = df_hour_online.pop('Date')
			df_hour_online.insert(0, 'Date', first_column2)
			df_hour_online=df_hour_online[['Date','BankName','SiteName','McuId','Hour_online','count_3days','count_7days','count_15days']]
			df_hour_online['Remark']="Online"

			df_hour_only_attime=df_hour_offline[['BankName','McuId','Hour_offline']].merge(df_hour_both[['BankName','McuId','Hour_offline']], how='left', indicator=True)
			df_hour_only_attime=df_hour_only_attime[df_hour_only_attime['_merge']=='left_only']
			df_hour_only_attime=df_hour_only_attime.iloc[:,df_hour_only_attime.columns!='_merge']
			df_hour_offline=pd.merge(df_hour_only_attime,df_hour_offline,on=['BankName','McuId','Hour_offline'])
			df_hour_offline['Date']=today
			first_column5 = df_hour_offline.pop('Date')
			df_hour_offline.insert(0, 'Date', first_column5)
			df_hour_offline=df_hour_offline[['Date','BankName','SiteName','McuId','Hour_offline','count_3days','count_7days','count_15days']]
			df_hour_offline['Remark']="Offline"

			df_Time_offline['Hour_offline']=df_Time_offline['Time_offline'].str[0:2]
			df_Time_online['Hour_online']=df_Time_online['Time_online'].str[0:2]
			df_both['Hour_online']=df_both['Time_online'].str[0:2]
			df_both['Hour_offline']=df_both['Time_offline'].str[0:2]

			df_closedon=df_hour_online[['BankName','McuId','Hour_online']].merge(df_Time_online[['BankName','McuId','Hour_online']], how='left', indicator=True)
			df_closedon=df_closedon[df_closedon['_merge']=='left_only']
			df_closedon=df_closedon.iloc[:,df_closedon.columns!='_merge']
			df_hour_online=pd.merge(df_closedon,df_hour_online,on=['BankName','McuId','Hour_online'])
			df_hour_online['Date']=today
			first_column6 = df_hour_online.pop('Date')
			df_hour_online.insert(0, 'Date', first_column6)
			df_hour_online=df_hour_online[['Date','BankName','SiteName','McuId','Hour_online','count_3days','count_7days','count_15days']]
			df_hour_online['Remark']="Online"

			df_attime=df_hour_offline[['BankName','McuId','Hour_offline']].merge(df_Time_offline[['BankName','McuId','Hour_offline']], how='left', indicator=True)
			df_attime=df_attime[df_attime['_merge']=='left_only']
			df_attime=df_attime.iloc[:,df_attime.columns!='_merge']
			df_hour_offline=pd.merge(df_attime,df_hour_offline,on=['BankName','McuId','Hour_offline'])
			df_hour_offline['Date']=today
			first_column7 = df_hour_offline.pop('Date')
			df_hour_offline.insert(0, 'Date', first_column7)
			df_hour_offline=df_hour_offline[['Date','BankName','SiteName','McuId','Hour_offline','count_3days','count_7days','count_15days']]
			df_hour_offline['Remark']="Offline"

			df_attime_closedon=df_hour_both[['BankName','McuId','Hour_online','Hour_offline']].merge(df_both[['BankName','McuId','Hour_online','Hour_offline']], how='left', indicator=True)
			df_attime_closedon=df_attime_closedon[df_attime_closedon['_merge']=='left_only']
			df_attime_closedon=df_attime_closedon.iloc[:,df_attime_closedon.columns!='_merge']
			df_hour_both=pd.merge(df_attime_closedon,df_hour_both,on=['BankName','McuId','Hour_online','Hour_offline'])
			df_hour_both['Date']=today
			first_column8 = df_hour_both.pop('Date')
			df_hour_both.insert(0, 'Date', first_column8)
			df_hour_both=df_hour_both[['Date','BankName','SiteName','McuId','Hour_offline','Hour_online','count_3days','count_7days','count_15days']]
			df_hour_both['Remark']="Offline-Online"

			df_hour_online.rename(columns={'Hour_online':'Time_online'},inplace=True)
			df_hour_offline.rename(columns={'Hour_offline':'Time_offline'},inplace=True)
			df_hour_both.rename(columns={'Hour_online':'Time_online','Hour_offline':'Time_offline'},inplace=True)
			df_both=df_both[['Date','BankName','SiteName','McuId','Time_offline','Time_online','count_3days','count_7days','count_15days','Remark']]
			df_Time_offline=df_Time_offline[['Date','BankName','SiteName','McuId','Time_offline','count_3days','count_7days','count_15days','Remark']]
			df_Time_online=df_Time_online[['Date','BankName','SiteName','McuId','Time_online','count_3days','count_7days','count_15days','Remark']]

			ls1=pd.merge(df_Time_offline,df_hour_both,on=['Date','BankName','SiteName','McuId'],how='left',indicator=True)
			ls1=ls1[ls1['_merge']=='left_only']
			ls1=ls1[['Date','BankName','SiteName','McuId']]
			df_Time_offline=df_Time_offline[df_Time_offline['McuId'].isin(ls1['McuId'])]
			ls2=pd.merge(df_Time_online,df_hour_both,on=['Date','BankName','SiteName','McuId'],how='left',indicator=True)
			ls2=ls2[ls2['_merge']=='left_only']
			ls2=ls2[['Date','BankName','SiteName','McuId']]
			df_Time_online=df_Time_online[df_Time_online['McuId'].isin(ls2['McuId'])]
			ls4=pd.merge(df_hour_offline,df_both,on=['Date','BankName','SiteName','McuId'],how='left',indicator=True)
			ls4=ls4[ls4['_merge']=='left_only']
			ls4=ls4[['Date','BankName','SiteName','McuId']]
			df_hour_offline=df_hour_offline[df_hour_offline['McuId'].isin(ls4['McuId'])]
			ls5=pd.merge(df_hour_online,df_both,on=['Date','BankName','SiteName','McuId'],how='left',indicator=True)
			ls5=ls5[ls5['_merge']=='left_only']
			ls5=ls5[['Date','BankName','SiteName','McuId']]
			df_hour_online=df_hour_online[df_hour_online['McuId'].isin(ls5['McuId'])]
			df_offline_online_pattern=pd.concat([df_both,df_hour_both,df_Time_offline,df_hour_offline,df_Time_online,df_hour_online],axis=0)


			df_15daysticket_count=Events.groupby(['BankName','SiteName','McuId']).count().reset_index()
			df_15daysticket_count=df_15daysticket_count[['BankName','SiteName','McuId','EventId']]
			df_15daysticket_count.rename(columns={'EventId':'count_15days'},inplace=True)
			df_15daysticket_count=df_15daysticket_count[df_15daysticket_count['count_15days']>=20]
			df_7daysticket_count=attime_last_7days.groupby(['BankName','SiteName','McuId']).count().reset_index()
			df_7daysticket_count=df_7daysticket_count[['BankName','SiteName','McuId','EventId']]
			df_7daysticket_count.rename(columns={'EventId':'count_7days'},inplace=True)
			df_7daysticket_count=df_7daysticket_count[df_7daysticket_count['count_7days']>=10]
			df_3daysticket_count=attime_last_3days.groupby(['BankName','SiteName','McuId']).count().reset_index()
			df_3daysticket_count=df_3daysticket_count[['BankName','SiteName','McuId','EventId']]
			df_3daysticket_count.rename(columns={'EventId':'count_3days'},inplace=True)
			df_3daysticket_count=df_3daysticket_count[df_3daysticket_count['count_3days']>=5]
			df_event_count=pd.merge(df_3daysticket_count,df_7daysticket_count,on=['BankName','SiteName','McuId']).merge(df_15daysticket_count,on=['BankName','SiteName','McuId'])
			df_event_count['Date']=today
			first_column = df_event_count.pop('Date')
			df_event_count.insert(0, 'Date', first_column)
			df_event_count=df_event_count[['Date','BankName','SiteName','McuId','count_3days','count_7days','count_15days']]
			df_event_count.rename(columns={'SiteName':'SiteName'},inplace=True)
			df_event_count['Remark']="TotalTickets"


			#df_daily=Events.groupby(['McuId','OpenedDate']).count()
			#df_daily=pd.DataFrame(df_daily)
			#df_daily=df_daily.reset_index()
			#df_daily=df_daily[['McuId','OpenedDate','EventId']]
			#df_daily.rename(columns={'EventId':'count'},inplace=True)
			#df_daily2=pd.DataFrame(df_daily['McuId'].value_counts())
			#df_daily2=df_daily2.reset_index()
			#df_daily2.rename(columns={'index':'McuId','McuId':'nc30_days_in_15days'},inplace=True)
			#df_daily_offline=df_daily2[df_daily2['nc30_days_in_15days']>=10]
			#df_daily_offline=pd.merge(df_sitemas[['BankName','SiteName','McuId']],df_daily_offline,on='McuId')
			#df_daily_offline['Date']=today
			#first_column = df_daily_offline.pop('Date')
			#df_daily_offline.insert(0, 'Date', first_column) 
			#df_daily_offline['Remark']="Daily Offline Sites"


			df_pattern=Events[((Events['Hour_offline']=='16') | (Events['Hour_offline']=='17') | (Events['Hour_offline']=='18') | (Events['Hour_offline']=='19') | (Events['Hour_offline']=='20') | (Events['Hour_offline']=='21') | (Events['Hour_offline']=='22') | (Events['Hour_offline']=='23') | (Events['Hour_offline']=='24') | (Events['Hour_offline']=='01')) & ((Events['Hour_online']=='06') | (Events['Hour_online']=='07') | (Events['Hour_online']=='08') | (Events['Hour_online']=='09') | (Events['Hour_online']=='10'))]
			pattern_last_15days=df_pattern['McuId'].value_counts()
			pattern_last_15days=(pd.DataFrame(pattern_last_15days)).reset_index()
			pattern_last_15days.rename(columns={'index':'McuId','McuId':'count_15days'},inplace=True)
			pattern_last_15days=pattern_last_15days[pattern_last_15days['count_15days']>=7]

			pattern_last_7days=df_pattern[df_pattern['AtTime']>=prior7days]
			pattern_last_7days=pattern_last_7days['McuId'].value_counts()
			pattern_last_7days=(pd.DataFrame(pattern_last_7days)).reset_index()
			pattern_last_7days.rename(columns={'index':'McuId','McuId':'count_7days'},inplace=True)
			pattern_last_7days=pattern_last_7days[pattern_last_7days['count_7days']>=2]

			pattern_last_3days=df_pattern[df_pattern['AtTime']>=prior3days]
			pattern_last_3days=pattern_last_3days['McuId'].value_counts()
			pattern_last_3days=(pd.DataFrame(pattern_last_3days)).reset_index()
			pattern_last_3days.rename(columns={'index':'McuId','McuId':'count_3days'},inplace=True)
			pattern_last_3days=pattern_last_3days[pattern_last_3days['count_3days']>=1]

			df_business_hour_pattern_events=pd.merge(pattern_last_3days,pattern_last_7days,on=['McuId']).merge(pattern_last_15days,on=['McuId'])
			df_business_hour_pattern_events=df_business_hour_pattern_events[['McuId','count_3days','count_7days','count_15days']]
			df_business_hour_pattern_events.sort_values('count_15days',ascending=False)
			df_business_hour_pattern_events=pd.merge(df_sitemas[['BankName','SiteName','McuId','Status']],df_business_hour_pattern_events,on='McuId')
			df_business_hour_pattern_events=df_business_hour_pattern_events[df_business_hour_pattern_events['Status']==1] # considering onsites only
			df_business_hour_pattern_events.drop('Status',axis=1,inplace=True)
			df_business_hour_pattern_events['Date']=today
			first_column2 = df_business_hour_pattern_events.pop('Date')
			df_business_hour_pattern_events.insert(0, 'Date', first_column2)
			df_business_hour_pattern_events['Remark']="Business hour sites"

			#df_daily_offline=df_daily_offline[~df_daily_offline['McuId'].isin(df_event_count['McuId'])]
			df_offline_online_pattern=df_offline_online_pattern[~df_offline_online_pattern['McuId'].isin(df_event_count['McuId'])]
			df_business_hour_pattern_events=df_business_hour_pattern_events[~df_business_hour_pattern_events['McuId'].isin(df_offline_online_pattern['McuId'])]
			df_business_hour_pattern_events=df_business_hour_pattern_events[~df_business_hour_pattern_events['McuId'].isin(df_event_count['McuId'])]

		else:
			df_offline_online_pattern=pd.DataFrame()
			df_event_count=pd.DataFrame()
			df_business_hour_pattern_events=pd.DataFrame()

		return df_offline_online_pattern,df_event_count,df_business_hour_pattern_events#,df_daily_offline
	def pushdata(df_offline_online_pattern,df_event_count,df_business_hour_pattern_events):
		if (len(df_offline_online_pattern)+len(df_event_count)+len(df_business_hour_pattern_events))>0:
			engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')        
			df_event_count.to_sql('nc30_frequentsites',engine,if_exists='append',index=False, method="multi", chunksize=500)
			df_offline_online_pattern.to_sql('nc30_frequentsites',engine,if_exists='append',index=False)
			df_business_hour_pattern_events.to_sql('nc30_frequentsites',engine,if_exists='append',index=False) 
			#df_daily_offline.to_sql('nc30_frequentsites',engine,if_exists='append',index=False) 
			engine.dispose()
		else :
			pass
				
	events,df_sitemas,today,prior3days,prior7days=fetch(ServerAddress,UserName,Password,DatabaseName)
	df_offline_online_pattern,df_event_count,df_business_hour_pattern_events=transformation(events,df_sitemas,today,prior3days,prior7days)
	pushdata(df_offline_online_pattern,df_event_count,df_business_hour_pattern_events)
	
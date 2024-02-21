def HDD_Issue_repeated(ServerAddress,UserName,Password,DatabaseName):

	import pymssql
	import pandas as pd
	import numpy as np
	import warnings
	from dateutil.relativedelta import relativedelta
	from datetime import datetime, timedelta
	import pytz
	from sqlalchemy import create_engine
	IST = pytz.timezone('Asia/Kolkata')
	Today=pd.to_datetime(datetime.strftime(datetime.now(IST), '%Y-%m-%d'))
	prior1week=datetime.strftime(datetime.now(IST) - timedelta(7), '%Y-%m-%d')

	def fetch(ServerAddress,UserName,Password,DatabaseName):
		
		conn=pymssql.connect(host='188.42.97.40',user='aaum_db_usr',password='nfm!DBj!Pu#hZmgThxAoZe$E',database='AAUM_Analytics_DB')
		cursor=conn.cursor()
		df_hdd_error=pd.read_sql("select BankName,SiteName,Error,OpenedOn,ClosedOn from HDD_Issues where OpenedOn between '{}' and '{}'".format(prior1week,Today),conn)
		return df_hdd_error,conn,cursor

	def transformation(df_hdd_error,conn,cursor):

		df_hdd_error=df_hdd_error.sort_values('OpenedOn')
		df_hdd_error_count=pd.crosstab([df_hdd_error['BankName'],df_hdd_error['SiteName']],df_hdd_error['Error'])
		df_hdd_error_count=df_hdd_error_count.reset_index()
		df_hdd_error_count['except_ENOENT']=df_hdd_error_count.drop('ENOENT', axis=1).sum(axis=1)
		df_only_enoent=df_hdd_error_count[df_hdd_error_count['except_ENOENT']<=0]

		df_only_enoent=df_only_enoent[df_only_enoent['ENOENT']>=3]
		df_only_enoent=df_only_enoent[['BankName','SiteName','ENOENT']]
		df_hdd_error_count.drop('except_ENOENT',axis=1,inplace=True)


		df_hdd_error_count['except_ENODR']=df_hdd_error_count.drop('ENODR', axis=1).sum(axis=1)
		df_only_enodr=df_hdd_error_count[df_hdd_error_count['except_ENODR']<=0]
		df_only_enodr=df_only_enodr[df_only_enodr['ENODR']>=3]
		df_only_enodr=df_only_enodr[['BankName','SiteName','ENODR']]
		df_hdd_error_count.drop('except_ENODR',axis=1,inplace=True)

		df_hdd_error_count['All_Error']=df_hdd_error_count.iloc[:,2:].sum(axis=1)
		df_hdd_error_count['ENOENT+ENODR']=df_hdd_error_count['ENOENT']+df_hdd_error_count["ENODR"]
		df_enoent_enodr=df_hdd_error_count[(df_hdd_error_count['ENOENT']!=0) | (df_hdd_error_count['ENODR']!=0)]
		df_enoent_enodr=df_hdd_error_count[df_hdd_error_count['ENOENT+ENODR']>=3]
		df_enoent_enodr=df_enoent_enodr[['BankName','SiteName','ENOENT+ENODR']]
		df_enoent_enodr=df_enoent_enodr[~df_enoent_enodr['SiteName'].isin(df_only_enoent['SiteName'])]
		df_enoent_enodr=df_enoent_enodr[~df_enoent_enodr['SiteName'].isin(df_only_enodr['SiteName'])]

		df_all_error=df_hdd_error_count[df_hdd_error_count['All_Error']>=3]
		df_all_error=df_all_error[['BankName','SiteName','All_Error']]
		df_all_error=df_all_error[~df_all_error['SiteName'].isin(df_only_enoent['SiteName'])]
		df_all_error=df_all_error[~df_all_error['SiteName'].isin(df_only_enodr['SiteName'])]
		df_all_error=df_all_error[~df_all_error['SiteName'].isin(df_enoent_enodr['SiteName'])]

		df_enoent_enodr['Remark']="ENOENT+ENODR"
		df_only_enoent['Remark']="ENOENT"
		df_only_enodr['Remark']="ENODR"
		df_all_error['Remark']="Others"

		df_all_error.rename(columns={'All_Error':'Count_7days'},inplace=True)
		df_only_enodr.rename(columns={'ENODR':'Count_7days'},inplace=True)
		df_only_enoent.rename(columns={'ENOENT':'Count_7days'},inplace=True)
		df_enoent_enodr.rename(columns={'ENOENT+ENODR':'Count_7days'},inplace=True)

		df_error_repeated=pd.concat([df_only_enoent,df_only_enodr,df_enoent_enodr,df_all_error],ignore_index=True)
		df_error_repeated['Date']=pd.to_datetime(datetime.strftime(datetime.now(IST), '%Y-%m-%d'))
		df_error_repeated=df_error_repeated[['Date','BankName','SiteName','Count_7days','Remark']]

		return df_error_repeated,conn,cursor

	def pushdata(df_error_repeated,conn,cursor):
		if len(df_error_repeated)>0:
			delet1="DELETE FROM HDD_Issues_Repeated "
			cursor.execute(delet1)
			conn.commit()
		    
			engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')        
			df_error_repeated.to_sql('HDD_Issues_Repeated',engine,if_exists='append',index=False, method="multi", chunksize=500)
		    
		else:
			pass

		cursor.close()
		conn.close()
		
	df_hdd_error,conn,cursor = fetch(ServerAddress,UserName,Password,DatabaseName)
	df_error_repeated,conn,cursor = transformation(df_hdd_error,conn,cursor)
	pushdata(df_error_repeated,conn,cursor)

	return df_error_repeated
df_error_repeated=  HDD_Issue_repeated(ServerAddress,UserName,Password,DatabaseName)
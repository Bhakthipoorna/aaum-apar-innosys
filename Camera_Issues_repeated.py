
def Camera_Issue_repeated():

	import pymssql
	import pandas as pd
	from datetime import datetime
	import pytz
	from sqlalchemy import create_engine
	IST = pytz.timezone('Asia/Kolkata')

	def fetch():
		
		conn=pymssql.connect(host='188.42.97.40',user='aaum_db_usr',password='nfm!DBj!Pu#hZmgThxAoZe$E',database='AAUM_Analytics_DB')
		cursor=conn.cursor()
		df_camera_error=pd.read_sql("select BankName,SiteName,Address,District,State,CameraId,CameraName,OpenedOn,ClosedOn from Camera_Issues where OpenedOn>CURRENT_TIMESTAMP-7",conn)
		return df_camera_error,conn,cursor

	def transformation(df_camera_error):

		df_camera_error=df_camera_error.sort_values('OpenedOn')
		df_camera_error_count=pd.crosstab([df_camera_error['BankName'],df_camera_error['SiteName'],df_camera_error['Address'],df_camera_error['District'],df_camera_error['State']],df_camera_error['CameraName'])
		df_camera_error_count=df_camera_error_count.reset_index()

		df_outdoor_camera_issue=df_camera_error_count[df_camera_error_count['Outdoor Camera']>=3]
		df_outdoor_camera_issue=df_outdoor_camera_issue[['BankName','SiteName','Address','District','State','Outdoor Camera']]
		df_outdoor_camera_issue.rename(columns={'Outdoor Camera':'Count_7days'},inplace=True)
		df_outdoor_camera_issue['Remark']='Outdoor Camera'

		df_lobby_camera_issue=df_camera_error_count[df_camera_error_count['Lobby Camera']>=3]
		df_lobby_camera_issue=df_lobby_camera_issue[['BankName','SiteName','Address','District','State','Lobby Camera']]
		df_lobby_camera_issue.rename(columns={'Lobby Camera':'Count_7days'},inplace=True)
		df_lobby_camera_issue['Remark']='Lobby Camera'

		df_lobby2_camera_issue=df_camera_error_count[df_camera_error_count['Lobby2 Camera']>=3]
		df_lobby2_camera_issue=df_lobby2_camera_issue[['BankName','SiteName','Address','District','State','Lobby2 Camera']]
		df_lobby2_camera_issue.rename(columns={'Lobby2 Camera':'Count_7days'},inplace=True)
		df_lobby2_camera_issue['Remark']='Lobby2 Camera'

		df_lobby3_camera_issue=df_camera_error_count[df_camera_error_count['Lobby3 Camera']>=3]
		df_lobby3_camera_issue=df_lobby3_camera_issue[['BankName','SiteName','Address','District','State','Lobby3 Camera']]
		df_lobby3_camera_issue.rename(columns={'Lobby3 Camera':'Count_7days'},inplace=True)
		df_lobby3_camera_issue['Remark']='Lobby3 Camera'

		df_backroom_camera_issue=df_camera_error_count[df_camera_error_count['Backroom Camera']>=3]
		df_backroom_camera_issue=df_backroom_camera_issue[['BankName','SiteName','Address','District','State','Backroom Camera']]
		df_backroom_camera_issue.rename(columns={'Backroom Camera':'Count_7days'},inplace=True)
		df_backroom_camera_issue['Remark']='Backroom Camera'

		df_pinhole_camera_issue=df_camera_error_count[df_camera_error_count['Pinhole Camera']>=3]
		df_pinhole_camera_issue=df_pinhole_camera_issue[['BankName','SiteName','Address','District','State','Pinhole Camera']]
		df_pinhole_camera_issue.rename(columns={'Pinhole Camera':'Count_7days'},inplace=True)
		df_pinhole_camera_issue['Remark']='Pinhole Camera'

		df_camera_issue=pd.concat([df_outdoor_camera_issue,df_backroom_camera_issue,df_pinhole_camera_issue,df_lobby_camera_issue,df_lobby2_camera_issue,df_lobby3_camera_issue],ignore_index=True)
		df_camera_issue=df_camera_issue.sort_values(['SiteName','Count_7days'],ascending=False)
		df_camera_issue['Date']=pd.to_datetime(datetime.strftime(datetime.now(IST), '%Y-%m-%d'))
		df_camera_issue=df_camera_issue[['Date','BankName','SiteName','Address','District','State','Count_7days','Remark']]

		return df_camera_issue

	def pushdata(df_camera_issue):
		if len(df_camera_issue)>0:
			delet1="DELETE FROM Camera_Issues_Repeated "
			cursor.execute(delet1)
			conn.commit()
		
			engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')        
			df_camera_issue.to_sql('Camera_Issues_Repeated',engine,if_exists='append',index=False, method="multi", chunksize=500)
		
		else:
			pass

		cursor.close()
		conn.close()
		
	df_camera_issue,conn,cursor = fetch()
	df_camera_issue = transformation(df_camera_issue)
	pushdata(df_camera_issue)
	return df_camera_issue
df_camera_issue = Camera_Issue_repeated()

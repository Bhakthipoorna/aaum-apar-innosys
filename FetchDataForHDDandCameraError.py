ServerAddress='103.24.203.148'
UserName='aaum_support'
Password='aaum@123'
DatabaseName='InnoculateNCRCUB'

def HDDandCameraError(ServerAddress,UserName,Password,DatabaseName):
	import pymssql
	import numpy as np
	import pandas as pd
	from sqlalchemy import create_engine

	def fetch(ServerAddress,UserName,Password,DatabaseName):
		conn=pymssql.connect(host=ServerAddress,user=UserName,password=Password,database=DatabaseName)
		cur=conn.cursor()
		df_sitemas = pd.read_sql("select a.s_Name,a.McuId,b.BankName,a.VBoxId from lbtSiteMas as a, lbtBankMas as b WHERE a.BankCode=b.BankCode AND a.s_Status=1 AND a.qstatus='True' AND b.qstatus='True' ",conn)
		df_vboxattendance_1=pd.read_sql("select * from lbtVboxAttendance ",conn)
		df_cam = pd.read_sql("SELECT * FROM tblVideoMas WHERE qstatus='True' " ,conn)
		df1_harddiskstatus = pd.read_sql("select * from lbtharddiskstatus ",conn)
		cur.close()
		conn.close()

		return df_sitemas, df_vboxattendance_1, df_cam, df1_harddiskstatus
	df_sitemas,df_vboxattendance_1,df_cam, df1_harddiskstatus = fetch(ServerAddress,UserName,Password,DatabaseName)

	return df_sitemas, df_vboxattendance_1, df_cam, df1_harddiskstatus
df_sitemas, df_vboxattendance_1, df_cam, df1_harddiskstatus = HDDandCameraError(ServerAddress,UserName,Password,DatabaseName)
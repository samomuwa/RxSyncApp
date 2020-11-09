import datetime
import sys
import  pyodbc
import ConfigParser
import logging
import os
import smtplib
import smtplib
import xlsxwriter
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import re, math
from collections import Counter
from  SyncOtherTables  import *
from dataTransfer import *

path=os.path.dirname(os.path.realpath(__file__))+'/RxDataLoad.ini'
#prepare logfile
logFileName = datetime.datetime.today().date()
logger = logging.getLogger(str(logFileName))
hdlr = logging.FileHandler('logs\\%s.log'%(logFileName))
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.WARNING)

logger.setLevel(logging.INFO)
logger.info("Started process at %s"%(datetime.datetime.now().time()))
#get datatbase configurations
#Config = ConfigParser.ConfigParser()
Config = ConfigParser.ConfigParser(allow_no_value=True)
Config.read(path)
WORD = re.compile(r'\w+')

def get_cosine(vec1, vec2):
     intersection = set(vec1.keys()) & set(vec2.keys())
     numerator = sum([vec1[x] * vec2[x] for x in intersection])

     sum1 = sum([vec1[x]**2 for x in vec1.keys()])
     sum2 = sum([vec2[x]**2 for x in vec2.keys()])
     denominator = math.sqrt(sum1) * math.sqrt(sum2)

     if not denominator:
        return 0.0
     else:
        return round(((float(numerator) / denominator)*100),0)

def text_to_vector(text):
     words = WORD.findall(text)
     return Counter(words)
	 
def getMostSimilar(caseName,CompareSet):
	scores={}
	for med in CompareSet:
		scorePercent=get_cosine(text_to_vector(med),text_to_vector(caseName))
		if scorePercent>=55:
			scores.update({scorePercent:med})
		
	if len(scores)>1:
		bestScore=max(scores.keys())
		return [scores[bestScore],bestScore]
	else:
		return ['No close match',0]
		
def writeXlxFile(fileName,dataDict):
	workbook = xlsxwriter.Workbook('%s.xlsx'%(fileName))
	for SheetName in dataDict.keys():
		worksheet = workbook.add_worksheet(SheetName)
		# Widen the first column to make the text clearer.
		#worksheet.set_column('A:A', 20)
		# Add a bold format to use to highlight cells.
		bold = workbook.add_format({'bold': True})
		# Write some simple text.
		worksheet.set_row(0, 20, bold)
		worksheet.write('A1',dataDict[SheetName]['heading'])
		col=0
		for head in dataDict[SheetName]['header']:
			worksheet.write(1,col,head)
			col+=1
		# Text with formatting.
		row=2
		for dList in dataDict[SheetName]['data']:
			colx=0
			for d in dList:
				try:
					worksheet.write(row,colx,d)
				except Exception as e:
					continue
				colx+=1
			row+=1
	workbook.close()
def xstr(s):
    if s is None or s=='NULL' or s=='NA':
        return ''
    return str(s)
def sendNortification(send_to, subject, text,masterCursor, files=None):
	#get smpt settings
	MailConfig=masterCursor.execute("select top 1 name,[smtp server] smpt ,port, username,password from  [MDM-01].[MDS_PD].mdm.MailConfig").fetchone()
	assert isinstance(send_to, list)
	msg = MIMEMultipart()
	msg['From'] = MailConfig.name
	msg['To'] = COMMASPACE.join(send_to)
	msg['Date'] = formatdate(localtime=True)
	msg['Subject'] = subject

	msg.attach(MIMEText(text))

	for f in files or []:
		with open(f, "rb") as fil:
			part = MIMEApplication(
				fil.read(),
				Name=basename(f)
			)
		# After the file is closed
		part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
		msg.attach(part)
		

	smtp = smtplib.SMTP_SSL(str(MailConfig.smpt),int(MailConfig.port))
	#smtp.connect("smtp.gmail.com",465)
	smtp.login(''+str(MailConfig.username),str(MailConfig.password))
	smtp.sendmail(str(MailConfig.username), send_to, msg.as_string())
	smtp.close()
	masterCursor.close()
def sendDataToRemoteServer(Config):
	cnxn = pyodbc.connect('''DRIVER={%s};
							 SERVER=%s;
							 DATABASE=%s;
							 UID=%s;
							 PWD=%s'''
						%( Config.get('SourceServer','ServerDriverType')
						  ,Config.get('SourceServer','host')
						  ,Config.get('SourceServer','database')
						  ,Config.get('SourceServer','uid')
						  ,Config.get('SourceServer','pwd')))
	cursor = cnxn.cursor()
	cnxnTarget = pyodbc.connect('''DRIVER={%s};
								SERVER=%s;
								DATABASE=%s;
								UID=%s;
								PWD=%s;TDS_VERSION=8.0'''
								%( Config.get('TargetServer','ServerDriverType')
								,Config.get('TargetServer','host')
								,Config.get('TargetServer','database')
								,Config.get('TargetServer','uid')
								,Config.get('TargetServer','pwd')))
	cursorTarget = cnxnTarget.cursor()
	cnxnMaster = pyodbc.connect('''DRIVER={%s};
							SERVER=%s;
							DATABASE=%s;
							UID=%s;
							PWD=%s'''
							%( Config.get('MasterDataServer','ServerDriverType')
							,Config.get('MasterDataServer','host')
							,Config.get('MasterDataServer','database')
							,Config.get('MasterDataServer','uid')
							,Config.get('MasterDataServer','pwd')))
	cursorMaster = cnxnMaster.cursor()
	#get all tables specified
	#from master data rx transfer list and transfer data
	tbls=cursorMaster.execute("select name from [MDM-01].[MDS_PD].mdm.RxTransferTables")
	#transfer data
	#common tables
	for tbl in tbls.fetchall():
		obj=dataTransfer(cursor,cursorTarget,Config,logger,tbl.name)
	cursorMaster.close()
	cursorTarget.close()
	cursor.close()
	cnxnMaster.close()
	cnxnTarget.close()
	cnxn.close()
	logger.setLevel(logging.INFO)
	logger.info("Data transfer process completed ended at %s"%(datetime.datetime.now().time()))

#function to return entity code from master data
def getEntityCode(entityDbName,EntityName,Cursor):
	Cursor.execute("select Code from [MDM-01].[MDS_PD].mdm.%s where Name='%s'"%(entityDbName,EntityName))
	return Cursor.fetchone()[0]

def updateLocalMedicines(Config):
	reprotData={}
	#get the current product catalog at the facility
	cnxnFacility = pyodbc.connect('''DRIVER={%s};
					 SERVER=%s;
					 DATABASE=%s;
					 UID=%s;
					 PWD=%s'''
				%( Config.get('SourceServer','ServerDriverType')
				  ,Config.get('SourceServer','host')
				  ,Config.get('SourceServer','database')
				  ,Config.get('SourceServer','uid')
				  ,Config.get('SourceServer','pwd')))
	cursorFacility = cnxnFacility.cursor()
	cnxnMaster = pyodbc.connect('''DRIVER={%s};
						 SERVER=%s;
						 DATABASE=%s;
						 UID=%s;
						 PWD=%s'''
					%( Config.get('MasterDataServer','ServerDriverType')
					  ,Config.get('MasterDataServer','host')
					  ,Config.get('MasterDataServer','database')
					  ,Config.get('MasterDataServer','uid')
					  ,Config.get('MasterDataServer','pwd')))
	cursorMaster = cnxnMaster.cursor()
	#synchronize generic name ranges, pack size ranges, strength ranges,formulation ranges
	SyncOtherTables(cursorFacility,cursorMaster,Config,logger)
	#remove products from facilty before quering the facility database

	cursorMaster.execute("""select Code
								,[Facility Medicine Description] description
								,[LastChgUserName]
								,[LastChgDateTime]
								,DMO_Class 
						  from [MDM-01].[MDS_PD].mdm.[CleanMedicinesAtFacility] 
						  where Facility_Code='%s' 
						  and Action_Code=3"""%(Config.get('HealthFacility','Code')))
	itemsDeleted=[]
	medAlreadyInCorrectionList=[]
	for med in cursorMaster.fetchall():
		product_id=xstr(med.Code).split('_')[1]
		sqlx="""delete from 
			tblProductPackSize 
		  where ProductCode_ID=%s"""%(product_id)
		cursorFacility.execute(sqlx)
		#add delete report
		itemsDeleted.append([med.description,med.DMO_Class,med.LastChgUserName,med.LastChgDateTime])
		#delete from facility
		#stage and get ride of the record from staging
		try:
			medAlreadyInCorrectionList.append(med.description)
			cursorFacility.commit()
			sqlDeleteCorrectionRequest="""insert 
						into [stg].[CleanMedicinesAtFacility_Leaf] 
						( [ImportType]
						  ,[ImportStatus_ID]
						  ,[BatchTag]
						  ,[Code])
						values(?,?,?,?)"""
			cursorMaster.commit()
			params=(4
			,0
			,'deleteProductAtFacility'
			,med.Code)
			cursorMaster.execute(sqlDeleteCorrectionRequest,params)
			cursorMaster.commit()
			#run the staged record to complete removal from cleaning list

		except Exception as e:
			logger.error('failed to insert product for cleaning')
			logger.error(e)
	#run staged data to delete items removed at the facility from corrected list
	#cursorMaster.execute("EXEC [MDM-01].[MDS_PD].[stg].[udp_CleanMedicinesAtFacility_Leaf] @VersionName = N'VERSION_1',@LogFlag = 1,@BatchTag ='deleteProductAtFacility'")
	#cursorMaster.commit()	
	cursorFacility.execute("select * from tblProductPackSize where DMO_str is not null --where InstitutionEDL_bol=1")
	medsFacility=[]
	medsFacilityDict={}
	medsFacilityDictNoReportCode=[]
	labItemsFacility=[]
	itemsFacility=[]
	itemsFacilityDict={}
	itemsFacilityDictNoReportCode=[]
	labItemsFacility=[]
	seen = set()
	uniq = []
	dup=[]
	facilityCodes=[]
	for row in  cursorFacility:
		if row.ProductReportCode:
			facilityCodes.append(str(row.ProductReportCode).encode('ascii', 'ignore'))
		#load medicines
		if row.DMO_str =="D":
			medsFacility.append(str(row.ProductReportCode).encode('ascii', 'ignore'))
			medsFacilityDict.update({str(row.ProductReportCode).encode('ascii', 'ignore'):row})
			
			#keep all the drugs that dont have a unique code from master data for updating 
			if row.ProductReportCode == None:
				medsFacilityDictNoReportCode.append(row.ProductReportCode)
		elif row.DMO_str != "D":
			itemsFacility.append(str(row.ProductReportCode).encode('ascii', 'ignore'))
			itemsFacilityDict.update({str(row.ProductReportCode).encode('ascii', 'ignore'):row})
			medsFacilityDict.update({str(row.ProductReportCode).encode('ascii', 'ignore'):row})
			#keep all the lab items that dont have a unique code from master data for updating 
			if row.ProductReportCode == None:
				itemsFacilityDictNoReportCode.append(row.ProductReportCode)
	#find duplicates at the facility one is turned on and one off 
	#delete on turned off maintain on turned on i.e InstitutionEDL_bol=1
	#and drugs to be deleted instruction coming from master data i.e toDeleteDrugs[] should be populated
	for x in (set(medsFacility)|set(itemsFacility)) :
		if x not in seen :
			uniq.append(x)
			seen.add(x)
		else:
			#print x
			dup.append(x)
			#delete the duplicate record that has InstitutionEDL_bol=0
			sqlx="""delete from 
						tblProductPackSize 
					  where description_str=?
					  and InstitutionEDL_bol=0"""
			data=(medsFacilityDict[x].Description_str)
			cursorFacility.execute(sqlx,data)
			cursorFacility.commit()
	
	#print dup
	#print len(dup)
	#print medsFacilityDict
	#print data
	#get master data from master data server
	masterDataDict={}
	masterData=[]
	masterDataMeds=[]
	masterDataMedsDict={}
	masterDataLab=[]
	masterDataLabDict={}
	masterDataCodes=[]
	#add new  products in master data before querying master data
	cursorMaster.execute(""" select
       [Name]
      ,[Code]
      ,[ChangeTrackingMask]
      ,[Facility_Code]
      ,[Master Data Medicine Description_Code] medCode
      ,[Master Data Medicine Description_Name] masterlistdesc
      ,[Facility Medicine Description] facilitylistdesc
      ,[Cosine Similarity Percent Rating]
      ,[DMO_Class] dmoclass
      ,[VEN regional referal_Code] venRR
      ,[VEN national referal_Code] venNR
      ,[VEN general hospital_Code] venGH
      ,[VEN HC4_Code] venHC4
      ,[VEN HC3_Code] venHC3
      ,[VEN HC2_Code] venHC2
      ,[level of care_Code] levelofcarecode
      ,[Strenght Unit_Code] strengthuintcode
      ,[Strength Value] strengthvalue
      ,[Pack size unit_Code] packsizeunitcode
      ,[Pack size value] packsizevalue
      ,[Route_Code] RouteCode
      ,[Trade Name_Code] TradeName
	  ,[Dispensing Form_Code] dispensingformcode
	  ,[Dispensing Unit_Code] dispensingunitcode
	  ,[Dispensed value] dispensedvalue
      ,[Parent medicine classification_Code] ParentClass
      ,[Child Medicine classification_Code] ChildClass
	  ,[LastChgUserName]
	  ,[LastChgDateTime]
	   from [MDM-01].[MDS_PD].mdm.[CleanMedicinesAtFacility] 
	 where [Facility_Code] = '%s' and [Action_Code]=5
	 and [ValidationStatus]='Validation Succeeded'"""%(Config.get('HealthFacility','Code')))
	newAutoProd=cursorMaster.fetchall()
	medsToAddAuto={}
	medsCreatedInMDM=[]
	
	#get the next code in the master data list
	cursorMaster.execute('select max(Code)+1 Code from [MDM-01].[MDS_PD].mdm.medicines')
	Code=int(cursorMaster.fetchone()[0])
	for med in newAutoProd:
		#make sure that item is not in medicine leaf list already and has not run
		cursorMaster.execute("select * from [MDM-01].[MDS_PD].mdm.medicines where [Code] = %s and [Description]='%s'"%(Code,medsFacilityDict[med.facilitylistdesc].Description_str))
		#add to the stage table to create member in medicine list
		count=cursorMaster.rowcount
		if count <=0:
			medsCreatedInMDM.append([med.facilitylistdesc,med.LastChgUserName,med.LastChgDateTime])
			medsToAddAuto.update({med.masterlistdesc:med.Code})
			sql=""" insert into [MDM-01].[MDS_PD].[stg].[MedicineProductCatlog_Leaf] 
			 (ImportType
			,ImportStatus_ID
			,BatchTag
			,[Name] 
			,[Code] 
			,[JMS Code] 
			,[NMS Code] 
			,[National Code] 
			--,[ATC code] 
			,[Generic Name]
			,[Description]
			,[Strength Value] 
			,[Strength Unit]
			,[Strength Extra Description] 
			,[Pack Size Value] 
			,[Pack Description] 
			,[pack size unit]
			,[Dispensing Form]
			,[Dispensed Value]
			,[Dispensing Unit]
			,[Refrigerated]
			,[Route]
			,[Supplement]
			,[Injectable]
			,[BarCode]
			,[Shipping Pack] 
			,[Storage Temperature]
			,[Storage Conditions] 
			,[Ven National Referal] 
			,[Ven Regional Referal] 
			,[Ven General Hospital] 
			,[Ven HC4] 
			,[Ven HC3] 
			,[Ven HC2] 
			,[Administration Unit] 
			,[Level Of Care]
			,[Medicine Classification] 
			,[Parent Medicine Classification]
			,[Trade Name] 
			,[row cleaned]	)
			values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
			shippingPack=0
			try:
				shippingPack=int(medsFacilityDict[med.facilitylistdesc].ShippingPack_int)
			except:
				shippingPack=None 
			
			#add to list corrected so they are not added to correction list again 
			medAlreadyInCorrectionList.append(med.facilitylistdesc)

			params=(1
					,0
					,'AddToMDS'
					,medsFacilityDict[med.facilitylistdesc].Description_str
					,Code
					,medsFacilityDict[med.facilitylistdesc].ECN_str
					,medsFacilityDict[med.facilitylistdesc].ICN_str
					,medsFacilityDict[med.facilitylistdesc].NSN_str
					#,medsFacilityDict[med.facilitylistdesc].ATC_str
					,medsFacilityDict[med.facilitylistdesc].GenericName_str
					,medsFacilityDict[med.facilitylistdesc].Description_str
					,int(med.strengthvalue)
					,med.strengthuintcode
					,medsFacilityDict[med.facilitylistdesc].strengthExtraDescription_str
					,int(med.packsizevalue)
					,medsFacilityDict[med.facilitylistdesc].packDescription_Str
					,med.packsizeunitcode
					,med.dispensingformcode
					,int(med.dispensedvalue)
					,med.dispensingunitcode
					,bool(medsFacilityDict[med.facilitylistdesc].Refrigerated_bol)
					,med.RouteCode
					,bool(medsFacilityDict[med.facilitylistdesc].Supplement_bol)
					,bool(medsFacilityDict[med.facilitylistdesc].Injectable_bol)
					,medsFacilityDict[med.facilitylistdesc].BarCode_str
					,shippingPack
					,medsFacilityDict[med.facilitylistdesc].StorageTemp_str
					,medsFacilityDict[med.facilitylistdesc].StorageConditions_str
					,med.venNR
					,med.venRR
					,med.venGH
					,med.venHC4
					,med.venHC3
					,med.venHC2
					,medsFacilityDict[med.facilitylistdesc].Administration_str
					,med.levelofcarecode
					,med.ChildClass
					,med.ParentClass
					,med.TradeName
					,'Yes'
					)
			#print params
			try:
				cursorMaster.execute(sql,params)
				#stage the correction record for deleting 
				sqlDeleteCorrectionRequest="""insert 
										into [MDM-01].[MDS_PD].[stg].[CleanMedicinesAtFacility_Leaf] 
										( [ImportType]
										  ,[ImportStatus_ID]
										  ,[BatchTag]
										  ,[Code])
										values(?,?,?,?)"""
				cursorMaster.commit()
				params=(4
				,0
				,'deleteCorrectionAutoCreate'
				,med.Code)
				cursorMaster.execute(sqlDeleteCorrectionRequest,params)
				cursorMaster.commit()
			except Exception as e:
				logger.error('Failed delete deleteCorrectionAutoCreate')
				logger.error(e)
				#continue
		Code=Code+1	
	#Also delete the product that was created as a new product before corrections were made	
	#update the medicine list
	#cursorMaster.execute("EXEC [MDM-01].[MDS_PD].[stg].[udp_MedicineProductCatlog_Leaf] @VersionName = N'VERSION_1',@LogFlag = 1,@BatchTag ='AddToMDS'")	
	#delete from correction list
	#cursorMaster.execute("EXEC [MDM-01].[MDS_PD].[stg].[udp_CleanMedicinesAtFacility_Leaf] @VersionName = N'VERSION_1',@LogFlag = 1,@BatchTag ='deleteCorrectionAutoCreate'")
	#cursorMaster.commit()
	#get current medicine master list for this faclility level of care plus 
	#exceptions for this facility
	sql="""select [ID]
      ,[MUID]
      ,[VersionName]
      ,[VersionNumber]
      ,[VersionFlag]
      ,[Name]
      ,[Code]
      ,[ChangeTrackingMask]
      ,[Product Code]
      ,[JMS Code] JMSCode
      ,[NMS Code] NMSCode
      ,[UHMG code]
      ,[Medical Access Code]
      ,[National Code] NationalCode
      ,[ATC code] ATCcode
      ,[Generic Name Range_Name] as GenericName
      ,[Cost]
      ,[Description]
      ,[Strength Value] StrengthValue
      ,Strength_Display
      ,[Strength Unit_Name] StrengthUnit_Name
      ,DispensingFormDisplay
      ,[Strength Extra Description] StrengthExtraDescription
      ,[Pack Size Value] PackSizeValue
      ,[Pack Description] PackDescription
      ,[pack size unit_Code]
      ,realPK packsizeunit_Name
      ,[pack size unit_ID]
      ,[Pack Size] PackSize
      ,[Dispensing Form_Code]
      ,[Dispensing Form_Name] DispensingForm_Name
      ,[DispensingUnitNameCased] DispensingUnitName
      ,[Dispensed Value] DispensingValue
      ,[Dispensing Unit_Code]
      ,[Dispensed Unit_Name] DispensedUnit_Name
      ,[Dispensing Unit_ID]
      ,[Refrigerated_Code]
      ,[Refrigerated_Name]
      ,[Refrigerated_ID]
      ,[Route_Code]
      ,[Route_Name]
      ,[Route_ID]
      ,[Supplement_Code]
      ,[Supplement_Name]
      ,[Supplement_ID]
      ,[Injectable_Code]
      ,[Injectable_Name]
      ,[Injectable_ID]
      ,[BarCode]
      ,[Shipping Pack] ShippingPack
      ,[Generic Code]
      ,[Storage Temperature] StorageTemperature
      ,[Storage Conditions] StorageConditions
      ,[Spars Form Sequence Number]
      ,[Ven National Referal_Code] VenNationalReferal_Code
      ,[Ven National Referal_Name] VenNationalReferal_Name
      ,[Ven National Referal_ID]
      ,[Ven Regional Referal_Code] VenRegionalReferal_Code
      ,[Ven Regional Referal_Name] VenRegionalReferal_Name
      ,[Ven Regional Referal_ID]
      ,[Ven General Hospital_Code] VenGeneralHospital_Code
      ,[Ven General Hospital_Name] VenGeneralHospital_Name
      ,[Ven General Hospital_ID]
      ,[Ven HC4_Code] VenHC4_Code
      ,[Ven HC4_Name] VenHC4_Name
      ,[Ven HC4_ID]
      ,[Ven HC3_Code] VenHC3_Code
      ,[Ven HC3_Name] VenHC3_Name
      ,[Ven HC3_ID]
      ,[Ven HC2_Code] VenHC2_Code
      ,[Ven HC2_Name] VenHC2_Name
      ,[Ven HC2_ID]
      ,[Random assesment start date]
      ,[Random assesment end date]
      ,[Administration Unit] AdministrationUnit
      ,[Level Of Care_Code] levelOfCare
      ,[Level Of Care_Name] 
      ,[Level Of Care_ID]
      ,[Medicine Classification_Code] MedicineClassification_Code
      ,[Medicine Classification_Name] group2
      ,[Medicine Classification_ID]
      ,[Medicine Group_Code]
      ,[Medicine Group_Name] group1
      ,[Medicine Group_ID]
      --,[drug_identifier]
      ,[EnterDateTime]
      ,[EnterUserName]
      ,[EnterVersionNumber]
      ,[LastChgDateTime]
	  ,pkLastChangeDate pkLastChgDateTime
	  ,gnLastChangeDate gnLastChgDateTime
	  ,sgLastChangeDate sgLastChgDateTime
	  ,fmLastChangeDate fmLastChgDateTime
	  ,admLastChangeDate admLastChgDateTime
      ,[LastChgUserName]
      ,[LastChgVersionNumber]
      ,[ValidationStatus] 
	  ,'D' 'itemClass'	 
	  ,[Parent Medicine Classification_Name] whoClass
	  ,[Trade Name_Name] TradeName
	  ,'No' 'exemptLevelOfCare' 
	  ,[Generic Name Range_Code] GenericNameRange_Code
	  ,[Strength Range_Code]  StrengthRange_Code
	  ,[Pack Size Range_Code] PackSizeRange_Code
	  ,[Formulation Range_Code] FormulationRange_Code
	  ,[contract_Code] contractCode
	  from [MDM-01].[MDS_PD].mdm.medicines
	  where [ValidationStatus] ='Validation Succeeded'
	 -- where [Level Of Care_Code] <= 
	  union
	  select med.[ID]
      ,med.[MUID]
      ,med.[VersionName]
      ,med.[VersionNumber]
      ,med.[VersionFlag]
      ,med.[Name]
      ,med.[Code]
      ,med.[ChangeTrackingMask]
      ,med.[Product Code]
      ,med.[JMS Code] JMSCode
      ,med.[NMS Code] NMSCode
      ,med.[UHMG code]
      ,med.[Medical Access Code]
      ,med.[National Code] NationalCode
      ,med.[ATC code] ATCcode
      ,med.[Generic Name] as GenericName
      ,med.[Cost]
      ,med.[Description]
      ,med.[Strength Value] StrengthValue
      ,med.Strength_Display
      ,med.[Strength Unit_Name] StrengthUnit_Name
      ,med.DispensingFormDisplay
      ,med.[Strength Extra Description] StrengthExtraDescription
      ,med.[Pack Size Value] PackSizeValue
      ,med.[Pack Description] PackDescription
      ,med.[pack size unit_Code]
      ,med.realPK packsizeunit_Name
      ,med.[pack size unit_ID]
      ,med.[Pack Size] PackSize
      ,med.[Dispensing Form_Code]
      ,med.[Dispensing Form_Name] DispensingForm_Name
      ,[DispensingUnitNameCased] DispensingUnitName
      ,med.[Dispensed Value] DispensingValue
      ,med.[Dispensing Unit_Code]
      ,med.[Dispensed Unit_Name] DispensedUnit_Name
      ,med.[Dispensing Unit_ID]
      ,med.[Refrigerated_Code]
      ,med.[Refrigerated_Name]
      ,med.[Refrigerated_ID]
      ,med.[Route_Code]
      ,med.[Route_Name]
      ,med.[Route_ID]
      ,med.[Supplement_Code]
      ,med.[Supplement_Name]
      ,med.[Supplement_ID]
      ,med.[Injectable_Code]
      ,med.[Injectable_Name]
      ,med.[Injectable_ID]
      ,med.[BarCode]
      ,med.[Shipping Pack] ShippingPack
      ,med.[Generic Code]
      ,med.[Storage Temperature] StorageTemperature
      ,med.[Storage Conditions] StorageConditions
      ,med.[Spars Form Sequence Number]
      ,med.[Ven National Referal_Code] VenNationalReferal_Code
      ,med.[Ven National Referal_Name] VenNationalReferal_Name
      ,med.[Ven National Referal_ID]
      ,med.[Ven Regional Referal_Code] VenRegionalReferal_Code
      ,med.[Ven Regional Referal_Name] VenRegionalReferal_Name
      ,med.[Ven Regional Referal_ID]
      ,med.[Ven General Hospital_Code] VenGeneralHospital_Code
      ,med.[Ven General Hospital_Name] VenGeneralHospital_Name
      ,med.[Ven General Hospital_ID]
      ,med.[Ven HC4_Code] VenHC4_Code
      ,med.[Ven HC4_Name] VenHC4_Name
      ,med.[Ven HC4_ID]
      ,med.[Ven HC3_Code] VenHC3_Code
      ,med.[Ven HC3_Name] VenHC3_Name
      ,med.[Ven HC3_ID]
      ,med.[Ven HC2_Code] VenHC2_Code
      ,med.[Ven HC2_Name] VenHC2_Name
      ,med.[Ven HC2_ID]
      ,med.[Random assesment start date]
      ,med.[Random assesment end date]
      ,med.[Administration Unit] AdministrationUnit
      ,med.[Level Of Care_Code] levelOfCare
      ,med.[Level Of Care_Name] 
      ,med.[Level Of Care_ID]
      ,med.[Medicine Classification_Code] MedicineClassification_Code
      ,med.[Medicine Classification_Name] group2
      ,med.[Medicine Classification_ID]
      ,med.[Medicine Group_Code]
      ,med.[Medicine Group_Name] group1
      ,med.[Medicine Group_ID]
      --,med.[drug_identifier]
      ,med.[EnterDateTime]
      ,med.[EnterUserName]
      ,med.[EnterVersionNumber]
      ,med.[LastChgDateTime]
	  ,pkLastChangeDate pkLastChgDateTime
	  ,gnLastChangeDate gnLastChgDateTime
	  ,sgLastChangeDate sgLastChgDateTime
	  ,fmLastChangeDate fmLastChgDateTime
	  ,admLastChangeDate admLastChgDateTime
      ,med.[LastChgUserName]
      ,med.[LastChgVersionNumber]
      ,med.[ValidationStatus]
	  ,'D' 'itemClass'	  
	  ,[Parent Medicine Classification_Name] whoClass
	  ,med.[Trade Name_Name] TradeName
	  ,'Yes' 'exemptLevelOfCare'
	 ,med.[Generic Name Range_Code] GenericNameRange_Code
	  ,med.[Strength Range_Code]  StrengthRange_Code
	  ,med.[Pack Size Range_Code] PackSizeRange_Code
	  ,med.[Formulation Range_Code] FormulationRange_Code
	  ,[contract_Code] contractCode
	  from [MDM-01].[MDS_PD].mdm.medicines med
	  ,[MDM-01].[MDS_PD].[mdm].[AllowedDrugsPNFP] pfp
	  where pfp.[drug_Code] = med.[Code]
	  and pfp.[health facility_Code] = '%s'
	  and med.[ValidationStatus] ='Validation Succeeded'
	  """%(Config.get('HealthFacility','code'))

	updatedDrugs=[]
	medsToReplace=[]
	cursorMaster.execute(sql)
	for row in  cursorMaster.fetchall():
		masterDataCodes.append(str(row.Code).encode('ascii', 'ignore'))
		masterDataMeds.append(str(row.Code).encode('ascii', 'ignore'))
		masterDataMedsDict.update({str(row.Code).encode('ascii', 'ignore'):row})
		#get Drugs updated in MDM from last check point
		if datetime.datetime.strptime(xstr(row.LastChgDateTime).split('.')[0],'%Y-%m-%d %H:%M:%S') > datetime.datetime.strptime(Config.get('checkPoint','cataloglastupdate'),'%Y-%m-%d %H:%M:%S') or datetime.datetime.strptime(xstr(row.pkLastChgDateTime).split('.')[0],'%Y-%m-%d %H:%M:%S') > datetime.datetime.strptime(Config.get('checkPoint','cataloglastupdate'),'%Y-%m-%d %H:%M:%S') or datetime.datetime.strptime(xstr(row.gnLastChgDateTime).split('.')[0],'%Y-%m-%d %H:%M:%S') > datetime.datetime.strptime(Config.get('checkPoint','cataloglastupdate'),'%Y-%m-%d %H:%M:%S') or datetime.datetime.strptime(xstr(row.sgLastChgDateTime).split('.')[0],'%Y-%m-%d %H:%M:%S') > datetime.datetime.strptime(Config.get('checkPoint','cataloglastupdate'),'%Y-%m-%d %H:%M:%S') or datetime.datetime.strptime(xstr(row.fmLastChgDateTime).split('.')[0],'%Y-%m-%d %H:%M:%S') > datetime.datetime.strptime(Config.get('checkPoint','cataloglastupdate'),'%Y-%m-%d %H:%M:%S') or datetime.datetime.strptime(xstr(row.admLastChgDateTime).split('.')[0],'%Y-%m-%d %H:%M:%S') > datetime.datetime.strptime(Config.get('checkPoint','cataloglastupdate'),'%Y-%m-%d %H:%M:%S'):
			updatedDrugs.append(row.Code)
			#print 'added to updated'
	#get lab items and other items from master data
	sqlLab="""SELECT [ID]
				  ,[MUID]
				  ,[VersionName]
				  ,[VersionNumber]
				  ,[VersionFlag]
				  ,[Name]
				  ,[Code]
				  ,[ChangeTrackingMask]
				  ,[NMS code] NMSCode
				  ,[JMS code] JMSCode
				  ,[National Code] NationalCode
				  ,[ATC code] ATCcode
				  ,[Generic name] GenericName
				  --,[Park Size] PackSize
				  ,[Formulation] Form
				  ,[Route]  'Route_Name'
				  ,[Cost]
				  ,[Strength Value] StrengthValue
				  ,[Description]
				  ,[Pack size value] PackSizeValue
				  ,[Barcode]
				  ,[Storage Temperature] StorageTemperature
				  ,[Storage Conditions] StorageConditions
				  ,[Ven National Referal_Code] VenNationalReferal_Code
				  ,[Ven National Referal_Name] VenNationalReferal_Name
				  ,[Ven National Referal_ID]
				  ,[Ven Regional Referal_Code] VenRegionalReferal_Code
				  ,[Ven Regional Referal_Name] VenRegionalReferal_Name
				  ,[Ven Regional Referal_ID]
				  ,[Ven General Hospital_Code] VenGeneralHospital_Code
				  ,[Ven General Hospital_Name] VenGeneralHospital_Name
				  ,[Ven General Hospital_ID]
				  ,[Ven HC4_Code] VenHC4_Code
				  ,[Ven HC4_Name] VenHC4_Name
				  ,[Ven HC4_ID]
				  ,[Ven HC3_Code] VenHC3_Code
				  ,[Ven HC3_Name] VenHC3_Name
				  ,[Ven HC3_ID]
				  ,[Ven HC2_Code] VenHC2_Code
				  ,[Ven HC2_Name] VenHC2_Name
				  ,[Ven HC2_ID]
				  ,[Level Of Care_Code] levelOfCare
				  ,[Level Of Care_Name] 
				  ,[Level Of Care_ID] 
				  ,[Product Code] ProductCode
				  ,[Dispensing Value] DispensingValue
				  ,[Dispensed Unit_Name] DispensedUnit
				  ,[Refrigerated] 
				  ,[Supplement]
				  ,[Shipping Pack] ShippingPack
				  ,[Generic Code]
				  ,[Spars Form Sequence Number] 
				  ,[Random assesment start date]
				  ,[Random assesment end date]
				  ,[Administration Unit] AdministrationUnit
				  ,[Pack Description] PackDescription
				  ,[Strength Unit_Code] Strength_Display
				  ,[Strength Unit_Name] StrengthUnit_Name
				  ,DispensingFormDisplay
				  ,DispensingUnitName
				  ,[Dispensing Form_Code]
				  ,[Dispensing Form_Name] DispensingForm_Name
				  ,[Dispensing Form_ID]
				  ,[Dispensed Unit_Name] DispensedUnit_Name
				  ,[Dispensing Unit_ID]
				  ,[Pack Size Unit_Code]
				  ,[Pack Size Unit_Name] packsizeunit_Name
				  ,[Pack Size Unit_ID]
				  ,[Pack Size] PackSize 
				  ,[row cleaned]
				  ,[EnterDateTime]
				  ,[EnterUserName]
				  ,[EnterVersionNumber]
				  ,[LastChgDateTime]
					,pkLastChangeDate pkLastChgDateTime
					,gnLastChangeDate gnLastChgDateTime
					,sgLastChangeDate sgLastChgDateTime
					,fmLastChangeDate fmLastChgDateTime
					,admLastChangeDate admLastChgDateTime
				  ,[LastChgUserName]
				  ,[LastChgVersionNumber]
				  ,[ValidationStatus]
				  ,'O' 'itemClass'
				  --,null 'StrengthValue'
				  ,[Dispensed Value] 'DispensingValue'
				  ,null 'StrengthExtraDescription'
				  ,0 'Refrigerated_Code'
				  ,0 'Supplement_Code'
				  ,0 'Injectable_Code'
				  ,[Eml Categorisation_Code] 'MedicineClassification_Code'
				  ,null 'BarCode'
				  ,[Eml Categorisation_Name] 'whoClass'
				  ,[Group 1_Name] 'group1'
				  ,[Group 2_Name] 'group2'
				  ,[Trade Name_Name] TradeName
				  ,'No' 'exemptLevelOfCare'
				  ,'Yes' 'exemptLevelOfCare'
				  ,[Generic Name Range_Code] GenericNameRange_Code
				  ,[Strength Range_Code]  StrengthRange_Code
				  ,[Pack Size Range_Code] PackSizeRange_Code
				  ,[Formulation Range_Code] FormulationRange_Code
				  ,[contract_Code] contractCode
			  from [MDM-01].[MDS_PD].mdm.labs
			  where [Description] is not null
			  and [ValidationStatus] ='Validation Succeeded'  """
			  #where [Level Of Care_Code] <= %s"""%(Config.get('HealthFacility','levelofcare'))
	updatedLabItems=[]
	cursorMaster.execute(sqlLab)
	for row in  cursorMaster.fetchall():
		masterDataCodes.append(str(row.Code).encode('ascii', 'ignore'))
		masterDataLab.append(str(row.Code).encode('ascii', 'ignore'))
		masterDataLabDict.update({str(row.Code).encode('ascii', 'ignore'):row})
		#get Drugs updated in MDM from last check point
		if datetime.datetime.strptime(xstr(row.LastChgDateTime).split('.')[0],'%Y-%m-%d %H:%M:%S') > datetime.datetime.strptime(Config.get('checkPoint','cataloglastupdate'),'%Y-%m-%d %H:%M:%S') or datetime.datetime.strptime(xstr(row.pkLastChgDateTime).split('.')[0],'%Y-%m-%d %H:%M:%S') > datetime.datetime.strptime(Config.get('checkPoint','cataloglastupdate'),'%Y-%m-%d %H:%M:%S') or datetime.datetime.strptime(xstr(row.gnLastChgDateTime).split('.')[0],'%Y-%m-%d %H:%M:%S') > datetime.datetime.strptime(Config.get('checkPoint','cataloglastupdate'),'%Y-%m-%d %H:%M:%S'):
			updatedLabItems.append(row.Code)
	
	facSet = set(medsFacility)
	facSetLab = set(itemsFacility)
	masterSetDrugs = set(masterDataMeds)
	masterSetLab = set(masterDataLab)
	#get any updated products
	interProdsNoCodes=masterSetDrugs.intersection(set(medsFacilityDictNoReportCode))
	interProdsLabNoCodes=masterSetLab.intersection(set(itemsFacilityDictNoReportCode))
	#exlude products whose  description has been update
	codesIntersection=set(facilityCodes).intersection(set(masterDataCodes))
	#assing codes from master data to effectively identify a drug across all facilities
	#also update drugs that have changed in MDS since the last sync date 
	#print len(interProdsLabNoCodes)
	masterDataDict=masterDataMedsDict.copy()
	masterDataDict.update(masterDataLabDict)
	#take into account items to be replaced from master data
	medsToReplaceDict={}
	labsToReplace={}
	cursorMaster.execute("""select
						  [Master Data Medicine Description_Name] masterlistdesc
						  ,[Facility Medicine Description] facilitylistdesc
						  ,Code
						   from [MDM-01].[MDS_PD].mdm.[CleanMedicinesAtFacility] where [Facility_Code] = '%s' and [Action_Code]=1"""%(Config.get('HealthFacility','Code')))
	for med in cursorMaster.fetchall():
		medsToReplaceDict.update({med.masterlistdesc:{'factdesc':med.facilitylistdesc,'mdmCode':med.Code}})
	dataUpdatedLabs=[]
	dataUpdatedDrugs=[]
	dataErrorUpdate=[]
	for drug in interProdsNoCodes | set(updatedDrugs) | set(interProdsLabNoCodes) | set(updatedLabItems) | set(medsToReplaceDict.keys()):
		try:
			venDict={4:masterDataDict[drug].VenGeneralHospital_Name
			,5:masterDataDict[drug].VenRegionalReferal_Name
			,6:masterDataDict[drug].VenNationalReferal_Name
			,3:masterDataDict[drug].VenHC4_Name
			,2:masterDataDict[drug].VenHC3_Name
			,1:masterDataDict[drug].VenHC2_Name
			}
			#print xstr(venDict[int(Config.get('HealthFacility','levelOfCare'))])[:1]

			
			#check if this is a replace case from cleaning or not  and change key appropriatly
			keyDescription=''
			if drug in medsToReplaceDict.keys():
				keyDescription=medsToReplaceDict[drug]['factdesc']
				medsToReplace.append([masterDataDict[drug].GenericName,keyDescription,masterDataDict[drug].Description,masterDataDict[drug].EnterUserName,masterDataDict[drug].EnterDateTime])
				#stage record for deleting from pending replacement list
				sqlDeleteCorrectionRequest="""insert 
												into [MDM-01].[MDS_PD].[stg].[CleanMedicinesAtFacility_Leaf] 
												( [ImportType]
												  ,[ImportStatus_ID]
												  ,[BatchTag]
												  ,[Code])
												values(?,?,?,?)"""
				params=(4
						,0
						,'deleteCorrectionRequest'
						,medsToReplaceDict[drug]['mdmCode'])
				cursorMaster.execute(sqlDeleteCorrectionRequest,params)
				#Also delete the product that was created as a new product before corrections were made	
				cursorFacility.execute("delete from tblProductPackSize where Description_str='%s'"%(drug))
				cursorFacility.commit()
			else:
				keyDescription=drug
			
			

			if masterDataDict[drug].Code  in codesIntersection:
				"""sqlNewProd='''update tblProductPackSize
					set 
						[GenericCode_str] = ?
					   ,[FormCode_str]= ?
					   ,[PackSizeCode_str] = ?
					   ,[StrengthCode_str] = ?
					   ,[ProductCode7_str] = ?
					   ,[ProductCode_str]= ?
					   ,[StrengthRangePackCoefficient_dbl] =?
					   ,[IsAvailableForDispensing_Bol] = ?
					  ,[ProductReportCode]= ?
					  ,[GenericName_str] = ?
					  ,[StrengthUnit_Str] = ?
					  ,[Form_str] = ?
					  ,[Route_str] = ?
					  ,[PackSizeUnit_str] = ?
					  ,[DispensingUnit_str] = ?
					  ,[StrengthValue_dbl] = ?
					  ,[PackSizeValue_dbl] = ?
					  ,[DispensingValue_dbl] = ?
					  ,[packDescription_Str] = ?
					  ,[dispensingForm_str] = ?
					  ,[dispensedUnit_str] = ?
					  ,[dispensedValue_dbl] = ?
					  ,[strengthExtraDescription_str] = ?
					  ,[ICN_str] = ?
					  ,[ECN_str] = ?
					  ,[Refrigerated_bol] = ?
					  ,[VEN_str] = ?
					  ,[Supplement_bol] = ?
					  ,[Injectable_bol] = ?
					  ,[LastUpdate_dat] = ?
					  ,[LastUpdateBy_str] = ?
					  ,[PackSize_str] = ?
					  ,[ShippingPack_int] = ?
					  ,[ATC_str] = ?
					  ,[WHOClassification_str] = ?
					 ,[StorageTemp_str] = ?
					  ,[StorageConditions_str] = ?
					  ,[Administration_str] = ?
					  ,[InstitutionEDL_bol] = ?
					  ,[Group1_str] = ?s
					  ,[Group2_str] = ?
					  ,[Strength_str] = ?
					  ,[TradeName_Str] = ?
					  ,description_str=?
					  ,[Cost_mon] = ?
					  ,[ContractCode_str] = ?
					where description_str = ?
							'''
				else:"""
				keyDescription=str(masterDataDict[drug].Code).encode('ascii', 'ignore')
				sqlNewProd='''update tblProductPackSize
					set
					  	[GenericCode_str] = ?
					   ,[FormCode_str]= ?
					   ,[PackSizeCode_str] = ?
					   ,[StrengthCode_str] = ?
					   ,[ProductCode7_str] = ?
					   ,[ProductCode_str]= ?
					   ,[StrengthRangePackCoefficient_dbl] =?
					   ,[IsAvailableForDispensing_Bol] = ?
					  ,[ProductReportCode]= ?
					  ,[GenericName_str] = ?
					  ,[StrengthUnit_Str] = ?
					  ,[Form_str] = ?
					  ,[Route_str] = ?
					  ,[PackSizeUnit_str] = ?
					  ,[DispensingUnit_str] = ?
					  ,[StrengthValue_dbl] = ?
					  ,[PackSizeValue_dbl] = ?
					  ,[DispensingValue_dbl] = ?
					  ,[packDescription_Str] = ?
					  ,[dispensingForm_str] = ?
					  ,[dispensedUnit_str] = ?
					  ,[dispensedValue_dbl] = ?
					  ,[strengthExtraDescription_str] = ?
					  ,[ICN_str] = ?
					  ,[ECN_str] = ?
					  ,[Refrigerated_bol] = ?
					  ,[VEN_str] = ?
					  ,[Supplement_bol] = ?
					  ,[Injectable_bol] = ?
					  ,[LastUpdate_dat] = ?
					  ,[LastUpdateBy_str] = ?
					  ,[PackSize_str] = ?
					  ,[ShippingPack_int] = ?
					  ,[ATC_str] = ?
					  ,[WHOClassification_str] = ?
					 ,[StorageTemp_str] = ?
					  ,[StorageConditions_str] = ?
					  ,[Administration_str] = ?
					  ,[InstitutionEDL_bol] = ?
					  ,[Group1_str] = ?
					  ,[Group2_str] = ?
					  ,[Strength_str] = ?
					  ,[TradeName_Str] = ?
					  ,description_str=?
					  ,[Cost_mon] = ?
					  ,[ContractCode_str] = ?
					where [ProductReportCode]=?'''
			#logger.info(sqlNewProd)
			productCode=xstr(masterDataDict[drug].FormulationRange_Code)+xstr(masterDataDict[drug].GenericNameRange_Code)+xstr(masterDataDict[drug].StrengthRange_Code)+xstr(masterDataDict[drug].PackSizeRange_Code)
			prodDesc=(masterDataDict[drug].GenericName)+' '+xstr(masterDataDict[drug].Strength_Display)+'('+xstr(masterDataDict[drug].DispensingFormDisplay)+')['+xstr(masterDataDict[drug].PackSize) +' '+xstr(masterDataDict[drug].packsizeunit_Name)+']'
			if masterDataDict[drug].TradeName and masterDataDict[drug].TradeName !='na':
				prodDesc+=' (%s)'%(masterDataDict[drug].TradeName)
			dataNewProd=(
			 masterDataDict[drug].GenericNameRange_Code
			 ,masterDataDict[drug].FormulationRange_Code
			 ,masterDataDict[drug].PackSizeRange_Code
			 ,masterDataDict[drug].StrengthRange_Code
			 ,productCode[:7]
			 ,productCode
			 ,1
			 ,1
			,masterDataDict[drug].Code
			,masterDataDict[drug].GenericName
			,masterDataDict[drug].StrengthUnit_Name
			,masterDataDict[drug].DispensingFormDisplay
			,masterDataDict[drug].Route_Name
			,masterDataDict[drug].packsizeunit_Name
			,masterDataDict[drug].DispensingUnitName
			,masterDataDict[drug].StrengthValue
			,masterDataDict[drug].PackSizeValue
			,masterDataDict[drug].DispensingValue
			,(masterDataDict[drug].GenericName)+' '+xstr(masterDataDict[drug].Strength_Display)+'('+xstr(masterDataDict[drug].DispensingFormDisplay)+')['+xstr(masterDataDict[drug].PackSize) +' '+xstr(masterDataDict[drug].packsizeunit_Name)+']'
			,masterDataDict[drug].DispensingForm_Name
			,masterDataDict[drug].DispensedUnit_Name
			,masterDataDict[drug].DispensingValue
			,(masterDataDict[drug].GenericName)+' '+xstr(masterDataDict[drug].Strength_Display)+'; '+xstr(masterDataDict[drug].DispensingFormDisplay)+' ['+xstr(masterDataDict[drug].Route_Name)+']'
			,xstr(masterDataDict[drug].NMSCode)
			,xstr(masterDataDict[drug].JMSCode)
			,masterDataDict[drug].Refrigerated_Code
			,xstr(venDict[int(Config.get('HealthFacility','levelOfCare'))])[:1]
			,masterDataDict[drug].Supplement_Code
			,masterDataDict[drug].Injectable_Code
			,masterDataDict[drug].LastChgDateTime
			,masterDataDict[drug].LastChgUserName
			,masterDataDict[drug].PackSize
			,masterDataDict[drug].ShippingPack
			,masterDataDict[drug].ATCcode 
			,masterDataDict[drug].whoClass
			,masterDataDict[drug].StorageTemperature
			,masterDataDict[drug].StorageConditions
			,masterDataDict[drug].AdministrationUnit
			,1
			,masterDataDict[drug].group1
			,masterDataDict[drug].group2
			,masterDataDict[drug].Strength_Display
			,masterDataDict[drug].TradeName
			,prodDesc
			,masterDataDict[drug].Cost
			,masterDataDict[drug].contractCode
			,keyDescription)
			logger.info(masterDataDict[drug].whoClass)
			logger.info(dataNewProd)
			cursorFacility.execute(sqlNewProd,dataNewProd)
			cnxnFacility.commit()
			cnxnMaster.commit()
			if str(masterDataDict[drug].Code).encode('ascii','ignore') in updatedLabItems:
				dataUpdatedLabs.append([masterDataDict[drug].GenericName,prodDesc,masterDataDict[drug].EnterUserName,masterDataDict[drug].EnterDateTime])	
				#writeXlxFile(,head,data,'%s product catelog sync report'%(Config.get('HealthFacility','Name'))})
			elif str(masterDataDict[drug].Code).encode('ascii','ignore') in updatedDrugs:
				dataUpdatedDrugs.append([masterDataDict[drug].GenericName,prodDesc,masterDataDict[drug].LastChgUserName,masterDataDict[drug].LastChgDateTime])
		except Exception as e:
			logger.error("Failed to update  %s product see data below"%(masterDataDict[drug].Description))
			logger.error(e)
			dataErrorUpdate.append([masterDataDict[drug].GenericName,prodDesc,masterDataDict[drug].LastChgUserName,masterDataDict[drug].LastChgDateTime,e])
			logger.error(dataNewProd)
			
			#break
			#continue
	
	
	#add updated lab items sheet
	head=['Generic Name','Description','updated by','update date']
	reprotData.update({'Updated lab products ':{'heading':'Updated lab items in %s''s product catelog'%(Config.get('HealthFacility','Name')),'header':head,'data':dataUpdatedLabs}})
	#add updated meds to a sheet in the sync report file
	#head=['Generic Name','Strength','Strenght Unit','Pack Size','Pack Size Unit','Dispensing Form','Created by']
	reprotData.update({'Updated medicine products ':{'heading':'Updated medicines  in %s''s product catelog'%(Config.get('HealthFacility','Name')),'header':head,'data':dataUpdatedDrugs}})
	#failed update	
	head=['Generic Name','Description','updated by','update date','error']
	reprotData.update({'Updated failed products ':{'heading':'Failed to update products in  %s''s product catelog'%(Config.get('HealthFacility','Name')),'header':head,'data':dataErrorUpdate}})
	#add prodcts created in mdm auto
	#head=['Description','Created by','Date Created']
	#reprotData.update({'Prodcuts created auto in MDM':{'heading':'Products created automaticaly in MDM','header':head,'data':medsCreatedInMDM}})
	#add replaced drugs ate the facility
	#head=['Generic Name','Old medicine description','New medicine description','Replaced by','Replacement date']
	#reprotData.update({'Replaced medicine products ':{'heading':'Replaced medicines  in %s''s product catelog'%(Config.get('HealthFacility','Name')),'header':head,'data':medsToReplace}})
	
	#get new medcines in master data that the facility dose not have
	newProducts=(masterSetDrugs-facSet) | (masterSetLab - facSetLab)

	logger.info(newProducts)
	logger.info(codesIntersection)
	errorNewProd=[]
	newProductsData=[]
	if len(newProducts)>0:#of there are new products add the to the facility list
		for drug in newProducts:
			#writeXlxFile(,head,data,'%s product catelog sync report'%(Config.get('HealthFacility','Name'))})
			#print drug
			#exlude products whose  description has been update
			if str(masterDataDict[drug].Code).encode('ascii', 'ignore') not in codesIntersection:
				try:
					#enforce level of care 
					#no faclility gets prodcut above its level of care
					if int(masterDataDict[drug].levelOfCare) <= int(Config.get('HealthFacility','levelOfCare')) or masterDataDict[drug].exemptLevelOfCare == 'Yes':
						logger.info("New product is being added !")
						venDict={4:masterDataDict[drug].VenGeneralHospital_Name
								,5:masterDataDict[drug].VenRegionalReferal_Name
								,6:masterDataDict[drug].VenNationalReferal_Name
								,3:masterDataDict[drug].VenHC4_Name
								,2:masterDataDict[drug].VenHC3_Name
								,1:masterDataDict[drug].VenHC2_Name
								}
						#assign appropriate classifications
						#print venDict
						sqlNewProd='''insert into  tblProductPackSize
											(
											[GenericCode_str]
											,[FormCode_str]
											,[PackSizeCode_str]
											,[StrengthCode_str]
											,[ProductCode7_str]
											,[ProductCode_str]
											,[StrengthRangePackCoefficient_dbl]
											,[IsAvailableForDispensing_Bol]
											,[ProductReportCode]
											,[GenericName_str]
											,[StrengthUnit_Str]
											,[Form_str]
											,[Route_str]
											,[PackSizeUnit_str]
											,[DispensingUnit_str]
											,[StrengthValue_dbl]
											,[PackSizeValue_dbl]
											,[DispensingValue_dbl]
											,[packDescription_Str]
											,[dispensingForm_str]
											,[dispensedUnit_str]
											,[dispensedValue_dbl]
											,[strengthExtraDescription_str]
											,[ICN_str]
											,[ECN_str]
											,[Refrigerated_bol]
											,[VEN_str]
											,[Supplement_bol]
											,[Injectable_bol]
											,[LastUpdate_dat]
											,[LastUpdateBy_str]
											,[PackSize_str]
											,[ShippingPack_int]
											,[ATC_str]
											,[WHOClassification_str]
											,[StorageTemp_str]
											,[StorageConditions_str]
											,[Administration_str]
											,[InstitutionEDL_bol]
											,[Group1_str]
											,[Group2_str]
											,[Strength_str]
											,[TradeName_Str]
											,description_str
											,DMO_str
											,[Cost_mon]
											,[ContractCode_str]) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
						
						Strength=masterDataDict[drug].Strength_Display
						productCode=xstr(masterDataDict[drug].FormulationRange_Code)+xstr(masterDataDict[drug].GenericNameRange_Code)+xstr(masterDataDict[drug].StrengthRange_Code)+xstr(masterDataDict[drug].PackSizeRange_Code)
						prodDesc=(masterDataDict[drug].GenericName)+' '+xstr(masterDataDict[drug].Strength_Display)+'('+xstr(masterDataDict[drug].DispensingFormDisplay)+')['+xstr(masterDataDict[drug].PackSize) +' '+xstr(masterDataDict[drug].packsizeunit_Name)+']'
						if masterDataDict[drug].TradeName and masterDataDict[drug].TradeName !='na':
							prodDesc+=' (%s)'%(masterDataDict[drug].TradeName)
						dataNewProd=(
						masterDataDict[drug].GenericNameRange_Code
						,masterDataDict[drug].FormulationRange_Code
						,masterDataDict[drug].PackSizeRange_Code
						,masterDataDict[drug].StrengthRange_Code
						,productCode[:7]
						,productCode
						,1
						,1
						,masterDataDict[drug].Code
						,masterDataDict[drug].GenericName
						,masterDataDict[drug].StrengthUnit_Name
						,masterDataDict[drug].DispensingFormDisplay
						,masterDataDict[drug].Route_Name
						,masterDataDict[drug].packsizeunit_Name
						,masterDataDict[drug].DispensingUnitName
						,masterDataDict[drug].StrengthValue
						,masterDataDict[drug].PackSizeValue
						,masterDataDict[drug].DispensingValue
						,(masterDataDict[drug].GenericName)+' '+xstr(masterDataDict[drug].Strength_Display)+'('+xstr(masterDataDict[drug].DispensingFormDisplay)+')['+xstr(masterDataDict[drug].PackSize) +' '+xstr(masterDataDict[drug].packsizeunit_Name)+']'
						,masterDataDict[drug].DispensingForm_Name
						,masterDataDict[drug].DispensedUnit_Name
						,masterDataDict[drug].DispensingValue
						,(masterDataDict[drug].GenericName)+' '+xstr(masterDataDict[drug].Strength_Display)+'; '+xstr(masterDataDict[drug].DispensingFormDisplay)+' ['+xstr(masterDataDict[drug].Route_Name)+']'
						,xstr(masterDataDict[drug].NMSCode)
						,xstr(masterDataDict[drug].JMSCode)
						,masterDataDict[drug].Refrigerated_Code
						,xstr(venDict[int(Config.get('HealthFacility','levelOfCare'))])[:1]
						,masterDataDict[drug].Supplement_Code
						,masterDataDict[drug].Injectable_Code
						,masterDataDict[drug].LastChgDateTime
						,masterDataDict[drug].LastChgUserName
						,masterDataDict[drug].PackSize
						,masterDataDict[drug].ShippingPack
						,masterDataDict[drug].ATCcode 
						,masterDataDict[drug].whoClass
						,masterDataDict[drug].StorageTemperature
						,masterDataDict[drug].StorageConditions
						,masterDataDict[drug].AdministrationUnit
						,1
						,masterDataDict[drug].group1
						,masterDataDict[drug].group2
						,masterDataDict[drug].Strength_Display
						,masterDataDict[drug].TradeName
						,prodDesc.title()
				  		,masterDataDict[drug].itemClass
						,masterDataDict[drug].Cost
						,masterDataDict[drug].contractCode)
						cursorFacility.execute(sqlNewProd,dataNewProd)
						cnxnFacility.commit()
						#report any new products added to the product catalogue 
						newProductsData.append([masterDataDict[drug].GenericName,prodDesc,masterDataDict[drug].EnterUserName])	
				except Exception as e:
					logger.error("Failed to add new product see data below")
					logger.error(e)
					errorNewProd.append([masterDataDict[drug].GenericName,prodDesc,masterDataDict[drug].LastChgUserName,e])
					logger.error(dataNewProd)
	head=['Generic Name','Description','Created by']
	reprotData.update({'New products added to facility':{'heading':'New products added to faclility %s product catelog'%(Config.get('HealthFacility','Name')),'header':head,'data':newProductsData,'heading':'New Medicines added to faciltys product catalog'}})
	head=['Generic Name','Description','Created by','Error']
	reprotData.update({'Error adding products':{'heading':'New products not added to faclility %s product catelog'%(Config.get('HealthFacility','Name')),'header':head,'data':errorNewProd,'heading':'Error adding new products to faciltys product catalog'}})
	writeXlxFile('%s product catelog sync report'%(Config.get('HealthFacility','Name')),reprotData)
	cnxnFacility.close()
	#get the current time for this sync state
	#save catalog sync check point for next sync
	timiestamp=datetime.datetime.strptime(str(cursorMaster.execute("select FORMAT(GETDATE(),'yyyy-MM-dd HH:mm:ss') stamp ").fetchone().stamp),'%Y-%m-%d %H:%M:%S')- datetime.timedelta(hours=3)
	Config.set('checkPoint','cataloglastupdate',timiestamp)
	with open('RxDataLoad.ini', 'wb') as configfile:
		Config.write(configfile)
	#print len(updatedDrugs)
print datetime.datetime.now()
#update prodcut catalog
updateLocalMedicines(Config)
sendDataToRemoteServer(Config)
#send prodct catalog update report
#get email addresses to contacts
cnxnMaster = pyodbc.connect('''DRIVER={%s};
					 SERVER=%s;
					 DATABASE=%s;
					 UID=%s;
					 PWD=%s'''
				%( Config.get('MasterDataServer','ServerDriverType')
				  ,Config.get('MasterDataServer','host')
				  ,Config.get('MasterDataServer','database')
				  ,Config.get('MasterDataServer','uid')
				  ,Config.get('MasterDataServer','pwd')))
cursorMaster = cnxnMaster.cursor()
cursorMaster.execute("select [Name],[Facility_Code],[Facility_Name],[Email Address] email from [MDM-01].[MDS_PD].mdm.RxReportContacts where [Facility_Code] = '%s' "%(Config.get('HealthFacility','Code')))
for contact in cursorMaster.fetchall():
	sendNortification([contact.email], 'Rx Auto Product sync report from %s'%(Config.get('HealthFacility','Name')), 'Good day %s \n Please find attached file \n product syncronization report for your ACTION.'%(contact.Name),cnxnMaster.cursor(),['%s product catelog sync report.xlsx'%(Config.get('HealthFacility','Name'))])	
cursorMaster.close()
cnxnMaster.close()
#cursorFacility.close()
#cnxnFacility.close()
print datetime.datetime.now()
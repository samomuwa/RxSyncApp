
class SyncOtherTables:

	def __init__(self,cursorFacility,cursorMaster,Config,logger):
		self.Config = Config
		self.logger = logger
		#start sync of administration units
		self.syncAdminUnits(cursorFacility,cursorMaster)
		#start sync of formulation ranges
		self.syncFormulationRanges(cursorFacility,cursorMaster)
		#start sync of strength ranges
		self.syncStrengthRanges(cursorFacility,cursorMaster)
		#start pack size range synchronization
		self.syncPackSizeRanges(cursorFacility,cursorMaster)
		#start generic name ranges synchronization
		self.syncGenericNames(cursorFacility,cursorMaster)
	#load formulations
	def syncFormulationRanges(self,cursorFacility,cursorMaster):
		#get MDM formulation combinations
		self.logger.info('Started synchronizing Formulation ranges..')
		cursorMaster.execute('''
								SELECT 
									[Name]
									,[Code]
									,[Injectable_Code]
									,[Dispensing form_Name] form
									,[Administration Mode_Name] admin
									,[Admin Code] admincode
									,[LastChgDateTime]
									,[ValidationStatus]
								FROM [MDM-01].[MDS_PD].[mdm].[formulationRange]
								where [ValidationStatus] ='Validation succeeded'
								and [LastChgDateTime] > cast('%s' as datetime)'''%(self.Config.get('checkPoint','cataloglastupdate')))
		#get all formulation from facility catalog
		cursorFacility.execute('select cast([Code_str] as varchar) Code from [dbo].[TblFormRange]')
		formFacility=[]
		for code in cursorFacility.fetchall():
			formFacility.append(code.Code)
		for form in cursorMaster.fetchall():
			if str(form.Code) not in formFacility:
				self.logger.info('Adding new formulation range %s'%(form.Name))
				#combination is new and is to be added to the facility catalog
				sql='''insert into [dbo].[TblFormRange]
							([Code_str]
							,[Description_str]
							,[Injectable_bol]
							,[dispensingForm_str]
							,[Administration_str]
							,[AdministrationCode_str]
							)values(?,?,?,?,?,?)'''
				data=(form.Code,form.Name,form.Injectable_Code,form.form,form.admin,form.admincode)
				#self.logger.info(data)
			else:
				self.logger.info('Updating formulation range %s'%(form.Name))
				# just update the combination
				sql='''update  [dbo].[TblFormRange]
							set [Description_str] = ?
								,[Injectable_bol] = ?
								,[dispensingForm_str] = ?
								,[Administration_str] = ?
								,[AdministrationCode_str] = ?
						where [Code_str] = ?'''
				data=(form.Name,form.Injectable_Code,form.form,form.admin,form.admincode,form.Code)
				#self.logger.info(data)
			try:
				cursorFacility.execute(sql,data)
				cursorFacility.commit()
			except Exception as e:
				self.logger.error('Failed to add or update form')
				self.logger.error(data)
				self.logger.error(data)
	def syncConrtacts(self,cursorFacility,cursorMaster):
		#get MDM formulation combinations
		self.logger.info('Started synchronizing contracts ..')
		cursorMaster.execute('''
								SELECT 
									[Name]
									,[Code]
								FROM [MDM-01].[MDS_PD].[mdm].[contracts]
								where [ValidationStatus] ='Validation succeeded'
								and [LastChgDateTime] > cast('%s' as datetime)'''%(self.Config.get('checkPoint','cataloglastupdate')))
		#get all formulation from facility catalog
		cursorFacility.execute('select cast([Code_str] as varchar) Code from [dbo].[TblContract]')
		formFacility=[]
		for code in cursorFacility.fetchall():
			formFacility.append(code.Code)
		for form in cursorMaster.fetchall():
			if str(form.Code) not in formFacility:
				self.logger.info('Adding new contact  %s'%(form.Name))
				#combination is new and is to be added to the facility catalog
				sql='''insert into [dbo].[TblFormContract]
							([Code_str]
							,[Description_str]
							)values(?,?)'''
				data=(form.Code,form.Name)
				#self.logger.info(data)
			else:
				self.logger.info('Updating contract %s'%(form.Name))
				# just update the combination
				sql='''update  [dbo].[TblContract]
							set [Description_str] = ?
						where [Code_str] = ?'''
				data=(form.Name)
				#self.logger.info(data)
			try:
				cursorFacility.execute(sql,data)
				cursorFacility.commit()
			except Exception as e:
				self.logger.error('Failed to add or update Contact')
				self.logger.error(data)
				self.logger.error(data)

	#load administration units
	def syncAdminUnits(self,cursorFacility,cursorMaster):
		self.logger.info('Started synchronizing administration units..')
		#administration unit synchronization from
		cursorMaster.execute('''
				SELECT 
					[Name]
					,cast([Code] as int) [Code]
					,[amount]
					,[unit] unit
					,[ValidationStatus]
				FROM [MDM-01].[MDS_PD].[mdm].[AdminUnit]
				where ValidationStatus='Validation Succeeded'
				and [LastChgDateTime] > cast('%s' as datetime)'''%(self.Config.get('checkPoint','cataloglastupdate')))
		#get the admin units that are at the faclility already
		cursorFacility.execute('select cast([Code] as int) Code from [dbo].[TlkAdministrationUnit]')
		adminFacility=[]
		for code in cursorFacility.fetchall():
			adminFacility.append(code.Code)
		for unit in cursorMaster.fetchall():
			if int(unit.Code) in adminFacility:
				self.logger.info('Updating administration unit %s'%(unit.Name))
				#the admin unit exists and has been updated you need to change it 
				sql='''update [dbo].[TlkAdministrationUnit]
							set [AdminUnit_Qty] = ?
								,[AdminUnit_Unit] = ?
								,[AdminUnit_Type]= ?
						where [Code] = ? '''
				data=(unit.amount,unit.unit,unit.Name,unit.Code)
				#self.logger.info(data)
			else:
				self.logger.info('Adding new administration unit %s'%(unit.Name))
				#prduct not in admin uint new and needs to be added 
				sql='''insert into [dbo].[TlkAdministrationUnit]
								([AdminUnit_Qty] 
								,[AdminUnit_Unit] 
								,[AdminUnit_Type]
								,[Code]) 
						values(?,?,?,?)'''
				data=(unit.amount,unit.unit,unit.Name,unit.Code)
				#self.logger.info(data)
				#save the information to facility catalog
			try: 
				cursorFacility.execute(sql,data)
				cursorFacility.commit()
			except Exception as e:
				self.logger.error('Failed to add or update admin unit see error below')
				self.logger.error(data)
				self.logger.error(e)
		self.logger.info('Finished synchronizing administration units..')
	def syncStrengthRanges(self,cursorFacility,cursorMaster):
		self.logger.info('Started synchronizing strength ranges...')
		cursorMaster.execute('''SELECT 
								[Name]
								,[Code]
								,[value]
								,[Ratio]
								,[Unit_Name]
							FROM [MDM-01].[MDS_PD].[mdm].[StrengthRanges]
							where [ValidationStatus] ='Validation succeeded'
							and [LastChgDateTime] > cast('%s' as datetime)'''%(self.Config.get('checkPoint','cataloglastupdate')))
		#get all strength combinations at the facility
		stcombinations=[]
		cursorFacility.execute('select cast([StrengthRangeCode_str] as int) Code from [dbo].[TblStrengthRange]')
		for code in cursorFacility.fetchall():
			stcombinations.append(code.Code)
		for st in cursorMaster.fetchall():
			if int(st.Code) not in stcombinations:
				#stength combination is new and needs to be added to the facility catalog
				self.logger.info('Adding new strength unit %s'%(st.Name))
				sql='''insert 
							into  [dbo].[TblStrengthRange]
							(
								[Description_str]
							,[StrengthRangeCode_str]
							,[StrengthRangeValue_dbl]
							,[StrengthRangeDispensingValue_dbl]
							,[StrengthRangeUnit_str]
							,[Original_str]
								)values(?,?,?,?,?,?)'''
				data=(st.Name,st.Code,st.value,st.Ratio,st.Unit_Name,st.Name)
			else:
				#strength range is already at facility just needs to be update
				self.logger.info('Updating strength range %s'%(st.Name))
				sql='''update [dbo].[TblStrengthRange]
							set  [Description_str] = ?
								,[StrengthRangeValue_dbl] = ?
								,[StrengthRangeDispensingValue_dbl] = ?
								,[StrengthRangeUnit_str] = ?
								,[Original_str] = ?
						where [StrengthRangeCode_str]= ? '''
				data=(st.Name,st.value,st.Ratio,st.Unit_Name,st.Name,st.Code)
			try:
				cursorFacility.execute(sql,data)
				cursorFacility.commit()
			except Exception as e:
				self.logger.error('Faild to update/ add new strength range %s'%(st.Name))
				self.logger.error(data)
				self.logger.error(e)

	def syncPackSizeRanges(self,cursorFacility,cursorMaster):
		self.logger.info('Started synchronizing pack size ranges...')
		cursorMaster.execute('''
								SELECT 
									[Name]
									,[Code]
									,[Pack size value] value
									,[pack size unit_Name] unitname
									,[LastChgDateTime]
									,[ValidationStatus]
								FROM [MDM-01].[MDS_PD].[mdm].[PackSizeRanges]
								where [ValidationStatus] = 'Validation Succeeded'
								and [LastChgDateTime] > cast('%s' as datetime)
							'''%(self.Config.get('checkPoint','cataloglastupdate')))
		#get all packs in the facility catalog
		packFacility=[]
		cursorFacility.execute('select cast([Code_str] as int) Code from [dbo].[TblPackSizeRange]')
		for code in cursorFacility.fetchall():
			packFacility.append(code.Code)
		for pk in cursorMaster.fetchall():
			if int(pk.Code) in packFacility:
				#pack combination is not new just needs to be updated
				self.logger.info('Updating pack size range %s'%(pk.Name))
				sql='''update [dbo].[TblPackSizeRange]
							set  [Description_str]=?
								,[Code_str]=?
								,[PackSizeValue_dbl]=?
								,[PackSizeUnit_str]=?
								,[Original_Str]=?
						where [Code_str]=?'''
				data=(pk.Name,pk.Code,pk.value,pk.unitname,pk.Name,int(pk.Code))
				
			else:
				# it is a new pack combination that needs to be added to the catalog
				self.logger.info('Adding new pack size range %s'%(pk.Name))
				sql='''insert [dbo].[TblPackSizeRange]
							([Description_str]
							,[Code_str]
							,[PackSizeValue_dbl]
							,[PackSizeUnit_str]
							,[Original_Str])
							values(?,?,?,?,?)
							'''
				data=(pk.Name,pk.Code,pk.value,pk.unitname,pk.Name)
			try:
				cursorFacility.execute(sql,data)
				cursorFacility.commit()
			except Exception as e:
				self.logger.error('Failed to create new or update pack size range %s see errors below'%(pk.Name))
				self.logger.error(data)
				self.logger.error(e)

	def syncGenericNames(self,cursorFacility,cursorMaster):
		#GENERIC NAME SYNCHRONIZATION
		self.logger.info('Started Generic name range synchronization...')
		cursorMaster.execute('''select   [Name] GenericName
										,[Code] Code
										,[level] 
										,[Is Active_Code] isactive
										,[LastChgDateTime]
										,[ValidationStatus]
									from [MDM-01].[MDS_PD].mdm.GenericNameRanges 
								where [LastChgDateTime] > cast('%s' as datetime)
								and ValidationStatus='Validation Succeeded' '''%(self.Config.get('checkPoint','cataloglastupdate')))
		#get the generic name codes preset at the facility
		GenericCodesAtFacility=[]
		cursorFacility.execute('select cast([genericNameCode_str] as int) Code from [dbo].[tblGenericName]')
		for code in cursorFacility.fetchall():
			GenericCodesAtFacility.append(code.Code)
		for gen in cursorMaster.fetchall():
			#check if the generic name is already in facility catalogue
			if int(gen.Code) in GenericCodesAtFacility:
				#means changes were made to it therefore just update the record
				self.logger.info('Updating generic name range %s'%(gen.GenericName))
				sql='''update  [dbo].[tblGenericName]
							set [genericNameCode_str] = ?
								,[genericName_Str] = ?
								,[genericRxLevel_str] = ?
								,[genericActive_Bol] = ?
								,[Description_str] = ?
								,[Code_str] =? 
						where [genericNameCode_str] = ?'''
				data=(gen.Code,gen.GenericName,gen.level,gen.isactive,gen.GenericName,gen.Code,gen.Code)
			else:
				#prodcut is new and needs to bee added to the facility catalog
				self.logger.info('Adding new generic name range %s'%(gen.GenericName))
				sql='''insert into  [dbo].[tblGenericName] 
							(	[genericNameCode_str] 
								,[genericName_Str] 
								,[genericRxLevel_str] 
								,[genericActive_Bol] 
								,[Description_str] 
								,[Code_str] 
							)values(?,?,?,?,?,?)
							'''
				data=(gen.Code,gen.GenericName,gen.level,gen.isactive,gen.GenericName,gen.Code)
			#save the information to facility catalog 
			try:
				cursorFacility.execute(sql,data)
				cursorFacility.commit()
			except Exception as e:
				self.logger.error('Failed to create new or update generic name range %s see errors below'%(pk.Name))
				self.logger.error(data)
				self.logger.error(e)
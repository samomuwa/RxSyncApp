import datetime
class dataTransfer:
    table=''
    columns=[]
    colNames=[]
    def __init__(self,srcCursor,destCursor,Config,logger,tableName):
        self.Config=Config
        self.logger=logger
        self.table=tableName
        try:
            #populate table column names
            self.setColumns(srcCursor)
            #preparre source and data destination tables
            #create table if it does not exisit
            self.createDestTable(destCursor,srcCursor)
            #copy over data
            self.copyData(srcCursor,destCursor)
        except Exception as e:
            self.logger.error('process failed ! see error below')
            self.logger.error(e)
    #check if column exists in table
    def colExists(self,table,col,cursor):
        cx=cursor.execute("select ISNULL(COL_LENGTH('%s','%s'),0) length"%(table,col))
        if int(cx.fetchone().length) > 0:
            return True
        else:
            return False

    #chek table existance
    def tableExists(self,table,cursor):
        if cursor.tables(table=table, tableType='TABLE').fetchone():
            return True
        else:
            return False
    #create column list
    def setColumns(self,srcCursor):
        self.columns=[]
        self.colNames=[]
        self.logger.info('Started collecting table columns.')
        try:
            if self.tableExists(self.table,srcCursor):
                for col in srcCursor.columns(table=self.table):
                    self.columns.append(col)
                    self.colNames.append(col.column_name)
            else:
                self.logger.error('Table %s does not exist'%(self.table))
        except Exception as e:
            self.logger.error('problem occured copying table columns')
            self.logger.error(e)
    #create destination table
    def createDestTable(self,destCursor,srcCursor):
        #check if table is already at dest
        self.logger.info('Try creating a destination table to stage data to.')
        if not self.tableExists('stg_'+str(self.table),destCursor):
            num_cols=len(self.columns)
            col_def=''
            for col in self.columns:
                D_type=''
                #get rid of idetities
                if col.type_name=='int identity':
                    D_type='int'
                else:
                    D_type= col.type_name
                #take care of image type sizes and other data types with no size allowed
                if col.type_name in ['ntext','date','bit','float','image','int identity','datetime','money','bit','int','uniqueidentifier','ntext','smallint','smalldatetime']:
                    col_def=col_def+'[%s][%s] NULL,'%(col.column_name,D_type)
                else:
                     col_def=col_def+'[%s][%s](%s) NULL,'%(col.column_name,D_type,col.column_size)

            
            sql1='''CREATE TABLE %s 
                    (%s
                    [loadStatus] [varchar](10) NOT NULL DEFAULT ('new'),
                    [loadDate] [datetime] NULL,
                    --[ProductReportCode] [varchar](50) NULL,
                    [FacilityCode] [nvarchar](350) NULL,
                    [load_id] int,)
                    '''%('stg_'+self.table,col_def)
            
            try:
                destCursor.execute(sql1)
                destCursor.commit()
                self.logger.info('Table created succesfully ! '+'stg_'+self.table)
                self.logger.info('Destination and source prepared succesfully !')
            except Exception as e:
                self.logger.error('Could not prepare source and destination tables')
                raise Exception(e)
        else:
             self.logger.error('Table %s already exists table creation skipped'%(self.table))
        #prep source table
        self.logger.info('Adjusting source table to track record changes on next load')
        if not self.colExists(self.table,'loadStatus',srcCursor):
            srcCursor.execute("ALTER TABLE %s  ADD loadStatus VARCHAR(10) not null default('new')"%(self.table))
            srcCursor.commit()
        if not self.colExists(self.table,'loadDate',srcCursor):
            srcCursor.execute("ALTER TABLE %s   ADD loadDate  datetime"%(self.table))
            srcCursor.commit()
        if not self.colExists(self.table,'modifiedDate',srcCursor):
            srcCursor.execute("ALTER TABLE %s   ADD modifiedDate  datetime"%(self.table))
            #Create modifed row trigger on the source table
            sql3='''CREATE TRIGGER modified_%s
            ON %s
            AFTER UPDATE
                AS 
                    BEGIN
                        SET NOCOUNT ON;
                        DECLARE @ts DATETIME;
                        SET @ts = CURRENT_TIMESTAMP;
                        UPDATE t SET modifiedDate = @ts
                        FROM %s AS t
                        WHERE EXISTS (SELECT 1 FROM inserted WHERE %s = t.%s);
                    END;
            '''%(self.table,self.table,self.table,self.columns[0].column_name,self.columns[0].column_name)
            srcCursor.execute(sql3)
            srcCursor.commit()
        if not self.colExists(self.table,'load_id',srcCursor):
            srcCursor.execute("ALTER TABLE %s   ADD load_id  int"%(self.table))
            self.logger.info("ALTER TABLE %s   ADD load_id  int"%(self.table))
            srcCursor.commit()
        self.logger.info('Source table adjustments complete')

    def craeteNewRecord(self,row,load_id,srcCursor,destCursor):
        cols=''
        vals=''
        values=(load_id,self.Config.get('HealthFacility','Code'),)
        acceptedCols= [x for x in self.colNames if x not in ['load_id','loadDate','FacilityCode','modifiedDate','loadStatus']]
        col_num=len(acceptedCols)
        count=1
        for col in acceptedCols:
            values=values+(getattr(row,col),)
            if count==col_num:#this is the last value loose remove the comma
                vals=vals+'?'
                cols=cols+str(col)
            else:
                vals=vals+'?,'
                cols=cols+str(col)+','
            count=count+1
                
        sql='''insert into stg_%s
                (load_id,FacilityCode,loadDate,%s)
                values(?,?,CURRENT_TIMESTAMP,%s)
                '''%(self.table,cols,vals)
        try:
            destCursor.execute(sql,values)
            #update the source that the trcord has been loaded
            sql='update %s set loadStatus=?,loadDate=CURRENT_TIMESTAMP,load_id=? where %s=?'%(self.table,self.columns[0].column_name)
            #self.logger.info(sql)
            data=('loaded',load_id,getattr(row,self.columns[0].column_name))
            srcCursor.execute(sql,data)
            srcCursor.commit()
            destCursor.commit()
            self.logger.info('Record copied successfully !')
        except Exception as e:
            self.logger.error(sql)
            self.logger.error(values)
            self.logger.error('Failed to copy record.')
            self.logger.error(e)

    def copyData(self,srcCursor,destCursor):
        #before copying over data 
        # check if the columns are updated
        if  'loadStatus' not in self.columns:
            #update the columns list
            self.setColumns(srcCursor)
        #collect qualifying data from the source
        self.logger.info('Started copying data from %s to stage destination'%(self.table))
        #get all data that is new in this table
        #and send them to the destination table
        #get load id
        self.logger.info("""select max(load_id) id_num from %s """%(self.table))
        idD=srcCursor.execute("""select max(load_id) id_num from %s """%(self.table)).fetchone()[0]
        
        if idD:
            load_id=int(idD)
        else:
            load_id=0
        data_new=srcCursor.execute("""select * 
                                from %s 
                            where loadStatus='new'"""%(self.table))
        for row in data_new.fetchall():
            load_id=int(load_id)+1
            #create the new record
            self.craeteNewRecord(row,load_id,srcCursor,destCursor)
        #or get all data that has been updated since the last load
        sql='''select * 
                                from %s  
                            where loadDate < modifiedDate  '''%(self.table)
        #self.logger.info(sql)
        update_data=srcCursor.execute(sql)
        for row in update_data.fetchall():
            self.logger.info('Started to update record load id %s',row.load_id)
            self.logger.info(row)
            #create the new record
            #first delete the old reocrd before update
            dltSql='delete from stg_%s where load_id=?'%(self.table)
            param=(row.load_id,)
            destCursor.execute(dltSql,param)
            if destCursor:
                self.craeteNewRecord(row,row.load_id,srcCursor,destCursor)
                self.logger.info('update process completed successfully')
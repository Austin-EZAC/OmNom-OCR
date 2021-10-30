import json
import uuid
from helper import FileHelper, S3Helper, AuroraHelper
from trp import Document
import boto3

class OutputGenerator:
    def __init__(self, documentId, response, bucketName, objectName, forms, tables, ddbFiles, ddbForms, ddbTables, dbCluserArn, dbSecretArn):
        self.documentId = documentId
        self.response = response
        self.bucketName = bucketName
        self.objectName = objectName
        self.forms = forms
        self.tables = tables
        self.ddbFiles = ddbFiles
        self.ddbForms = ddbForms
        self.ddbTables = ddbTables
        self.dbCluserArn = dbCluserArn
        self.dbSecretArn = dbSecretArn
        print("FINISHED OUTPUT GENERATOR INIT WITH DDB_FORM")

        self.outputPath = "{}-analysis/{}/".format(objectName, documentId)

        self.document = Document(self.response)

    def saveItem(self, pk, sk, output):
        # Where database is saving its output
        jsonItem = {}
        jsonItem['documentId'] = pk
        jsonItem['outputType'] = sk
        jsonItem['outputPath'] = output

        self.ddbFiles.put_item(Item=jsonItem)

    def saveForm(self, pk, page, p):
        # Where database is saving its form details
        print("STARTED SAVEFORM FUNCTION")
        print("DOCUMENT ID: {}".format(pk))
        
        # Initiate the DynamoDB jsonItem
        jsonItem = {}
        jsonItem['documentId'] = pk

        print("ddbForms - {}".format(self.ddbForms))

        # self.ddbForms.put_item(Item=jsonItem)

        jsonItem['pageNumber'] = p

        print("STARTED FOR LOOP")
        # Export all of the document page's form's fields as key/value pairs
        for field in page.form.fields:
            if field.key and field.value and field.value.text:
                jsonItem[field.key.text] = str(field.value.text)
        print("FINISHED FOR LOOP")

        print("jsonItem - {}".format(jsonItem))

        # Put that thing where it belongs
        print("STARTED PUT_ITEM")
        
        self.ddbForms.put_item(Item=jsonItem)
        print("FINISHED PUT_ITEM")


    def saveTable(self, pk, page, p):
        # Where database is saving its table rows
        print("STARTED SAVETABLE FUNCTION")
        print("DOCUMENT ID: {}".format(pk))

        print("STARTED TABLE FOR LOOP")

        # Export all of the document page's table's rows of cells as lists of lists of a list
        for table_i, table in enumerate(page.tables, 1):
            print("TABLE: {}".format(table))
            print("TABLE ROWS: {}".format(table.rows))
            
            column_headers = {} # Keys are column indexes and values are column name strings

            # For each table, get the column headers from the first row
            # column_headers = list(table.rows[0])
            # print("COLUMN HEADERS: {}".format(column_headers))
            
            
            # rows = list(table.rows)
            # print(rows)
            # column_headers = list(rows[0])

            # Loop through remaining rows
            for row_i, row in enumerate(table.rows):  
                print("ROW #{}: {}".format(row_i, row))

                # Initiate the DynamoDB jsonItem
                jsonItem = {}
                jsonItem['recordId'] = "{}-{}-{}".format(pk, p, table_i)
                jsonItem['documentId'] = pk
                jsonItem['pageNumber'] = p
                jsonItem['tableNumber'] = table_i
                jsonItem['rowNumber'] = row_i

                if row_i == 0:
                    # Get the column headers from the first row
                    for cell_i, cell in enumerate(row.cells):
                        if cell.text:
                            column_headers[cell_i] = cell.text
                    print("COLUMN HEADERS: {}".format(column_headers))
                    continue
                else:
                    # Build out database table row record for import 
                    for cell_i, cell in enumerate(row.cells):
                        print('cell_i: {}'.format(cell_i))
                        if cell.text:
                            column_header = column_headers[cell_i]
                            print('column_header: {}'.format(column_header))
                            jsonItem[column_header] = cell.text
                
                # Import jsonItem into ddb table
                print("jsonItem - {}".format(jsonItem))
                print("STARTED PUT_ITEM")
                self.ddbTables.put_item(Item=jsonItem)
                print("FINISHED PUT_ITEM")

        print("FINISHED TABLE FOR LOOP")

    def aurora_upload(self):
        # This is a demo to access an Aurora Serverless MySQL Database Cluster, insert records, and query rows

        # Reterive the Cluster ARN and Secret ARN
        dbCluserArn = self.dbCluserArn
        dbSecretArn = self.dbSecretArn

        # These prints help for connecting to RDS SQL Query Editor. Not needed for long term use.
        print('dbCluserArn: {}'.format(dbCluserArn))
        print('dbSecretArn: {}'.format(dbSecretArn))


        # Access the Data API Client and wakeup the Aurora Cluster if currently inactive
        rdsData = boto3.client('rds-data')
        AuroraHelper.wake_up_cluster(rdsData, dbCluserArn, dbSecretArn, max_attempts = 10)


        # Create testing database and table
        dbName = 'omnom_aurora'
        formTable = 'extracted_forms'

        # Create database if it does not already exist.
        sql = """
        CREATE DATABASE IF NOT EXISTS {};
        """.format(dbName)

        # Executes SQL statement with parameters
        setupResponse = rdsData.execute_statement(
            resourceArn = dbCluserArn, 
            secretArn = dbSecretArn,  
            sql = sql,
            continueAfterTimeout = True)
        print(str(setupResponse))

        # Creates and defines table structure
        sql = """
        CREATE TABLE IF NOT EXISTS {} (
            uuid VARCHAR(36),
            document_id VARCHAR(255),
            form_key VARCHAR(255),
            form_value MEDIUMTEXT
            );
        """.format(formTable)

        setupResponse = rdsData.execute_statement(
            resourceArn = dbCluserArn, 
            secretArn = dbSecretArn,  
            database = dbName, 
            sql = sql,
            continueAfterTimeout = True)
        print(str(setupResponse))

        # Create a uuid to be inserted as the table primary key
        recordId = str(uuid.uuid4())

        # Insert a record into the testing table
        sql = """
        INSERT INTO {} (uuid, 
            document_id, 
            form_key, 
            form_value,
            PRIMARY KEY (uuid)
            ) VALUES(:uuid, :document_id, :form_key, :form_value)       
        """.format(formTable)

        # Insert parameters. Not finished
        param_set = [ 
            {'name': 'uuid',
            'value': {'stringValue': recordId} },
            {'name': 'document_id',
            'value': {'stringValue': "I am a document Id"} },
            {'name': 'form_key',
            'value': {'stringValue': "I am a form key"} },
            {'name': 'form_value',
            'value': {'stringValue': "I am a form value"} }
        ]
        
        insertResponse = rdsData.execute_statement(
            resourceArn = dbCluserArn, 
            secretArn = dbSecretArn, 
            database = dbName, 
            sql = sql,
            parameters = param_set)

        print('SQL INSERT COMPLETE')
        print(str(insertResponse))


        #Query the contents of the testing table
        sql = """SELECT * FROM {}""".format(formTable)

        queryResponse = rdsData.execute_statement(
            resourceArn = dbCluserArn, 
            secretArn = dbSecretArn, 
            database = dbName, 
            sql = sql)
        queryrecords = queryResponse['records']

        print("Query Response: {}".format(queryResponse)) 
        print("Query Records: {}".format(queryrecords))



    def _outputText(self, page, p):
        text = page.text
        opath = "{}page-{}-text.txt".format(self.outputPath, p)
        S3Helper.writeToS3(text, self.bucketName, opath)
        self.saveItem(self.documentId, "page-{}-Text".format(p), opath)

        textInReadingOrder = page.getTextInReadingOrder()
        opath = "{}page-{}-text-inreadingorder.txt".format(self.outputPath, p)
        S3Helper.writeToS3(textInReadingOrder, self.bucketName, opath)
        self.saveItem(self.documentId, "page-{}-TextInReadingOrder".format(p), opath)

    def _outputForm(self, page, p):
        csvData = []
        for field in page.form.fields: #Field contains a key/value pair
            csvItem  = []
            if(field.key):
                csvItem.append(field.key.text) #append key to csvFieldNames (wouldn't work with more than 1 file)
            else:
                csvItem.append("")
            if(field.value):
                csvItem.append(field.value.text)
            else:
                csvItem.append("")
            csvData.append(csvItem)
        csvFieldNames = ['Key', 'Value'] #Delete
        opath = "{}page-{}-forms.csv".format(self.outputPath, p)
        S3Helper.writeCSV(csvFieldNames, csvData, self.bucketName, opath)
        self.saveItem(self.documentId, "page-{}-Forms".format(p), opath)

    def _outputTable(self, page, p):

        csvData = []
        for table in page.tables:
            csvRow = []
            csvRow.append("Table")
            csvData.append(csvRow)
            for row in table.rows:
                csvRow  = []
                for cell in row.cells:
                    csvRow.append(cell.text)
                csvData.append(csvRow)
            csvData.append([])
            csvData.append([])

        opath = "{}page-{}-tables.csv".format(self.outputPath, p)
        S3Helper.writeCSVRaw(csvData, self.bucketName, opath)
        self.saveItem(self.documentId, "page-{}-Tables".format(p), opath)

    def run(self):

        # aurora_upload is only half set up to import forms. That needs to be completed and a new function created to upload OmNom tables.
        self.aurora_upload()

        if(not self.document.pages):
            return

        opath = "{}response.json".format(self.outputPath)
        S3Helper.writeToS3(json.dumps(self.response), self.bucketName, opath)
        self.saveItem(self.documentId, 'Response', opath)

        print("Total Pages in Document: {}".format(len(self.document.pages)))

        docText = ""

        p = 1
        for page in self.document.pages:

            opath = "{}page-{}-response.json".format(self.outputPath, p)
            S3Helper.writeToS3(json.dumps(page.blocks), self.bucketName, opath)
            self.saveItem(self.documentId, "page-{}-Response".format(p), opath)


            self._outputText(page, p)

            docText = docText + page.text + "\n"

            if(self.forms):
                print("STARTED SAVEFORM IN RUN")
                self.saveForm(self.documentId, page, p)
                print("FINISHED SAVE FORM IN RUN")
                
                self._outputForm(page, p)

            if(self.tables):
                print("STARTED SAVE TABLE IN RUN")
                self.saveTable(self.documentId, page, p)
                print("FINISHED SAVE TABLE IN RUN")
                
                self._outputTable(page, p)

            p = p + 1
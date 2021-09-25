import json
from helper import FileHelper, S3Helper
from trp import Document
import boto3

class OutputGenerator:
    def __init__(self, documentId, response, bucketName, objectName, forms, tables, ddb, ddb_form, ddb_table):
        self.documentId = documentId
        self.response = response
        self.bucketName = bucketName
        self.objectName = objectName
        self.forms = forms
        self.tables = tables
        self.ddb = ddb
        self.ddb_form = ddb_form
        self.ddb_table = ddb_table
        print("FINISHED OUTPUT GENERATOR INIT WITH DDB_FORM")

        self.outputPath = "{}-analysis/{}/".format(objectName, documentId)

        self.document = Document(self.response)

    def saveItem(self, pk, sk, output):
        # Where database is saving its output
        jsonItem = {}
        jsonItem['documentId'] = pk
        jsonItem['outputType'] = sk
        jsonItem['outputPath'] = output

        self.ddb.put_item(Item=jsonItem)

    def saveForm(self, pk, page, p):
        # Where database is saving its form details
        print("STARTED SAVEFORM FUNCTION")
        print("DOCUMENT ID: {}".format(pk))
        
        # Initiate the DynamoDB jsonItem
        jsonItem = {}
        jsonItem['documentId'] = pk

        print("ddb_form - {}".format(self.ddb_form))

        # self.ddb_form.put_item(Item=jsonItem)

        jsonItem['pageNumber'] = p

        print("STARTED FOR LOOP")
        # Export all of the document page's form's fields as key/value pairs
        for field in page.form.fields:
            if field.key and field.value:
                jsonItem[field.key.text] = str(field.value.text)
        print("FINISHED FOR LOOP")

        print("jsonItem - {}".format(jsonItem))

        # Put that thing where it belongs
        print("STARTED PUT_ITEM")
        
        self.ddb_form.put_item(Item=jsonItem)
        print("FINISHED PUT_ITEM")


    def saveTable(self, pk, page, p):
        # Where database is saving its table rows
        print("STARTED SAVETABLE FUNCTION")
        print("DOCUMENT ID: {}".format(pk))

        print("STARTED TABLE FOR LOOP")

        # Export all of the document page's table's rows of cells as lists of lists of a list
        for table_i, table in enumerate(page.tables, 1):
            print("TABLE: {}".format(table))
            
            # For each table, get the column headers from the first row
            column_headers = table.rows[0]
            print("COLUMN HEADERS: {}".format(column_headers))

            # Loop through remaining rows
            for row_i, row in enumerate(table.rows[1:], 1):  
                print("ROW: {}".format(row))

                # Initiate the DynamoDB jsonItem
                jsonItem = {}
                jsonItem['documentId'] = pk
                jsonItem['pageNumber'] = p
                jsonItem['tableNumber'] = table_i
                jsonItem['rowNumber'] = row_i

                # Build out database table row records to import 
                for cell_i, cell  in enumerate(row.cells):
                    column_header = column_headers[cell_i]
                    jsonItem[column_header] = cell.text
                
                # Import jsonItem into ddb table
                print("jsonItem - {}".format(jsonItem))
                print("STARTED PUT_ITEM")
                self.ddb_table.put_item(Item=jsonItem)
                print("FINISHED PUT_ITEM")

        print("FINISHED TABLE FOR LOOP")


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
import json
import os
import boto3
import time
from helper import AwsHelper
from og import OutputGenerator
import datastore

def getJobResults(api, jobId):

    pages = []

    time.sleep(5)

    client = AwsHelper().getClient('textract')
    if(api == "StartDocumentTextDetection"):
        response = client.get_document_text_detection(JobId=jobId)
    else:
        response = client.get_document_analysis(JobId=jobId)
    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']
        print("Next token: {}".format(nextToken))

    while(nextToken):
        time.sleep(5)

        if(api == "StartDocumentTextDetection"):
            response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)
        else:
            response = client.get_document_analysis(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']
            print("Next token: {}".format(nextToken))

    return pages

def processRequest(request):

    output = ""

    print(request)

    jobId = request['jobId']
    jobTag = request['jobTag']
    jobStatus = request['jobStatus']
    jobAPI = request['jobAPI']
    bucketName = request['bucketName']
    objectName = request['objectName']
    outputFiles = request["outputFiles"]
    outputForms = request["outputForms"]
    outputTables = request["outputTables"]
    documentsTable = request["documentsTable"]
    dbCluserArn = request["dbCluserArn"]
    dbSecretArn = request["dbSecretArn"]

    pages = getJobResults(jobAPI, jobId)

    print("Result pages recieved: {}".format(len(pages)))

    detectForms = False
    detectTables = False
    if(jobAPI == "StartDocumentAnalysis"):
        detectForms = True
        detectTables = True

    # Delete all cap print statements
    print("STARTING TO RUN DDB_FORM TABLE SEARCH")

    dynamodb = AwsHelper().getResource('dynamodb')
    ddbFiles = dynamodb.Table(outputFiles)
    ddbForms = dynamodb.Table(outputForms)
    ddbTables = dynamodb.Table(outputTables)

    print("ddbFiles: {}".format(ddbFiles))
    print("ddbForms: {}".format(ddbForms))
    print("ddbTables: {}".format(ddbTables))
    print("FINISHED RUN DDB_FORM TABLE SEARCH")

    print("STARTED TO RUN OUTPUT GENERATOR TABLE SEARCH WITH DDB_FORM")
    opg = OutputGenerator(jobTag, pages, bucketName, objectName, detectForms, detectTables, ddbFiles, ddbForms, ddbTables, dbCluserArn, dbSecretArn)
    print("FINISHED RUN OUTPUT GENERATOR TABLE SEARCH WITH DDB_FORM")

    opg.run()

    print("DocumentId: {}".format(jobTag))

    ds = datastore.DocumentStore(documentsTable, outputFiles)
    ds.markDocumentComplete(jobTag)

    output = "Processed -> Document: {}, Object: {}/{} processed.".format(jobTag, bucketName, objectName)

    print(output)

    return {
        'statusCode': 200,
        'body': output
    }

def lambda_handler(event, context):

    print("event: {}".format(event))

    body = json.loads(event['Records'][0]['body'])
    message = json.loads(body['Message'])

    print("Message: {}".format(message))

    request = {}

    request["jobId"] = message['JobId']
    request["jobTag"] = message['JobTag']
    request["jobStatus"] = message['Status']
    request["jobAPI"] = message['API']
    request["bucketName"] = message['DocumentLocation']['S3Bucket']
    request["objectName"] = message['DocumentLocation']['S3ObjectName']
    
    request["outputFiles"] = os.environ['OUTPUT_FILES']
    request["outputForms"] = os.environ['OUTPUT_FORMS']
    request["outputTables"] = os.environ['OUTPUT_TABLES']
    request["documentsTable"] = os.environ['DOCUMENTS_TABLE']
    request["dbCluserArn"] = os.environ['DB_CLUSTER_ARN']
    request["dbSecretArn"] = os.environ['DB_SECRET_ARN']

    return processRequest(request)

def lambda_handler_local(event, context):
    print("event: {}".format(event))
    return processRequest(event)

import aws_cdk.core as cdk
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_sns as sns
import aws_cdk.aws_sns_subscriptions as sns_subscriptions
import aws_cdk.aws_iam as iam
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_rds as rds
import aws_cdk.aws_sqs as sqs
import aws_cdk.aws_lambda as lambda_        #Because lambda is a python-reserved word, we add an underscore for the package alias
import aws_cdk.aws_events as events
import aws_cdk.aws_ssm as ssm
import aws_cdk.aws_secretsmanager as secrets
from aws_cdk.aws_lambda_event_sources import S3EventSource, DynamoEventSource, SqsEventSource, SnsEventSource    # Trigger Lambda Functions these event 
from aws_cdk.aws_events_targets import LambdaFunction


class OmnomStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        # **********SNS Topics**********
        jobCompletionTopic = sns.Topic(self, 'Omnom-JobCompletion')


        # **********IAM Roles******************************
        textractServiceRole = iam.Role(self, 'TextractServiceRole', assumed_by=iam.ServicePrincipal('textract.amazonaws.com'))
        textractServiceRole.add_to_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            resources = [jobCompletionTopic.topic_arn],
            actions = ["sns:Publish"]))




        # **********VPC******************************
        vpc = ec2.Vpc(self, "VPC")


        # **********S3 Batch Operations Role******************************
        s3BatchOperationsRole = iam.Role(self, 'S3BatchOperationsRole', assumed_by=iam.ServicePrincipal('batchoperations.s3.amazonaws.com'))


        # **********S3 Bucket******************************
        # S3 bucket for input documents and output
        contentBucket = s3.Bucket(self, 'DocumentsBucket', versioned= False, auto_delete_objects = True, removal_policy = cdk.RemovalPolicy.DESTROY)
        
        existingContentBucket = s3.Bucket(self, 'ExistingDocumentsBucket', versioned= False, auto_delete_objects = True, removal_policy = cdk.RemovalPolicy.DESTROY)
        existingContentBucket.grant_read_write(s3BatchOperationsRole)

        inventoryAndLogsBucket = s3.Bucket(self, 'InventoryAndLogsBucket', versioned= False, auto_delete_objects = True, removal_policy = cdk.RemovalPolicy.DESTROY)
        inventoryAndLogsBucket.grant_read_write(s3BatchOperationsRole)



        # **********DynamoDB Table*************************
        # https://docs.aws.amazon.com/cdk/api/latest/python/aws_cdk.aws_dynamodb/Table.html

        # DynamoDB table with links to output in S3
        outputFiles = dynamodb.Table(self, 'Output-Files', 
            partition_key = dynamodb.Attribute(name = 'documentId', type = dynamodb.AttributeType.STRING),
            sort_key = dynamodb.Attribute(name = 'outputType', type = dynamodb.AttributeType.STRING)
        )

        #DynamoDB table with Output-Forms field value pair extraction
        outputForms = dynamodb.Table(self, 'Output-Forms', 
            partition_key = dynamodb.Attribute(name = 'documentId', type = dynamodb.AttributeType.STRING),
            sort_key = dynamodb.Attribute(name = 'pageNumber', type = dynamodb.AttributeType.NUMBER)
        )

        #DynamoDB table with Output-Forms field value pair extraction
        outputTables = dynamodb.Table(self, 'Output-Tables', 
            partition_key = dynamodb.Attribute(name = 'recordId', type = dynamodb.AttributeType.STRING),
            sort_key = dynamodb.Attribute(name = 'rowNumber', type = dynamodb.AttributeType.NUMBER)
        )

        #DynamoDB table with links to output in S3
        documentsTable = dynamodb.Table(self, 'DocumentsTable', 
            partition_key = dynamodb.Attribute(name = 'documentId', type = dynamodb.AttributeType.STRING),
            stream = dynamodb.StreamViewType.NEW_IMAGE
        )

        # Remove old DynamoDB tables when app is destroyed
        outputFiles.apply_removal_policy(cdk.RemovalPolicy.DESTROY)
        outputForms.apply_removal_policy(cdk.RemovalPolicy.DESTROY)
        outputTables.apply_removal_policy(cdk.RemovalPolicy.DESTROY)
        documentsTable.apply_removal_policy(cdk.RemovalPolicy.DESTROY)


        
        # **********RDS Databases******************************
        # Aurora Serverless database cluster
        rdsCluster = rds.ServerlessCluster(self, "OmnomCluster",
            engine=rds.DatabaseClusterEngine.AURORA_MYSQL,
            vpc=vpc,
            enable_data_api=True
        )



        # **********SQS Queues*****************************
        # https://docs.aws.amazon.com/cdk/api/latest/python/aws_cdk.aws_sqs/Queue.html

        # Dead Letter Queue (DLQ)
        dlq = sqs.Queue(self, 'DeadLetterQueue',
            visibility_timeout = cdk.Duration.seconds(30), 
            retention_period = cdk.Duration.seconds(1209600)
        )

        # Input Queue for sync jobs
        syncJobsQueue = sqs.Queue(self, 'SyncJobs', 
            visibility_timeout = cdk.Duration.seconds(30), 
            retention_period = cdk.Duration.seconds(1209600), 
            dead_letter_queue = sqs.DeadLetterQueue(queue = dlq, max_receive_count = 50)
        )

        # Input Queue for async jobs
        asyncJobsQueue = sqs.Queue(self, 'AsyncJobs',
            visibility_timeout = cdk.Duration.seconds(30), 
            retention_period = cdk.Duration.seconds(1209600), 
            dead_letter_queue = sqs.DeadLetterQueue(queue = dlq, max_receive_count = 50)
        )

        # Job Results Queue
        jobResultsQueue = sqs.Queue(self, 'JobResults',
            visibility_timeout = cdk.Duration.seconds(900), 
            retention_period = cdk.Duration.seconds(1209600), 
            dead_letter_queue = sqs.DeadLetterQueue(queue = dlq, max_receive_count = 50)
        )

        # Job Completion Trigger
        jobCompletionTopic.add_subscription(
            sns_subscriptions.SqsSubscription(jobResultsQueue)
        )



        # **********Lambda Functions******************************
        # https://docs.aws.amazon.com/cdk/api/latest/python/aws_cdk.aws_lambda/LayerVersion.html

        # Helper Layer with helper functions
        helperLayer = lambda_.LayerVersion(self, 'HelperLayer', 
            code = lambda_.Code.from_asset('lambda/helper'),
            compatible_runtimes = [lambda_.Runtime.PYTHON_3_7],
            license = 'Apache-2.0',
            description = 'Helper layer.'
        )

        # Textractor helper layer
        textractorLayer = lambda_.LayerVersion(self, 'Textractor', 
            code = lambda_.Code.from_asset('lambda/textractor'),
            compatible_runtimes = [lambda_.Runtime.PYTHON_3_7],
            license = 'Apache-2.0',
            description = 'Helper layer.'
        )

        
        #------------------------------------------------------------

        # S3 Event processor
        s3Processor = lambda_.Function(self, 'S3Processor', 
            runtime = lambda_.Runtime.PYTHON_3_7,
            code = lambda_.Code.from_asset('lambda/s3processor'),
            handler = 'lambda_function.lambda_handler',
            timeout = cdk.Duration.seconds(30),
            environment = {
                'SYNC_QUEUE_URL': syncJobsQueue.queue_url,
                'ASYNC_QUEUE_URL': asyncJobsQueue.queue_url,
                'DOCUMENTS_TABLE': documentsTable.table_name,
                'OUTPUT_TABLE': outputFiles.table_name
            }
        )
        # Layer
        s3Processor.add_layers(helperLayer)
        # Trigger
        s3Processor.add_event_source(S3EventSource(contentBucket, 
            events = [ s3.EventType.OBJECT_CREATED ],
            filters = [ s3.NotificationKeyFilter(suffix = '.pdf') ]
        ))
        s3Processor.add_event_source(S3EventSource(contentBucket, 
            events = [ s3.EventType.OBJECT_CREATED ],
            filters = [ s3.NotificationKeyFilter(suffix = '.png') ]
        ))
        s3Processor.add_event_source(S3EventSource(contentBucket, 
            events = [ s3.EventType.OBJECT_CREATED ],
            filters = [ s3.NotificationKeyFilter(suffix = 'jpg') ]
        ))
        s3Processor.add_event_source(S3EventSource(contentBucket, 
            events = [ s3.EventType.OBJECT_CREATED ],
            filters = [ s3.NotificationKeyFilter(suffix = '.jpeg') ]
        ))
        # Permissions
        documentsTable.grant_read_write_data(s3Processor)
        syncJobsQueue.grant_send_messages(s3Processor)
        asyncJobsQueue.grant_send_messages(s3Processor)


        # ------------------------------------------------------------

        # S3 Batch Operations Event processor 
        s3BatchProcessor = lambda_.Function(self, 'S3BatchProcessor', 
            runtime = lambda_.Runtime.PYTHON_3_7,
            code = lambda_.Code.from_asset('lambda/s3batchprocessor'),
            handler = 'lambda_function.lambda_handler',
            reserved_concurrent_executions = 1,
            timeout = cdk.Duration.seconds(30),
            environment = {
                'DOCUMENTS_TABLE': documentsTable.table_name,
                'OUTPUT_TABLE': outputFiles.table_name
            }
        )
        # Layer
        s3BatchProcessor.add_layers(helperLayer)
        # Permissions
        documentsTable.grant_read_write_data(s3BatchProcessor)
        s3BatchProcessor.grant_invoke(s3BatchOperationsRole)
        s3BatchOperationsRole.add_to_policy(
            iam.PolicyStatement(
                actions = ["lambda:*"],
                resources = ["*"]
            )
        )


        #------------------------------------------------------------

        # Document processor (Router to Sync/Async Pipeline)
        documentProcessor = lambda_.Function(self, 'TaskProcessor', 
            runtime = lambda_.Runtime.PYTHON_3_7,
            code = lambda_.Code.from_asset('lambda/documentprocessor'),
            handler = 'lambda_function.lambda_handler',
            timeout = cdk.Duration.seconds(900),
            environment = {
                'SYNC_QUEUE_URL': syncJobsQueue.queue_url,
                'ASYNC_QUEUE_URL': asyncJobsQueue.queue_url
            }
        )
        # Layer
        documentProcessor.add_layers(helperLayer)
        # Trigger
        documentProcessor.add_event_source(
            DynamoEventSource(documentsTable,
                starting_position = lambda_.StartingPosition.TRIM_HORIZON
            )
        )
        # Permissions
        documentsTable.grant_read_write_data(documentProcessor)
        syncJobsQueue.grant_send_messages(documentProcessor)
        asyncJobsQueue.grant_send_messages(documentProcessor)


        #------------------------------------------------------------

        # Sync Jobs Processor (Process jobs using sync APIs)
        syncProcessor = lambda_.Function(self, 'SyncProcessor', 
            runtime = lambda_.Runtime.PYTHON_3_7,
            code = lambda_.Code.from_asset('lambda/syncprocessor'),
            handler = 'lambda_function.lambda_handler',
            reserved_concurrent_executions = 1,
            timeout = cdk.Duration.seconds(25),
            environment = {
                'OUTPUT_TABLE': outputFiles.table_name,
                'DOCUMENTS_TABLE': documentsTable.table_name,
                'AWS_DATA_PATH' : 'models'
            }
        )
        # Layer
        syncProcessor.add_layers(helperLayer)
        syncProcessor.add_layers(textractorLayer)
        # Trigger
        syncProcessor.add_event_source(
            SqsEventSource(syncJobsQueue, 
                batch_size = 1
            )
        )
        # Permissions
        contentBucket.grant_read_write(syncProcessor)
        existingContentBucket.grant_read_write(syncProcessor)
        outputFiles.grant_read_write_data(syncProcessor)
        documentsTable.grant_read_write_data(syncProcessor)
        syncProcessor.add_to_role_policy(
            iam.PolicyStatement(
                actions = ["textract:*"],
                resources = ["*"]
            )
        )



        #------------------------------------------------------------

        # Async Job Processor (Start jobs using Async APIs)
        asyncProcessor = lambda_.Function(self, 'ASyncProcessor',
            runtime = lambda_.Runtime.PYTHON_3_7,
            code = lambda_.Code.asset('lambda/asyncprocessor'),
            handler = 'lambda_function.lambda_handler',
            reserved_concurrent_executions = 1,
            timeout = cdk.Duration.seconds(60),
            environment = {
                'ASYNC_QUEUE_URL': asyncJobsQueue.queue_url,
                'SNS_TOPIC_ARN' : jobCompletionTopic.topic_arn,
                'SNS_ROLE_ARN' : textractServiceRole.role_arn,
                'AWS_DATA_PATH' : 'models'
            }
        )
        # Layer
        asyncProcessor.add_layers(helperLayer)
        # Triggers
        # Run async job processor every 5 minutes
        # Enable code below after test deploy
        rule = events.Rule(self, 'Rule',
            schedule = events.Schedule.expression('rate(2 minutes)')
        )
        rule.add_target(LambdaFunction(asyncProcessor))
        # Run when a job is successfully complete
        asyncProcessor.add_event_source(SnsEventSource(jobCompletionTopic))
        # Permissions
        contentBucket.grant_read(asyncProcessor)
        existingContentBucket.grant_read_write(asyncProcessor)
        asyncJobsQueue.grant_consume_messages(asyncProcessor)
        asyncProcessor.add_to_role_policy(
            iam.PolicyStatement(
                actions = ["iam:PassRole"],
                resources = [textractServiceRole.role_arn]
            )
        )
        asyncProcessor.add_to_role_policy(
            iam.PolicyStatement(
                actions = ["textract:*"],
                resources = ["*"]
            )
        )


        # ------------------------------------------------------------

        # Async Jobs Results Processor
        jobResultProcessor = lambda_.Function(self, 'JobResultProcessor', 
            runtime = lambda_.Runtime.PYTHON_3_7,
            code = lambda_.Code.from_asset('lambda/jobresultprocessor'),
            handler = 'lambda_function.lambda_handler',
            memory_size = 2000,
            reserved_concurrent_executions = 50,
            timeout = cdk.Duration.seconds(900),
            vpc = vpc,
            vpc_subnets = ec2.SubnetSelection( subnet_type= ec2.SubnetType.PRIVATE),
            environment = {
                'OUTPUT_FILES': outputFiles.table_name,
                'OUTPUT_FORMS': outputForms.table_name,
                'OUTPUT_TABLES': outputTables.table_name,
                'DOCUMENTS_TABLE': documentsTable.table_name,
                'AWS_DATA_PATH' : 'models',
                "DB_CLUSTER_ARN": rdsCluster.cluster_arn,
                "DB_SECRET_ARN": rdsCluster.secret.secret_arn
            }
        );
        # Layer
        jobResultProcessor.add_layers(helperLayer)
        jobResultProcessor.add_layers(textractorLayer)
        # Triggers
        jobResultProcessor.add_event_source(
            SqsEventSource(jobResultsQueue,
            batch_size = 1
            )
        )
        # Permissions
        outputFiles.grant_read_write_data(jobResultProcessor)
        outputForms.grant_read_write_data(jobResultProcessor)
        outputTables.grant_read_write_data(jobResultProcessor)
        documentsTable.grant_read_write_data(jobResultProcessor)
        contentBucket.grant_read_write(jobResultProcessor)
        existingContentBucket.grant_read_write(jobResultProcessor)
        rdsCluster.grant_data_api_access(jobResultProcessor)
        jobResultProcessor.add_to_role_policy(
            iam.PolicyStatement(
                actions = ["textract:*"],
                resources = ["*"]
            )
        )

        # ------------------------------------------------------------

        # PDF Generator
        pdfGenerator = lambda_.Function(self, 'PdfGenerator', 
            runtime = lambda_.Runtime.JAVA_8,
            code = lambda_.Code.from_asset('lambda/pdfgenerator'),
            handler = 'DemoLambdaV2::handleRequest',
            memory_size = 3000,
            timeout = cdk.Duration.seconds(900),
        )
        contentBucket.grant_read_write(pdfGenerator)
        existingContentBucket.grant_read_write(pdfGenerator)
        pdfGenerator.grant_invoke(syncProcessor)
        pdfGenerator.grant_invoke(asyncProcessor)


        
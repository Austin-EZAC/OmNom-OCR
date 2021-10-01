import aws_cdk.core as cdk
import aws_cdk.aws_sns as sns
import aws_cdk.aws_iam as iam
import aws_cdk.aws_s3 as s3

class OmnomStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        #**********SNS Topics**********
        jobCompletionTopic = sns.Topic(self, 'Omnom-JobCompletion')


        #**********IAM Roles******************************
        textractServiceRole = iam.Role(self, 'OmnomServiceRole', assumed_by=iam.ServicePrincipal('textract.amazonaws.com'))
        textractServiceRole.add_to_policy(iam.PolicyStatement(
            effect = iam.Effect.ALLOW,
            resources = [jobCompletionTopic.topic_arn],
            actions = ["sns:Publish"]))

        #**********S3 Batch Operations Role******************************
        s3BatchOperationsRole = iam.Role(self, 'S3BatchOperationsRole', assumed_by=iam.ServicePrincipal('batchoperations.s3.amazonaws.com'))


        # **********S3 Bucket******************************
        # S3 bucket for input documents and output
        contentBucket = s3.Bucket(self, 'DocumentsBucket', versioned= False, auto_delete_objects = True, removal_policy = cdk.RemovalPolicy.DESTROY)
        
        existingContentBucket = s3.Bucket(self, 'ExistingDocumentsBucket', versioned= False, auto_delete_objects = True, removal_policy = cdk.RemovalPolicy.DESTROY)
        existingContentBucket.grant_read_write(s3BatchOperationsRole)

        inventoryAndLogsBucket = s3.Bucket(self, 'InventoryAndLogsBucket', versioned= False, auto_delete_objects = True, removal_policy = cdk.RemovalPolicy.DESTROY)
        inventoryAndLogsBucket.grant_read_write(s3BatchOperationsRole)

        

        


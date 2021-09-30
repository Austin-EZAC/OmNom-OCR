import aws_cdk.core as cdk
import aws_cdk.aws_sns as sns
import aws_cdk.aws_iam as iam

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

        


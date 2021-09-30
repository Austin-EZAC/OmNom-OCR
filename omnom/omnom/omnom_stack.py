import aws_cdk.core as cdk
import aws_cdk.aws_sns as sns

class OmnomStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        #**********SNS Topics**********
        jobCompletionTopic = sns.Topic(self, 'Omnom-JobCompletion')
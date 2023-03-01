import os
import boto3

def lambda_handler(event, context):
    """Lambda process handler"""
    # Get environment variables for aws configuration
    aws_security_group = os.getenv('AWS_SECURITY_GROUP')
    aws_datacite_subnet_private = os.getenv('AWS_DATACITE_SUBNET_PRIVATE')
    aws_datacite_subnet_alt = os.getenv('AWS_DATACITE_SUBNET_ALT')
    aws_cluster = os.getenv('AWS_CLUSTER')

    # Get environment variables for report
    repo_id = os.getenv('REPO_ID')
    begin_date = os.getenv('BEGIN_DATE')
    end_date = os.getenv('END_DATE')
    platform = os.getenv('PLATFORM')
    publisher = os.getenv('PUBLISHER')
    publisher_id = os.getenv('PUBLISHER_ID')

    # Create ECS client
    ecs_client = boto3.client('ecs')

    # Run task on fargate with environment variable overrides
    response = ecs_client.run_task(
        cluster=aws_cluster,
        taskDefinition='analytics-worker-stage',
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [
                    aws_datacite_subnet_private,
                    aws_datacite_subnet_alt,
                ],
                'securityGroups': [
                    aws_security_group,
                ],
            }
        },
        overrides={
            'containerOverrides': [
                {
                    'name': 'analytics-worker-stage-' + repo_id,
                    'environment': [
                        {
                            'name': 'REPO_ID',
                            'value': repo_id
                        },
                        {
                            'name': 'BEGIN_DATE',
                            'value': begin_date
                        },
                        {
                            'name': 'END_DATE',
                            'value': end_date
                        },
                        {
                            'name': 'PLATFORM',
                            'value': platform
                        },
                        {
                            'name': 'PUBLISHER',
                            'value': publisher
                        },
                        {
                            'name': 'PUBLISHER_ID',
                            'value': publisher_id
                        },
                    ]
                }
            ]
        },
        count=1
    )

    # Print response
    return str(response)

if __name__ == '__main__':
    # For local testing fake the arguments to lambda handler function
    event = []
    context = []
    lambda_handler(event, context)
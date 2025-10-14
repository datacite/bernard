import os
import boto3

# Get environment variables for aws configuration
AWS_SECURITY_GROUP = os.getenv('AWS_SECURITY_GROUP')
AWS_DATACITE_SUBNET_PRIVATE = os.getenv('AWS_DATACITE_SUBNET_PRIVATE')
AWS_DATACITE_SUBNET_ALT = os.getenv('AWS_DATACITE_SUBNET_ALT')
AWS_CLUSTER = os.getenv('AWS_CLUSTER')
TASK_DEFINITION = os.getenv('TASK_DEFINITION')


def run_datafile_generator():
    print('Running datafile generator container')

    # Create ECS client
    ecs_client = boto3.client('ecs')

    # Run task on fargate with environment variable overrides
    response = ecs_client.run_task(
        cluster=AWS_CLUSTER,
        taskDefinition=TASK_DEFINITION,
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [
                    AWS_DATACITE_SUBNET_PRIVATE,
                    AWS_DATACITE_SUBNET_ALT,
                ],
                'securityGroups': [
                    AWS_SECURITY_GROUP,
                ],
            }
        },
        count=1
    )

    return response


def lambda_handler(event, context):
    try:
        run_datafile_generator()
    except Exception as e:
        print("Unexpected error: {0}".format(e))


if __name__ == '__main__':
    # For local testing fake the arguments to lambda handler function
    event = []
    context = []
    lambda_handler(event, context)
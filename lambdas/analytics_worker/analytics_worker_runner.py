import os
import datetime
import boto3

# Get environment variables for aws configuration
AWS_SECURITY_GROUP = os.getenv('AWS_SECURITY_GROUP')
AWS_DATACITE_SUBNET_PRIVATE = os.getenv('AWS_DATACITE_SUBNET_PRIVATE')
AWS_DATACITE_SUBNET_ALT = os.getenv('AWS_DATACITE_SUBNET_ALT')
AWS_CLUSTER = os.getenv('AWS_CLUSTER')
TASK_DEFINITION = os.getenv('TASK_DEFINITION')


# Function to get reports to generate
def get_reports_to_generate():
    # Not implemented
    print('Report query not implemented')
    return []

def run_generate_report(repo_id, begin_date, end_date, platform, publisher, publisher_id):
    # Format string with report parameters
    print('Running report for repo_id: %s, begin_date: %s, end_date: %s, platform: %s, publisher: %s, publisher_id: %s' % (repo_id, begin_date, end_date, platform, publisher, publisher_id))

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
        overrides={
            'containerOverrides': [
                {
                    'name': TASK_DEFINITION,
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

    return response

# Lambda handler
def lambda_handler(event, context):
    """Lambda process handler"""

    # Calculate default begin and date as last 30 days
    default_begin_date = datetime.datetime.now() - datetime.timedelta(days=30)
    default_end_date = datetime.datetime.now()
    # Format to YYYY-MM-DD
    default_begin_date = default_begin_date.strftime('%Y-%m-%d')
    default_end_date = default_end_date.strftime('%Y-%m-%d')

    # Get report parameters from event object
    repo_id = event.get('repo_id', '')
    begin_date = event.get('begin_date', default_begin_date)
    end_date = event.get('end_date', default_end_date)
    platform = event.get('platform', '')
    publisher = event.get('publisher', '')
    publisher_id = event.get('publisher_id', '')

    # If the repo_id is not set, get all reports to generate
    if repo_id == '':
        reports_to_generate = get_reports_to_generate()
    else:
        reports_to_generate = [repo_id]

    # If no reports to generate, return
    if len(reports_to_generate) == 0:
        return 'No reports to generate'

    # Loop through reports to generate
    response = ''
    for report in reports_to_generate:
        response = run_generate_report(repo_id, begin_date, end_date, platform, publisher, publisher_id)

    # Print response
    return str(response)

if __name__ == '__main__':
    # For local testing fake the arguments to lambda handler function
    event = {
        'repo_id': 'crossref',
    }
    context = []
    lambda_handler(event, context)
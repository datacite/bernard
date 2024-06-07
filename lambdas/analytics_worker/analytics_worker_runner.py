import os
import datetime
import boto3
import json

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

class Report:
    def __init__(self, repo_id, begin_date, end_date, platform, publisher, publisher_id):
        self.repo_id = repo_id
        self.begin_date = begin_date
        self.end_date = end_date
        self.platform = platform
        self.publisher = publisher
        self.publisher_id = publisher_id

    def generate_report(self):
        run_generate_report(self.repo_id, self.begin_date, self.end_date, self.platform, self.publisher, self.publisher_id)

# Lambda handler
def lambda_handler(event, context):
    """Lambda process handler"""

    # Calculate default begin and date as last 30 days
    default_begin_date = datetime.datetime.now() - datetime.timedelta(days=30)
    default_end_date = datetime.datetime.now()
    # Format to YYYY-MM-DD
    default_begin_date = default_begin_date.strftime('%Y-%m-%d')
    default_end_date = default_end_date.strftime('%Y-%m-%d')

    reports_to_generate = []
    # Parse SQS records from event
    if 'Records' in event:
        for record in event['Records']:
            if 'body' in record:
                event = json.loads(record['body'])

                # Get report parameters from parsed event
                repo_id = event.get('repo_id', '')
                begin_date = event.get('begin_date', default_begin_date)
                end_date = event.get('end_date', default_end_date)
                platform = event.get('platform', '')
                publisher = event.get('publisher', '')
                publisher_id = event.get('publisher_id', '')

                report = Report(repo_id, begin_date, end_date, platform, publisher, publisher_id)
                reports_to_generate.append(report)
    else:
        # Get report parameters from event object
        repo_id = event.get('repo_id', '')
        begin_date = event.get('begin_date', default_begin_date)
        end_date = event.get('end_date', default_end_date)
        platform = event.get('platform', '')
        publisher = event.get('publisher', '')
        publisher_id = event.get('publisher_id', '')

        report = Report(repo_id, begin_date, end_date, platform, publisher, publisher_id)
        reports_to_generate.append(report)

    # If no reports to generate, return
    if len(reports_to_generate) == 0:
        return 'No reports to generate'

    # Loop through reports to generate
    responses= []
    for report in reports_to_generate:
        # Generate report
        response = report.generate_report()
        responses.append(response)

    # Print responses
    return json.dumps(responses)

if __name__ == '__main__':
    # For local testing fake the arguments to lambda handler function
    event = {
        'repo_id': 'datacite',
    }
    context = []
    lambda_handler(event, context)

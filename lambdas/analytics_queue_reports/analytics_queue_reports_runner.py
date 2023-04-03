"""
This lambda function is triggered by a CloudWatch event rule on a schedule.
It will query the API for a list of repositories that have a tracking id and then
queue a task for each repository to generate the reports for the past calendar month.
"""

import os
import requests
import json
import datetime
import boto3

def last_day_of_month(any_day):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - datetime.timedelta(days=next_month.day)

# Lambda handler
def lambda_handler(event, context):
    """Lambda process handler"""
    print("Starting analytics_queue_reports_runner lambda function")

    # Configuration
    api_username = os.getenv('API_USERNAME')
    api_password = os.getenv('API_PASSWORD')
    datacite_api_url = os.getenv('DATACITE_API_URL', 'https://api.stage.datacite.org')
    queue_name = os.getenv('QUEUE_NAME')

    # Call API to get list of repositories
    api_url = datacite_api_url + '/repositories?query=_exists_:analytics_tracking_id'
    response = requests.get(api_url, auth=(api_username, api_password))

    # Parse API response into a Python dictionary
    response_dict = json.loads(response.text)

    # Calculate the date range for the report
    # begin date is start of the calender month
    # end date is end of the calender month
    begin_date = datetime.datetime.now().replace(day=1).strftime("%Y-%m-%d")
    end_date = last_day_of_month(datetime.datetime.now()).strftime("%Y-%m-%d")

    repositories = response_dict['data']
    report_details_list = []
    for repository in repositories:
        name = repository['attributes']['name']
        tracking_id = repository['attributes']['analyticsTrackingId']
        symbol = repository['attributes']['symbol']
        report_details_list.append(
            {
                'repo_id': tracking_id,
                'publisher': name,
                'publisher_id': symbol,
                'platform': 'datacite',
                'begin_date': begin_date,
                "end_date": end_date,
            }
        )

    # Queue a task for each repository
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    for report_details in report_details_list:
        queue.send_message(MessageBody=json.dumps(report_details))

    return("Queued {} tasks".format(len(report_details_list)))

if __name__ == '__main__':
    # For local testing fake the arguments to lambda handler function
    event = {}
    context = []
    lambda_handler(event, context)
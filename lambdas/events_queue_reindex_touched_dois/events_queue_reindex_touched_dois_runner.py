"""
This lambda function is triggered by a CloudWatch event rule on a schedule.
It add a message to the queue with the current date, to be processed by Events.
Events will call the `reindex_touched_dois` method to reindex the DOIs touched today.
"""

import os
import json
import datetime
import boto3

# Lambda handler
def lambda_handler(event, context):
    """Lambda process handler"""
    print("Starting reindex_touched_dois_runner lambda function")

    # Configuration
    queue_name = os.getenv('QUEUE_NAME')

    # Calculate the date range for the reindex
    today = datetime.date.today().strftime("%Y-%m-%d")

    message = {
        'Name': 'shoryuken_class',
        'Type': 'String',
        'Value': 'ReindexTouchedDoisWorker',

        'date': today,
    }

    # Queue a task for each repository
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    queue.send_message(MessageBody=json.dumps(message))

    return("Queued reindex for dois touched on {}".format(today))

if __name__ == '__main__':
    # For local testing fake the arguments to lambda handler function
    event = {}
    context = []
    lambda_handler(event, context)

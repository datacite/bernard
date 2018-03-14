"""Seed links for checking to the PidCheck service via redis"""

import urllib.request
import urllib.parse
import json
import logging
import os
import redis

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Rest API endpoint where we want to gather doi's to check from.
API_ENDPOINT =  os.getenv('API_ENDPOINT', 'https://api.datacite.org/works')

# Redis key collections for urls and results
START_URLS_KEY = os.getenv('START_URLS_KEY', 'pidcheck:start_urls')
RESULTS_ITEMS_KEY = os.getenv('REDIS_ITEMS_KEY', 'pidcheck:items')

# Redis details
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', 6379)

# Configure basic logging
logging.basicConfig(level=LOG_LEVEL)

def get_samples(sample_per_client=1):
    """Get random samples of doi's across clients"""
    page = 1
    total_pages = 1
    results = []
    # Multiple pages of results
    while page <= total_pages:
        params = {
            'sample': sample_per_client,
            'sample-group': 'data-center',
            'page[size]': 1000,
            'url': '*'
        }

        # Build the request to get the sample dois
        payload = urllib.parse.urlencode(params)
        url = API_ENDPOINT + '?%s' % payload
        with urllib.request.urlopen(url) as f:
            data = json.loads(f.read())

            # Extract just doi and url
            for work in data['data']:
                result = {
                    'doi': work['attributes']['doi'],
                    'url': work['attributes']['url']
                }
                results.append(result)

            total_pages = data['meta']['total_pages']

        page += 1

    return results

def seed_pid(pid, url):
    """Push a pid url into redis for processing by the crawler"""
    pl = {'pid': pid, 'url': url}
    logging.info("Queueing '{0}' with url '{1}' for processing".format(pid, url))
    # Redis server connector
    redis_conn = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    redis_conn.lpush(START_URLS_KEY, json.dumps(pl))

def lambda_handler(event, context):
    """Lambda process handler"""

    # Obtain the random samples for checking
    num_samples = 1
    logging.info("Sampling for %s samples per client" % (num_samples))
    samples = get_samples(num_samples)

    # Parse samples and request for checking each one.
    for sample in samples:
        doi = sample['doi']
        url = sample['url']

        # Queue the pid and url for checking
        seed_pid(doi, url)

if __name__ == '__main__':
    # For local testing fake the arguments to lambda handler function
    event = []
    context = []
    lambda_handler(event, context)
"""Store link checker crawler results via redis"""

import urllib.request
import base64
import json
import logging
import os
import redis

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Rest API endpoint where we want to send the link check results to.
API_ENDPOINT =  os.getenv('API_ENDPOINT', 'https://api.test.datacite.org/dois/')

# Redis key collection for results
RESULTS_ITEMS_KEY = os.getenv('REDIS_ITEMS_KEY', 'pidcheck:items')

# Redis details
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', 6379)

# Admin credentials for the API
ADMIN_USERNAME = os.getenv('ADMIN_USERNAME')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD')

# Batch size to process in results
BATCH_SIZE = os.getenv('BATCH_SIZE', 500)

# Configure basic logging
logger = logging.getLogger()
logger.setLevel(level=LOG_LEVEL)

# Redis server connector
redis = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)

def pop_result():
    """Pop a link check result from redis"""
    raw = redis.lpop(RESULTS_ITEMS_KEY)
    if raw:
        try:
            result = json.loads(raw)
        except Exception as e:
            logging.error("Coudn't parse JSON from result {0}".format(e))
            redis.lpush(RESULTS_ITEMS_KEY, raw)
            return None

        logging.info("Popped result for '{0}' with url '{1}'".format(result['pid'], result['checked_url']))
        return result
    else:
        return None

def push_result(result):
    """Push a link check result back to redis"""
    try:
        redis.rpush(RESULTS_ITEMS_KEY, result)
    except:
        # This is bad
        logging.error("While trying to store a link check result we failed storing back in redis. result: {0}".format(result))

def process_result(result):
    """Process a result into desired format and return other data"""
    # Doi is just the PID the crawler checked
    doi = result.get('pid')
    if not doi:
        raise RuntimeError("No pid found, can't store")

    status = result.get('http_status')

    # Checked date is directly from the crawler checked date
    checked_date = result.get('checked_date', None)

    # Content type directly from results
    content_type = result.get('content_type', '')

    # URL we checked
    url = result.get('checked_url', '')

    # PidCheck raw data has this as a very precise MS with floating points
    # So convert this into a float and round it.
    download_latency = round(float(result.get('download_latency', 0)))

    # Build the results into one object
    landing_page = {
        "checked": checked_date,
        "url": url,
        "status": status,
        "contentType": content_type,
        "error": result.get('error', ''),
        "redirectCount": result.get('redirect_count', 0),
        "redirectUrls": result.get('redirect_urls', []),
        "downloadLatency": download_latency,
        "hasSchemaOrg": bool(result.get('schema_org')),
        "schemaOrgId": result.get('schema_org_id'),
        "dcIdentifier": result.get('dc_identifier'),
        "citationDoi": result.get('citiation_doi'),
        "bodyHasPid": result.get('body_has_pid'),
    }

    return doi, landing_page

def send_result(doi, landing_page):
    """Send the result to datacite API for storage and updating matched doi"""

    credentials = ('%s:%s' % (ADMIN_USERNAME, ADMIN_PASSWORD))
    encoded_credentials = base64.b64encode(credentials.encode('ascii'))

    # Construct the payload for updating
    data = {
        "data" : {
            "type" : "dois",
            "attributes" : {
                "landingPage" : landing_page,
            }
        }
    }

    params = json.dumps(data).encode('utf8')

    headers = {
        "content-type": "application/json",
        "accept": "application/json"
    }
    url = API_ENDPOINT + doi
    req = urllib.request.Request(url, data=params, method='PATCH', headers=headers)
    req.add_header('Authorization', 'Basic %s' % encoded_credentials.decode("ascii"))

    try:
        with urllib.request.urlopen(req) as _:
            logger.info("Successfully pushed doi {0} link check results to DataCite API".format(doi))
    except urllib.error.HTTPError as e:
        r = json.loads(e.read())
        message = e.reason
        errors = r.get('errors')
        if errors:
            message = errors[0].get('title')

        raise RuntimeError("Failed to send result to DataCite API, reason: {0}".format(message))


def lambda_handler(event, context):
    """Lambda process handler"""

    # For each result of batch size
    for _ in range(BATCH_SIZE):
        # Pop the raw result from Redis
        raw_result = pop_result()

        # Only do anything if we got something
        if not raw_result:
            continue

        try:
            # Parse the data we want to store
            doi, landing_page = process_result(raw_result)

            # Send request to Lupo via /doi/DOI
            send_result(doi, landing_page)
        except Exception as e:
            logger.error("Unexpected error, pushing back onto redis, {0}".format(e))
            push_result(json.dumps(raw_result))

if __name__ == '__main__':
    # For local testing fake the arguments to lambda handler function
    event = []
    context = []
    lambda_handler(event, context)
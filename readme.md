Bernard
-------

## Description
DataCite serverless code that operates across our services.

## Lambdas

### Link Checking

Works in collaboration with the generic [PidCheck](https://github.com/datacite/pidcheck) service.
Serverless functions used for seeding and processing link checking data.

* check_links - Collects random samples from DataCite API and seeds into redis
* store_crawler_results - Collects results stored in redis by link checker crawler

### Analytics

Works in collaboration with the analytics service [Keeshond](https://github.com/datacite/keeshond)

* analytics_worker - Takes messages from SQS and spins up fargate tasks for report generation workers
* analytics_queue_reports - Retrieves what repositories have analytics set up and adds to SQS queue

## Building

./build.sh - This will produce zip files in the 'build' directory
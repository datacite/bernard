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

## Building

./build.sh - This will produce zip files in the 'build' directory
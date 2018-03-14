Bernard
-------

## Description
DataCite specific services for DOI link checking and processing.
Works in collaboration with the generic [PidCheck](https://github.com/datacite/pidcheck) service.

## Lambdas
Serverless functions used for seeding and processing link checking data.

* check_links - Collects random samples from DataCite API and seeds into redis

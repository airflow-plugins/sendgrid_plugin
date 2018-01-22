# Plugin - Sendgrid to S3

This plugin moves data from the [Sendgrid](https://sendgrid.com/docs/API_Reference/Web_API_v3) API to S3. Implemented for blocks API, bounces API, invalid emails API, spam reports API and global stats api.
## Hooks
### Sendgrid hook
This hook handles the authentication and request to Sendgrid. Inherits from [http-hook](https://airflow.apache.org/_modules/http_hook.html)

### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html) with the standard boto dependency.

## Operators
### SendgridToS3Operator
This operator composes the logic for this plugin. It fetches a specific endpoint and saves the result in a S3 Bucket, under a specified key, in
njson format. The parameters it can accept include the following.

- `sendgrid_conn_id`: The Airflow id used to store the Sendgrid credentials.
- `sendgrid_endpoint`: The Sendgrid endpoint we are fetching data from.
- `sendgrid_args`: Extra arguments for sendgrid, acording to documentation. For the `stats` you need to add here a start_date in YYYY-MM-DD format, otherwise it will default to 2017-01-01.
- `s3_conn_id`:  S3 connection id from Airflow.  
- `s3_bucket`: The output s3 bucket.  
- `s3_key`: The input s3 key.
- `s3_bucket`: The s3 bucket where the result should be stored

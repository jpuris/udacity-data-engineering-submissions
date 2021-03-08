# s3

## Pre-requisites
To successfully run the provided commands, one needs to have following tools installed
- [jq](https://stedolan.github.io/jq/), a lightweight and flexible command-line JSON processor
- [aws](https://aws.amazon.com/cli/), a Command Line Interface (CLI) tool to manage your AWS services

### Configuring AWS CLI
For more information, see [Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) docs.

## Customisation
The below variables will be used in setting up the EMR cluster. AWS profile is the name of profile configured in your aws cli configuration.

```shell
AWS_PROFILE='admin'
AWS_REGION='eu-north-1'
S3_BUCKET_NAME='udacity-data-engineer-nanodegree'
```

### See what region s3 bucket resides in

```shell
aws --profile $AWS_PROFILE s3api get-bucket-location \
--bucket $S3_BUCKET_NAME \
| jq
```

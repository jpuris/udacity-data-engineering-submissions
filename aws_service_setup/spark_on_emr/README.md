# Spark cluster on EMR

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
AWS_EC2_KEYPAIR_NAME='udacity-emr-cluster-keypair'
AWS_EMR_CLUSTER_NAME='udacity-emr-cluster'
```

### Create a keypair
Following command will create and download the a .pem file necessary to establish ssh connection to the cluster.
The key file will be created in your current directory.
```shell
aws --profile $AWS_PROFILE --region $AWS_REGION ec2 create-key-pair \
--key-name $AWS_EC2_KEYPAIR_NAME \
--query "KeyMaterial" \
--output text > $AWS_EC2_KEYPAIR_NAME.pem && \
chmod 400 $AWS_EC2_KEYPAIR_NAME.pem
```

### Listing existing EMR clusters
With following command you can check, if there are any clusters in ready state ("Waiting").
This is useful after the cluster creation command has been issued.
```shell
aws --profile $AWS_PROFILE emr --region $AWS_REGION list-clusters | jq '.Clusters[] | .Id + " " + .Name + " " + .Status.State'
```

### Creation of EMR cluster
Following command will create 3 node cluster with default roles, spark installed and EMR version 5.32.0.
"m5.xlarge' is the cheapest instance type available.
```shell
aws --profile $AWS_PROFILE --region $AWS_REGION emr create-cluster \
--name $AWS_EMR_CLUSTER_NAME \
--release-label emr-5.32.0 \
--applications Name=Spark \
--ec2-attributes KeyName=$AWS_EC2_KEYPAIR_NAME \
--instance-type m5.xlarge \
--instance-count 3 \
--use-default-roles
```

### Finding the hostname of Master node
```shell
# Replace CLUSTER_ID with the cluster's id you want to describe
aws --profile $AWS_PROFILE emr --region $AWS_REGION describe-cluster \
--cluster-id CLUSTER_ID \
| jq '.Cluster.MasterPublicDnsName'
```

### Shut down of an EMR cluster
This will terminate an EMR cluster with specified Cluster ID
```shell
# Replace CLUSTER_ID with proper cluster id you want to terminate
aws --profile $AWS_PROFILE emr --region $AWS_REGION terminate-clusters --cluster-ids CLUSTER_ID
```

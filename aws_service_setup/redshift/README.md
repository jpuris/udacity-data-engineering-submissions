# Redshift

## Pre-requisites
To successfully run the provided commands, one needs to have following tools installed
- [jq](https://stedolan.github.io/jq/) a lightweight and flexible command-line JSON processor
- [aws](https://aws.amazon.com/cli/) a Command Line Interface (CLI) tool to manage your AWS services

### Configuring AWS CLI
For more information, see [Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) docs.

## Customisation
The below variables will be used in setting up the Redshift cluster. AWS profile is the name of profile configured in your aws cli configuration.

```shell
AWS_PROFILE='admin'
AWS_REGION='eu-north-1'
# A unique identifier for the cluster.
REDSHIFT_CLUSTER_IDENTIFIER='udacity-redshift-cluster'
# The type of the cluster. When cluster type is specified as
# single-node , the NumberOfNodes parameter is not required.
# multi-node , the NumberOfNodes parameter is required.
REDSHIFT_CLUSTER_TYPE='multi-node'
# The node type to be provisioned for the cluster.
REDSHIFT_CLUSTER_NODE_TYPE='dc2.large'
# The number of compute nodes in the cluster.
# This parameter is required when the ClusterType parameter is specified as multi-node.
REDSHIFT_CLUSTER_NODE_NUM='3'
# The name of the first database to be created when the cluster is created.
REDSHIFT_DB_NAME='dwh'
# The user name associated with the master user account for the cluster that is being created.
REDSHIFT_MASTER_USERNAME='dwh_user'
# The password associated with the master user account for the cluster that is being created.
REDSHIFT_MASTER_PASSWORD='CHANGE-ME'
```

### Listing existing Redshift clusters
With following command you can check, if there are any clusters in ready state ("Waiting").
This is useful after the cluster creation command has been issued.
```shell
aws --profile $AWS_PROFILE redshift --region $AWS_REGION describe-clusters | jq '.Clusters[] | "Cluster Identifier: " + .ClusterIdentifier, "Cluster Status: " + .ClusterStatus, "Cluster availability status: " + .ClusterAvailabilityStatus'
```

### Creation of Redshift cluster
Following command will create Redshift cluster based on parameters from customisation part.

Warning! The below cluster creates _**publicly accessible**_ cluster.
If you do not want this, replace `--publicly-accessible` with `--no-publicly-accessible`
```shell
aws --profile $AWS_PROFILE --region $AWS_REGION redshift create-cluster \
--cluster-identifier $REDSHIFT_CLUSTER_IDENTIFIER \
--cluster-type $REDSHIFT_CLUSTER_TYPE \
--node-type $REDSHIFT_CLUSTER_NODE_TYPE \
--number-of-nodes $REDSHIFT_CLUSTER_NODE_NUM \
--db-name $REDSHIFT_DB_NAME \
--master-username $REDSHIFT_MASTER_USERNAME \
--master-user-password $REDSHIFT_MASTER_PASSWORD \
--publicly-accessible
```

### Finding the hostname of Master node
```shell
# Replace CLUSTER_ID with the cluster's id you want to describe
aws --profile $AWS_PROFILE redshift --region $AWS_REGION describe-clusters \
| jq '.Clusters[] | "Cluster Identifier: " + .ClusterIdentifier, "Address: " + .Endpoint.Address, "Port: " + (.Endpoint.Port|tostring)'
```

### Removal of Redshift cluster
This will terminate an EMR cluster with specified Cluster ID

Warning! The following command does not preserve final snapshot,
if you want it, replace `--skip-final-cluster-snapshot` with
```shell
--final-cluster-snapshot-identifier <value> \
--final-cluster-snapshot-retention-period <value>
```
Command to remove the cluster
```shell
aws --profile $AWS_PROFILE redshift --region $AWS_REGION delete-cluster \
--cluster-identifier $REDSHIFT_CLUSTER_IDENTIFIER \
--skip-final-cluster-snapshot
```

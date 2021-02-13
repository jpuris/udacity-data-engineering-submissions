import boto3
import json
import configparser
import argparse
from time import sleep
import sys


def get_config(conf_file, section=None):
    """Reads init type configuration file.
    Returns JSON with config detail

    :param conf_file: string containing file path
    :param section: (Optional) string containing section name in config file
    :return: dict
    """
    parser = configparser.ConfigParser()
    parser.read_file(open(conf_file))
    if not section:
        return {sect: dict(parser.items(sect)) for sect in parser.sections()}
    else:
        return dict(parser.items(section))


def create_cluster(redshift, iam, ec2, cluster_config, wait_status=False):
    """ Create publicly available redshift cluster per provided cluster configuration.

    :param redshift: boto.redshift object to use
    :param iam: boto.iam object to use
    :param ec2: boto.ec2 object to use
    :param cluster_config: configparser cluster configuration, from manage_cluster.cfg file
    :param wait_status: bool, default is False. Should function wait and repeatedly check if cluster has
    reached its desired state.
    :return: Returns JSON, if successful, otherwise displays error and returns integer 1
    """

    print("Attempting to create a new IAM Role")
    iam_role_name = cluster_config['iam_role_name']
    try:
        iam.create_role(
            Path='/',
            RoleName=iam_role_name,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [
                    {
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {
                            'Service': 'redshift.amazonaws.com'
                        }
                    }
                ],
                'Version': '2012-10-17'
            })
        )
        print(f"Role '{iam_role_name}' created")
    except iam.exceptions.EntityAlreadyExistsException:
        print("Role already exists")

    print("Attaching AmazonS3ReadOnlyAccess policy to the role")
    iam.attach_role_policy(
        RoleName=iam_role_name,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )

    print("Retrieving role ARN")
    aws_role_arn = iam.get_role(RoleName=cluster_config['iam_role_name'])['Role']['Arn']
    print(f"Role ARN: {aws_role_arn}")

    try:
        redshift.create_cluster(
            # HW
            ClusterType=cluster_config['cluster_type'],
            NodeType=cluster_config['node_type'],
            NumberOfNodes=int(cluster_config['num_nodes']),

            # Identifiers & Credentials
            DBName=cluster_config['db_name'],
            ClusterIdentifier=cluster_config['cluster_identifier'],
            MasterUsername=cluster_config['db_user'],
            MasterUserPassword=cluster_config['db_password'],

            # Roles (for s3 access)
            IamRoles=[aws_role_arn]
        )
    except Exception as e:
        print(f"ERROR: {e}")
        return 1

    if wait_status:
        expected_status = 'available'
    else:
        expected_status = None
    cluster_info = get_cluster_status(
        redshift,
        cluster_config['cluster_identifier'],
        expected_status=expected_status
    )
    print(f"DWH_ENDPOINT :: {cluster_info['Endpoint']['Address']}")
    print(f"DWH_ROLE_ARN :: {cluster_info['IamRoles'][0]['IamRoleArn']}")

    vpc_id = cluster_info['VpcId']
    vpc_cidr_ip = '0.0.0.0/0'
    vpc_ip_proto = 'TCP'
    vpc_port = int(cluster_config['db_port'])
    try:
        vpc = ec2.Vpc(id=vpc_id)
        default_sec_group = list(vpc.security_groups.all())[0]
        print(default_sec_group)
        default_sec_group.authorize_ingress(
            GroupName=default_sec_group.group_name,
            CidrIp=vpc_cidr_ip,
            IpProtocol=vpc_ip_proto,
            FromPort=vpc_port,
            ToPort=vpc_port
        )
        print(f"VPC {vpc_id} access has been granted to {vpc_ip_proto} {vpc_cidr_ip} "
              f"for port {vpc_port}")
    except Exception as e:
        print(f"ERROR: {e}")
        return 1


def check_status(redshift, cluster_id, expected_status=None):
    """ Check redshift cluster state with optional wait and recheck until in desired state.

    :param redshift: boto.redshift. Redshift object to use to communicate with AWS
    :param cluster_id: string. Cluster ID
    :param expected_status: If provided, will repeat check until cluster status matches value
    :return:
    """
    try:
        cluster_status = redshift.describe_clusters(ClusterIdentifier=cluster_id)['Clusters'][0]
    except Exception as e:
        if e.response['Error']['Code'] == 'ClusterNotFound':
            if expected_status == 'deleted':
                print(f"Cluster '{cluster_id}' has been deleted")
            print(e.response['Error']['Message'])
        else:
            print(f"ERROR: {e.response}")
        sys.exit(0)
    return cluster_status


def get_cluster_status(redshift, cluster_id=None, expected_status=None):
    cluster_info = check_status(redshift, cluster_id, expected_status=expected_status)
    cluster_status = cluster_info['ClusterStatus']
    if expected_status and cluster_status != expected_status:
        print(f"Cluster status is in '{cluster_status}' state, ",
              f"expected '{expected_status}'. Sleeping for 30 seconds...")
        while cluster_status != expected_status:
            sleep(30)
            cluster_info = check_status(redshift, cluster_id)
            cluster_status = cluster_info['ClusterStatus']
            print(f"Cluster status is in '{cluster_status}' state, ",
                  f"expected '{expected_status}'. Sleeping for 30 seconds...")
    else:
        print(f"Cluster is in '{cluster_status}' state")
        return cluster_info

    print(f"Cluster is in '{cluster_status}' state")
    return cluster_info


def delete_cluster(redshift, iam, cluster_id, iam_role_name, wait_status=False):
    """ Removes redshift cluster matching provided cluster_id as well as iam roles matching provided iam_role_name

    :param redshift: boto.redshift. Redshift object to use to communicate with AWS
    :param iam: boto.iam. IAM object to use to communicate with AWS
    :param cluster_id: string. Redshift cluster ID
    :param iam_role_name: boto.iam. IAM object to use to communicate with AWS
    :param wait_status: bool, default is False. Should function wait and repeatedly check if cluster has
    reached its desired state.
    :return: None
    """
    try:
        redshift.delete_cluster(
            ClusterIdentifier=cluster_id,
            SkipFinalClusterSnapshot=True
        )
        print(f"Cluster with id [{cluster_id}] has been scheduled for deletion")
    except Exception as e:
        print(f'ERROR: {e}')
        return None

    if wait_status:
        expected_status = 'deleted'
    else:
        expected_status = None
    get_cluster_status(redshift, cluster_id, expected_status=expected_status)

    print(f"Removing '{iam_role_name}' role")
    iam.detach_role_policy(
        RoleName=iam_role_name,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    iam.delete_role(RoleName=iam_role_name)
    print(f"Role '{iam_role_name}' has been removed")


def main():
    """ Crude IOC (Infrastructure as code) implementation with Boto. Uses provided configuration file to
    create or delete redshift cluster including IAM role.
    Can also be used to check cluster status by its ID.
    :return: None
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=['create', 'status', 'delete'],
                        help="cluster management function")
    parser.add_argument("-c", "--config", default='manage_cluster.cfg',
                        help="config file")
    parser.add_argument("-w", "--wait", action="store_true",
                        help="wait until operation is done")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="increase output verbosity")
    args = parser.parse_args()

    # Load DWH Params from a file
    aws_config = get_config(args.config, section="AWS")
    cluster_config = get_config(args.config, section="CLUSTER")

    # Create clients for IAM, EC2, S3 and Redshift
    aws_client_params = {
        "region_name": aws_config['region_name'],
        "aws_access_key_id": aws_config['key'],
        "aws_secret_access_key": aws_config['secret'],
    }

    ec2 = boto3.resource('ec2', **aws_client_params)
    iam = boto3.client('iam', **aws_client_params)
    redshift = boto3.client('redshift', **aws_client_params)

    if args.action == 'create':
        create_cluster(redshift, iam, ec2, cluster_config, wait_status=args.wait)

    if args.action == 'status':
        get_cluster_status(redshift, cluster_config['cluster_identifier'])

    if args.action == 'delete':
        delete_cluster(redshift, iam,
                       cluster_config['cluster_identifier'],
                       cluster_config['iam_role_name'],
                       wait_status=args.wait)


if __name__ == '__main__':
    main()

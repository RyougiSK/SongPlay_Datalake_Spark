import logging
import boto3
from botocore.exceptions import ClientError
import configparser
import os

def check_buckets(region, KEY, SECRET, bucket_name):
    """
    Check if a bucket exists in Amazon S3.

    :param region: String region to create the S3 resource in, e.g., 'us-west-2'
    :param KEY: AWS access key ID
    :param SECRET: AWS secret access key
    :param bucket_name: Name of the bucket to check
    :return: True if the bucket exists, False otherwise
    """

    # Create an S3 resource with the specified region and credentials
    s3 = boto3.resource('s3',
                        region_name=region,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

    # Retrieve the list of buckets
    response = s3.list_buckets()

    # Check if the specified bucket name is in the list of buckets
    if bucket_name in response['Buckets']["Name"]:
        return True
    else:
        return False


def create_bucket(region, KEY, SECRET, bucket_name, location):
    """
    Create an S3 bucket in a specified region.

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param region: String region to create the S3 bucket in, e.g., 'us-west-2'
    :param KEY: AWS access key ID
    :param SECRET: AWS secret access key
    :param bucket_name: Name of the bucket to create
    :param location: Dictionary specifying the location constraint for the bucket
    :return: True if the bucket was successfully created, False otherwise
    """

    try:
        # Create an S3 client with the specified region and credentials
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True
    
def main():
    """
    Main function that checks if a bucket exists and creates it if it doesn't.

    The function reads AWS credentials from a configuration file, checks if the
    specified bucket exists, and creates it if it doesn't.

    :return: None
    """

    # Read AWS credentials from the configuration file
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    KEY = config['S3']['AWS_ACCESS_KEY_ID']
    SECRET = config['S3']['AWS_SECRET_ACCESS_KEY']
    
    region = "us-west-2"
    location = {'LocationConstraint': region}
    bucket_name = "datalake-target-s3"
    
    # Check if the bucket already exists
    check_buckets_result = check_buckets(region, KEY, SECRET, bucket_name)
    
    if check_buckets_result == False:
        # Create the bucket if it doesn't exist
        create_bucket(region, KEY, SECRET, bucket_name, location)
    else:
        pass
    

if __name__ == "__main__":
    main()

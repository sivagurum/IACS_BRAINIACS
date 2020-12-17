import sys

try:
    import boto3
    import botocore
    import pandas as pd

    from tabulate import tabulate

except ImportError as e:
    print("Oops!", e, "occurred.")
    print("Please install necessary module to use this tool.")
    sys.exit(1)


class CommonS3:

    def __init__(self, df):
        self.df = df

    @classmethod
    def check_bucket(self, bucket_name, s3_resource):
        try:
            head_s3 = s3_resource.meta.client.head_bucket(Bucket=bucket_name)
            #print(head_s3)
            df.loc[df['BUCKET_NAME'] == bucket_name, "BUCKET_STATUS"] = "SUCCESS"
            df.loc[df['BUCKET_NAME'] == bucket_name, "BUCKET_REGION"] = head_s3['ResponseMetadata']['HTTPHeaders'][
                'x-amz-bucket-region']
            # print("Bucket Exists!")
            return True
        except botocore.exceptions.ClientError as e:
            # print(e.response)
            df.loc[df['BUCKET_NAME'] == bucket_name, "BUCKET_STATUS"] = e.response['Error']['Message']
            return False
        #     # If a client error is thrown, then check that it was a 404 error.
        #     # If it was a 404 error, then the bucket does not exist.
        #     error_code = int(e.response['Error']['Code'])
        #     if error_code == 403:
        #         print("Private Bucket. Forbidden Access!")
        #         return True
        #     elif error_code == 404:
        #         print("Bucket Does Not Exist!")
        #         return False
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the check_bucket()")
            return False

    @classmethod
    def insert_bucket_policy(self, bucket_name, s3_client):
        try:
            result = s3_client.get_bucket_policy(Bucket=bucket_name)
            # print(result)
            df.loc[df['BUCKET_NAME'] == bucket_name, "POLICIES"] = result['Policy']
        except botocore.exceptions.ClientError as e:
            # print(e.response)
            df.loc[df['BUCKET_NAME'] == bucket_name, "BUCKET_REGION"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the insert_bucket_policy()")

    @classmethod
    def insert_access_control_list(self, bucket_name, s3_client):
        try:
            result = s3_client.get_bucket_acl(Bucket=bucket_name)
            df.loc[df['BUCKET_NAME'] == bucket_name, "ACCESS_CONTROL_LIST"] = result['Grants']
        except botocore.exceptions.ClientError as e:
            # print(e.response)
            df.loc[df['BUCKET_NAME'] == bucket_name, "ACCESS_CONTROL_LIST"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the insert_access_control_list()")

    @classmethod
    def insert_bucket_replication(self, bucket_name, s3_client):
        try:
            result = s3_client.get_bucket_replication(Bucket=bucket_name)
            # print(result)
            first_replication_bucket = result['ReplicationConfiguration']['Rules'][0]['Destination']['Bucket']
            df.loc[df['BUCKET_NAME'] == bucket_name, "REPLICATION_BUCKET"] = str(first_replication_bucket).split(':')[5]
        except botocore.exceptions.ClientError as e:
            # print(e.response)
            df.loc[df['BUCKET_NAME'] == bucket_name, "REPLICATION_BUCKET"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the insert_bucket_replication()")

    @classmethod
    def insert_bucket_notification(self, bucket_name, s3_client):
        try:
            result = s3_client.get_bucket_notification_configuration(Bucket=bucket_name)
            #print(result)
            first_notification = result.get('TopicConfigurations', [{'TopicArn': None}])[0]['TopicArn']
            df.loc[df['BUCKET_NAME'] == bucket_name, "NOTIFICATION"] = first_notification
        except botocore.exceptions.ClientError as e:
            # print(e.response)
            df.loc[df['BUCKET_NAME'] == bucket_name, "NOTIFICATION"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the insert_bucket_notification()")


if __name__ == "__main__":
    try:
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client('s3')

        df = pd.DataFrame(
            columns=['BUCKET_NAME', 'BUCKET_STATUS', 'BUCKET_REGION', 'REPLICATION_BUCKET', 'ACCESS_CONTROL_LIST',
                     'POLICIES', 'NOTIFICATION'])
        c = CommonS3(df)

        for bucket in s3_client.list_buckets()["Buckets"]:
            bucket_name = bucket["Name"]
            df = df.append({'BUCKET_NAME': bucket_name}, ignore_index=True)
            c.check_bucket(bucket_name, s3_resource)
            c.insert_bucket_policy(bucket_name, s3_client)
            c.insert_access_control_list(bucket_name, s3_client)
            c.insert_bucket_replication(bucket_name, s3_client)
            c.insert_bucket_notification(bucket_name, s3_client)

        print(tabulate(df, headers='keys', tablefmt='psql'))

    except Exception as e:
        print("Oops!", e, ". Check the __main__() Function")

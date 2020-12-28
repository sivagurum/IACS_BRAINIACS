import sys

try:
    import boto3
    import botocore
    import pandas as pd
    from io import StringIO
    import numpy as np
    from  datetime import datetime, timedelta
    from tabulate import tabulate
    from matplotlib import pyplot as plt

except ImportError as e:
    print("Oops!", e, "occurred.")
    print("Please install necessary module to use this tool.")
    sys.exit(1)


class CommonS3:

    def __init__(self, df):
        self.df = df
    
    @classmethod
    def writeData(self, df, bucket, objectname,  s3_resource):
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)        
            s3_resource.Object(bucket, objectname).put(Body=csv_buffer.getvalue())
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the writeData()")

    @classmethod
    def buildQuery(self, bucketname, period, start_time, end_time, metricVar, storagetype, stat='Average'):
        response = client.get_metric_statistics(
                    Namespace = 'AWS/S3',
                    Period = period,
                    StartTime = start_time,  
                    EndTime = end_time, 
                    MetricName = metricVar,
                    Statistics=[stat],
                    Dimensions = [
                    {'Name': 'BucketName', 'Value': bucketname},
                    {'Name': 'StorageType', 'Value': storagetype}
                ])

        return response
    @classmethod
    def buildQuery2(self, bucketname, period, start_time, end_time, metricVar, filtervalue, stat='Sum'):
        response = client.get_metric_statistics(
                    Namespace = 'AWS/S3',
                    Period = period,
                    StartTime = start_time,  
                    EndTime = end_time, 
                    MetricName = metricVar,
                    Statistics=[stat],
                    Dimensions = [
                    {'Name': 'BucketName', 'Value': bucketname},
                    {'Name': 'FilterId', 'Value': filtervalue}
                ])

        return response

    @classmethod
    def Metricutilization(self, df, response, bucketname,mvalue, column_name):
        metricvalue ={}

        for i in response['Datapoints']:
            if 'BUCKETSIZE' in column_name:
                value = i[mvalue]/1024/1024
                metricvalue['Timestamp'] = value
            else:
                metricvalue['Timestamp'] = i[mvalue]
        for key in sorted(metricvalue.keys()): 
            df.loc[df['BUCKET_NAME']  == bucketname, column_name ] = metricvalue[key]
        

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
            #df.loc[df['BUCKET_NAME'] == bucket_name, "POLICIES"] = e.response['Error']['Message']
            df.loc[df['BUCKET_NAME'] == bucket_name, "POLICIES"] = 'NA'
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the insert_bucket_policy()")

    @classmethod
    def insert_access_control_list(self, bucket_name, s3_client):
        try:
            result = s3_client.get_bucket_acl(Bucket=bucket_name)
            df.loc[df['BUCKET_NAME'] == bucket_name, "ACCESS_CONTROL_LIST"] = result['Grants']
        except botocore.exceptions.ClientError as e:
            # print(e.response)
            #df.loc[df['BUCKET_NAME'] == bucket_name, "ACCESS_CONTROL_LIST"] = e.response['Error']['Message']
            df.loc[df['BUCKET_NAME'] == bucket_name, "ACCESS_CONTROL_LIST"] = 'NA'
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
            #df.loc[df['BUCKET_NAME'] == bucket_name, "REPLICATION_BUCKET"] = e.response['Error']['Message']
            df.loc[df['BUCKET_NAME'] == bucket_name, "REPLICATION_BUCKET"] = 'NA'
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the insert_bucket_replication()")

    @classmethod
    def insert_bucket_notification(self, bucket_name, s3_client):
        try:
            result = s3_client.get_bucket_notification_configuration(Bucket=bucket_name)
            #print(result)
            first_notification = result.get('TopicConfigurations', [{'TopicArn': 'NA'}])[0]['TopicArn']
            df.loc[df['BUCKET_NAME'] == bucket_name, "NOTIFICATION"] = first_notification
        except botocore.exceptions.ClientError as e:
            # print(e.response)
            #df.loc[df['BUCKET_NAME'] == bucket_name, "NOTIFICATION"] = e.response['Error']['Message']
            df.loc[df['BUCKET_NAME'] == bucket_name, "NOTIFICATION"] = 'NA'
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the insert_bucket_notification()")
            df.loc[df['BUCKET_NAM'] == bucket_name, "NOTIFICATION"] = 'NA'


if __name__ == "__main__":
    try:
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client('s3')
        client = boto3.client(service_name='cloudwatch', region_name='us-east-1')

        # Defining Bucket Details 
        outputbucket='brain-iacs-east'
        objectname='S3-metrics/s3metrics.csv'

        

        df = pd.DataFrame(
            columns=['BUCKET_NAME', 'BUCKET_STATUS', 'BUCKET_REGION', 'REPLICATION_BUCKET', 'ACCESS_CONTROL_LIST',
                     'POLICIES', 'NOTIFICATION'])
        c = CommonS3(df)

        for bucket in s3_client.list_buckets()["Buckets"]:
            # Defining Time Periods 
            start= datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            start_time = start - timedelta(days=1)
            request_start_time = datetime.utcnow() - timedelta(minutes=15)
            end_time = datetime.utcnow()

            bucket_name = bucket["Name"]
            df = df.append({'BUCKET_NAME': bucket_name}, ignore_index=True)
            c.check_bucket(bucket_name, s3_resource)
            c.insert_bucket_policy(bucket_name, s3_client)
            c.insert_access_control_list(bucket_name, s3_client)
            c.insert_bucket_replication(bucket_name, s3_client)
            c.insert_bucket_notification(bucket_name, s3_client)

            s3size_response = c.buildQuery(bucket_name,86400,start_time, end_time, 'BucketSizeBytes', 'StandardStorage')
            s3object_response = c.buildQuery(bucket_name,86400,start_time, end_time, 'NumberOfObjects', 'AllStorageTypes')
            allreq_response = c.buildQuery2(bucket_name,900,request_start_time, end_time, 'AllRequests', 'GETREQUEST')
            fourxx_response = c.buildQuery2(bucket_name,900,request_start_time, end_time, '4xxErrors', 'GETREQUEST')
            get_req_response= c.buildQuery2(bucket_name,900,request_start_time, end_time, 'GetRequests', 'GETREQUEST')
            put_req_response =c.buildQuery2(bucket_name,900,request_start_time, end_time, 'PutRequests', 'GETREQUEST')
            #
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_colwidth', 200)
            #print(df)
            conditions_b=((df['POLICIES']=='NA'),(df['POLICIES']!='NA'))
            conditions_a=((df['ACCESS_CONTROL_LIST']=='NA'),(df['ACCESS_CONTROL_LIST']!='NA'))
            conditions_n=((df['NOTIFICATION']=='NA'),(df['NOTIFICATION']!='NA'))
            conditions_r=((df['REPLICATION_BUCKET']=='NA'),(df['REPLICATION_BUCKET']!='NA'))
            choice_list=('NO','YES')
            df['BUCKET_POLICY_ENABLED']=np.select(conditions_b,choice_list)
            df['ACL_ENABLED']=np.select(conditions_a,choice_list)
            df['NOTIFICATION_SET']=np.select(conditions_n,choice_list)
            df['REPLICATION_BUCKET_ENABLED']=np.select(conditions_r,choice_list)
            
            # parsing the Cloud Watch Metrics          

            c.Metricutilization(df,s3size_response,bucket_name,'Average','BUCKET_SIZE')
            c.Metricutilization(df,s3object_response,bucket_name,'Average','TOTAL_OBJECTS')
            c.Metricutilization(df,allreq_response,bucket_name,'Sum','ALL_REQ_COUNT')
            c.Metricutilization(df,fourxx_response,bucket_name,'Sum','4XX_REQ_COUNT')
            c.Metricutilization(df,get_req_response,bucket_name,'Sum','GET_REQ_COUNT')
            c.Metricutilization(df,put_req_response,bucket_name,'Sum','PUT_REQ_COUNT')
            

        print(tabulate(df, headers='keys', tablefmt='psql'))
        # Writing Data to S3 Bukcet 
        c.writeData(df,outputbucket,objectname, s3_resource)

    except Exception as e:
        print("Oops!", e, ". Check the __main__() Function")

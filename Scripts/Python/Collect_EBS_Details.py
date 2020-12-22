import boto3
import botocore
from datetime import datetime, timedelta
import pandas as pd
from tabulate import tabulate
import sys

class MetricParser:
    def __init__(self, df):
        self.cpu_util_df = df

    @classmethod
    def build_query(self, volume_id, start_time, end_time, metricVar):
        response = client.get_metric_statistics(
            Namespace='AWS/EBS',
            Period=900,
            StartTime=start_time,
            EndTime=end_time,
            MetricName=metricVar,
            Statistics=['Maximum'],
            Dimensions=[
                {'Name': 'VolumeId', 'Value': volume_id}
            ])

        return response

    @classmethod
    def logic_builder(self, cpu_util_df, volume_id, metricVar):
        try:
            # Get tPe Volume Status Based on the onehour_utils Value
            cpu_util_df.loc[cpu_util_df['VolumeId'] == volume_id, 'VOLUME_STATUS'] = cpu_util_df[str(metricVar+'1')].apply(
                lambda x: 'OVER_UTILIZED' if x > 150 else 'NORMAL')
        except botocore.exceptions.ClientError as e:
            cpu_util_df.loc[cpu_util_df['VolumeId'] == volume_id, "VOLUME_STATUS"] = e.response['Error'][
                'Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the getInstanceDetails()")

    @classmethod
    def volume_utilization(self, cpu_util_df, response, volume_id, metricVar):
        metricvalue = {}
        for i in response['Datapoints']:
            metricvalue[i['Timestamp']] = i['Maximum']

        #print(f"Length of DataPoints in the Metric {len(metricvalue.keys())}")
        # for key in sorted(metricvalue.keys()):
        #     print("%s: %s" % (key, metricvalue[key]))
        hour = 1
        for key in sorted(metricvalue.keys()):
            cpu_util_df.loc[cpu_util_df['VolumeId'] == volume_id, str(metricVar+str(hour))] = metricvalue[key]
            hour = hour + 1


if __name__ == "__main__":
    try:
        # Ccreate Cloudwatch Object
        client = boto3.client(service_name='cloudwatch', region_name='us-east-1')
        ec2_resource = boto3.resource('ec2')
        ec2_client = boto3.client('ec2')
        paginator = client.get_paginator('list_metrics')

        #metricVar = 'VolumeIdleTime'
        #metricVar= 'VolumeQueueLength'
        metricVar = 'VolumeWriteOps'
        columns = ['VolumeId', 'VOLUME_STATUS', metricVar+'1']

        cpu_util_df = pd.DataFrame(columns=columns)

        cw_collector = MetricParser(cpu_util_df)

        # Listing all the avaialble ebs instance
        for response in paginator.paginate(MetricName=metricVar, Namespace='AWS/EBS'):
            for res in response['Metrics']:
                volume_id = res.get('Dimensions')[0].get('Value')
                cpu_util_df = cpu_util_df.append({'VolumeId': volume_id}, ignore_index=True)
                start_time = datetime.utcnow() - timedelta(minutes=15)
                end_time = datetime.utcnow()
                response = cw_collector.build_query(volume_id, start_time, end_time, metricVar)
                #print(response)
                cw_collector.volume_utilization(cpu_util_df, response, volume_id, metricVar)

                # Calculating the Status
                cw_collector.logic_builder(cpu_util_df, volume_id, metricVar)

        print(tabulate(cpu_util_df, headers='keys', tablefmt='psql'))

    except Exception as e:
        print("Oops!", e, ". Check the __main__() Function")

# dd if=/dev/zero of=/dev/null

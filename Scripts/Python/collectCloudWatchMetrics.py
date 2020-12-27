import boto3
import botocore
from  datetime import datetime, timedelta
from collections import  OrderedDict
import json
import pandas as pd
import numpy as np
from io import StringIO
from tabulate import tabulate

class MetricParser:
    def __init__(self, df):
        self.cpu_util_df = df

    def writeData(self, df, bucket, objectname,  s3_resource):
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)        
            s3_resource.Object(bucket, objectname).put(Body=csv_buffer.getvalue())
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the writeData()")


    def buildQuery(self, instance_id, period, stat_time, end_time, metricVar, stat='Maximum'):
        response = client.get_metric_statistics(
                    Namespace = 'AWS/EC2',
                    Period = period,
                    StartTime = start_time,  
                    EndTime = end_time, 
                    MetricName = metricVar,
                    Statistics=[stat],
                    Dimensions = [
                    {'Name': 'InstanceId', 'Value': instance_id}
                ])

        return response

    def logicBuilder(self,cpu_util_df,instance_id):
        try:            
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'CPU_UTIL_STATUS' ] =  cpu_util_df['CPU1'].apply(lambda x: 'OVER_UTILIZED' if x > 80 else 'AVAILABLE')
        except botocore.exceptions.ClientError as e:
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID'] == instance_id, "CPU_UTIL_STATUS"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the getInstanceDetails()")

    def StatusCheck(self,cpu_util_df,instance_id):
        try:            
            # Calculating Average Value 
            #cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'one_hour_utils' ]  = ( cpu_util_df['P1'] + cpu_util_df['P2'] + cpu_util_df['P3'] + cpu_util_df['P4'] + cpu_util_df['P5'] + cpu_util_df['P6'] + cpu_util_df['P7'] + cpu_util_df['P8'] + cpu_util_df['P9'] + cpu_util_df['P10'] + cpu_util_df['P11'] ) / 11
            #cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'one_hour_utils' ]  = ( cpu_util_df['P1'] + cpu_util_df['P2'] + cpu_util_df['P3'] + cpu_util_df['P4']  ) / 11
            # Get tPe Cpu Status Based on the onehour_utils Value 
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'STATUSCHECK' ] =  cpu_util_df['STATUS_VALUE'].apply(lambda x: 'PASSED' if x == 0 else 'FAILED')
        except botocore.exceptions.ClientError as e:
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID'] == instance_id, "STATUSCHECK"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the getInstanceDetails()")

    def Metricutilization(self, df, response, instance_id,column_name):
        network_value = {}
        for i in response['Datapoints']:
            network_value[i['Timestamp']] = i['Maximum']
        #print(f"Length of DataPoints in the Metric {len(network_value.keys())}")
        hour = 1
        for key in sorted(network_value.keys()):
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, column_name ] = network_value[key]
            hour = hour - 1



    def cpuUtilization(self, cpu_util_df, response, instance_id):
        metricvalue = {}
        for i in response['Datapoints']:
            metricvalue[i['Timestamp']] = i['Maximum']        
        
        print(f"Length of DataPoints in the Metric {len(metricvalue.keys())}")
        # for key in sorted(metricvalue.keys()):
        #     print("%s: %s" % (key, metricvalue[key]))
        hour = 1
        for key in sorted(metricvalue.keys()):
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'CPU'+str(hour) ] = metricvalue[key]
            hour = hour - 1

        
     



if __name__ == "__main__":
    try:
        # Ccreate Cloudwatch Object 
        client = boto3.client(service_name='cloudwatch', region_name='us-east-1')
        ec2_resource = boto3.resource('ec2')
        ec2_client = boto3.client('ec2')
        s3_resource = boto3.resource('s3')

        # Defining Bucket Details 
        bucket='brain-iacs-east'
        objectname='ec2-metrics/cloudwatchmetric.csv'

        # Define the DataFrame Columns 
        columns= ['INSTANCE_ID',  'CPU_UTIL_STATUS',  'STATUSCHECK', 'CPU1', 'NETIN', 'NETOUT', 'DISKREAD', 'DISKWRITE', 'STATUS_VALUE']
        cpu_util_df = pd.DataFrame(columns= columns)

        cw_collector = MetricParser(cpu_util_df)

        # Listing all the avaialble ec2 instance
        for instance in ec2_resource.instances.all():
            instance_id = instance.id
            cpu_util_df = cpu_util_df.append({'INSTANCE_ID': instance_id}, ignore_index=True)

            # Metrics Start And End Time 
            start_time = datetime.utcnow() -  timedelta(minutes = 15)
            end_time = datetime.utcnow()
            # Setting the DataPoints Period 0f 15 Minutes 15*60 = 900 
            period = 900
            response = cw_collector.buildQuery( instance_id, period,start_time, end_time,'CPUUtilization')
            #print(response)
            #
            # Collecting Response from the Cloud Watch 
            network_in_response = cw_collector.buildQuery( instance_id, period,start_time, end_time,'NetworkIn', )
            network_out_response = cw_collector.buildQuery( instance_id, period,start_time, end_time,'NetworkOut', )
            diskRead_response = cw_collector.buildQuery( instance_id, period,start_time, end_time,'DiskReadBytes')
            diskWrite_response = cw_collector.buildQuery( instance_id, period,start_time, end_time,'DiskWriteBytes')
            status_response = cw_collector.buildQuery( instance_id, period,start_time, end_time,'StatusCheckFailed')
            #print(status_response)
            # Parsing The Response and Add it To DataFrame 
            cw_collector.cpuUtilization(cpu_util_df,response,instance_id)
            cw_collector.Metricutilization(cpu_util_df,network_in_response,instance_id,'NETIN')
            cw_collector.Metricutilization(cpu_util_df,network_in_response,instance_id,'NETOUT')
            cw_collector.Metricutilization(cpu_util_df,diskRead_response,instance_id,'DISKREAD')
            cw_collector.Metricutilization(cpu_util_df,diskWrite_response,instance_id,'DISKWRITE')
            cw_collector.Metricutilization(cpu_util_df,diskWrite_response,instance_id,'STATUS_VALUE')
                      
            
            # Calculating the Status
            cw_collector.logicBuilder(cpu_util_df,instance_id)
            cw_collector.StatusCheck(cpu_util_df,instance_id)
        #       
        print(tabulate(cpu_util_df, headers='keys', tablefmt='psql'))
        # Writing Data to S3 Bukcet 
        cw_collector.writeData(cpu_util_df,bucket,objectname, s3_resource)


    except Exception as e:
        print("Oops!", e, ". Check the __main__() Function")


        


# dd if=/dev/zero of=/dev/null

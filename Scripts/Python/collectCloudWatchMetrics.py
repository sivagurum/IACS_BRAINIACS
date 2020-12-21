import boto3
import botocore
from  datetime import datetime, timedelta
from collections import  OrderedDict
import json
import pandas as pd
import numpy as np
from tabulate import tabulate

class MetricParser:
    def __init__(self, df):
        self.cpu_util_df = df

    def buildQuery(self, instance_id, stat_time, end_time, metricVar):
        response = client.get_metric_statistics(
                    Namespace = 'AWS/EC2',
                    Period = 900,
                    StartTime = start_time,  
                    EndTime = end_time, 
                    MetricName = metricVar,
                    Statistics=['Maximum'],
                    Dimensions = [
                    {'Name': 'InstanceId', 'Value': instance_id}
                ])

        return response

    def logicBuilder(self,cpu_util_df,instance_id):
        try:            
            # Calculating Average Value 
            #cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'one_hour_utils' ]  = ( cpu_util_df['P1'] + cpu_util_df['P2'] + cpu_util_df['P3'] + cpu_util_df['P4'] + cpu_util_df['P5'] + cpu_util_df['P6'] + cpu_util_df['P7'] + cpu_util_df['P8'] + cpu_util_df['P9'] + cpu_util_df['P10'] + cpu_util_df['P11'] ) / 11
            #cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'one_hour_utils' ]  = ( cpu_util_df['P1'] + cpu_util_df['P2'] + cpu_util_df['P3'] + cpu_util_df['P4']  ) / 11
            # Get tPe Cpu Status Based on the onehour_utils Value 
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'CPU_UTIL_STATUS' ] =  cpu_util_df['P1'].apply(lambda x: 'OVER_UTILIZED' if x > 80 else 'AVAILABLE')
        except botocore.exceptions.ClientError as e:
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID'] == instance_id, "CPU_UTIL_STATUS"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the getInstanceDetails()")




    def cpuUtilization(self, cpu_util_df, response, instance_id):
        metricvalue = {}
        for i in response['Datapoints']:
            metricvalue[i['Timestamp']] = i['Maximum']        
        
        print(f"Length of DataPoints in the Metric {len(metricvalue.keys())}")
        # for key in sorted(metricvalue.keys()):
        #     print("%s: %s" % (key, metricvalue[key]))
        hour = 1
        for key in sorted(metricvalue.keys()):
            cpu_util_df.loc[cpu_util_df['INSTANCE_ID']  == instance_id, 'P'+str(hour) ] = metricvalue[key]
            hour = hour - 1

        
     



if __name__ == "__main__":
    try:
        # Ccreate Cloudwatch Object 
        client = boto3.client(service_name='cloudwatch', region_name='us-east-1')
        ec2_resource = boto3.resource('ec2')
        ec2_client = boto3.client('ec2')

        metricVar = 'CPUUtilization'
        #columns= ['INSTANCE_ID',  'CPU_UTIL_STATUS', 'P11', 'P10', 'P9','P8' , 'P7','P6','P5', 'P4', 'P3', 'P2', 'P1']
        columns= ['INSTANCE_ID',  'CPU_UTIL_STATUS',  'P1']
        cpu_util_df = pd.DataFrame(columns= columns)

        cw_collector = MetricParser(cpu_util_df)

        # Listing all the avaialble ec2 instance
        for instance in ec2_resource.instances.all():
            instance_id = instance.id
            cpu_util_df = cpu_util_df.append({'INSTANCE_ID': instance_id}, ignore_index=True)
            start_time = datetime.utcnow() -  timedelta(minutes = 15)
            end_time = datetime.utcnow()
            response = cw_collector.buildQuery( instance_id, start_time, end_time,metricVar)
            #print(response)
            cw_collector.cpuUtilization(cpu_util_df,response,instance_id)
            
            # Calculating the Status
            cw_collector.logicBuilder(cpu_util_df,instance_id)

       
        print(tabulate(cpu_util_df, headers='keys', tablefmt='psql'))   

    except Exception as e:
        print("Oops!", e, ". Check the __main__() Function")


        


# dd if=/dev/zero of=/dev/null

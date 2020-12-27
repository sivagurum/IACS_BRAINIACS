import botocore
from numpy.lib.shape_base import take_along_axis
import pandas as pd
import boto3
from tabulate import tabulate
from io import StringIO

# Collect Ec2 Metrics 
class CollectMetrics:
    def __init__(self, df):
        self.df = df 

    def writeData(self, df, bucket, objectname,  s3_resource):
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)        
            s3_resource.Object(bucket, objectname).put(Body=csv_buffer.getvalue())
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the writeData()")


    def getIamDetails(self, resource, instance_id):
        try:
            filter = {
                'Name': 'instance-id',
                'Values': [instance_id]
            }
            iam_details = []
            response = resource.describe_iam_instance_profile_associations(
                Filters = [filter]
            )
            for record in response['IamInstanceProfileAssociations']:
                iam_details.append(record['IamInstanceProfile']['Arn'])
            df.loc[df['INSTANCE_ID']  == instance_id, 'INSTANCE_IAM' ] = iam_details if iam_details else 'NA'
        except botocore.exceptions.ClientError as e:
            df.loc[df['INSTANCE_ID'] == instance.id, "INSTANCE_IAM"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the getTagDetails()")

    def getTagDetails(self, resource, instance_id):
        try:         
            tag_details = []
            filter = {
                'Name': 'resource-id', 
                        'Values': [instance_id]
            }
            response = resource.describe_tags(
                    Filters = [ filter ]
                )
          
            for i in response['Tags']:
                tag  = {}
                if i['Key'] in 'Name':
                    instance_name = i['Value']
                    print(instance_name)
                tag['key'] = i['Key']
                tag['value'] = i['Value']
                tag_details.append(tag)
            tag_details = ",".join([item['key']+":"+item['value'] for item in tag_details])
            # Adding the Tag Details to the DataFrame 
            df.loc[df['INSTANCE_ID']  == instance_id, 'INSTANCE_TAGS' ] = tag_details #pd.Series([tag_details])
            df.loc[df['INSTANCE_ID']  == instance_id, 'INSTANCE_NAME' ] = instance_name
        except botocore.exceptions.ClientError as e:
            df.loc[df['INSTANCE_ID'] == instance.id, "INSTANCE_NAME"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the getTagDetails()")

    def getSecurityGroupDetails(self,instance, instance_id):
        security_gp_name = []
        for i in instance.security_groups:
            security_gp_name.append(i['GroupName'])
        try:
            df.loc[df['INSTANCE_ID']  == instance_id, 'INSTANCE_SG'] = security_gp_name
        except botocore.exceptions.ClientError as e:
            print(e.response['Error']['Message'])
            df.loc[df['INSTANCE_ID'] == instance_id, "INSTANCE_SG"] = 'ERROR'
        except Exception as e1:
            print(e1)
            print("Oops!", e1.__class__, "occurred in the getSecurityGroup()")

    def getInstanceStatus(self, instance, instance_id ):
        try:
            df.loc[df['INSTANCE_ID']  ==  instance_id, 'INSTANCE_STATUS'] = instance.state['Name']
        except botocore.exceptions.ClientError as e:
            df.loc[df['INSTANCE_ID'] == instance_id, "INSTANCE_STATUS"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the getInstanceStatus()")

    def getInstanceDetails(self, instance, instance_id ):
        # Parse Information and add it to Dataframe
        try:            
            df.loc[df['INSTANCE_ID']  ==  instance_id, 'INSTANCE_TYPE'] = instance.instance_type
            df.loc[df['INSTANCE_ID']  == instance_id, 'INSTANCE_LAUNCH_TIME'] = instance.launch_time
            df.loc[df['INSTANCE_ID']  == instance_id, 'INSTANCE_PRV_IP'] = instance.private_ip_address
            df.loc[df['INSTANCE_ID']  == instance_id, 'INSTANCE_PUB_IP'] = instance.public_ip_address if instance.public_ip_address  else 'NONE'
            df.loc[df['INSTANCE_ID']  == instance_id, 'INSTANCE_SUBNET_ID'] = instance.subnet_id
            df.loc[df['INSTANCE_ID']  == instance_id, 'INSTANCE_VPC_ID'] = instance.vpc_id

        except botocore.exceptions.ClientError as e:
            df.loc[df['INSTANCE_ID'] == instance_id, "INSTANCE_NAME"] = e.response['Error']['Message']
        except Exception as e:
            print("Oops!", e.__class__, "occurred in the getInstanceDetails()")


if __name__ == "__main__":
    try:
        # Create a Client and resource Object for boto3
        ec2_resource = boto3.resource('ec2')
        ec2_client = boto3.client('ec2')
        s3_resource = boto3.resource('s3')

        # Defining Bucket Details 
        bucket='brain-iacs-east'
        objectname='ec2-metrics/ec2Details.csv'


        # Create a Dataframe 

        df = pd.DataFrame(
            columns= ['INSTANCE_ID',  'INSTANCE_NAME', 'INSTANCE_TYPE',  'INSTANCE_STATUS', 'INSTANCE_LAUNCH_TIME', 'INSTANCE_PRV_IP', 'INSTANCE_PUB_IP', 'INSTANCE_SUBNET_ID', 'INSTANCE_VPC_ID', 'INSTANCE_SG', 'INSTANCE_IAM','INSTANCE_TAGS' ]
        )

        
        collector = CollectMetrics(df)

        # Listing all the avaialble ec2 instance
        for instance in ec2_resource.instances.all():
            instance_id = instance.id
            
            df = df.append({'INSTANCE_ID': instance_id}, ignore_index=True)
            collector.getTagDetails(ec2_client, instance_id)
            collector.getInstanceDetails(instance, instance_id)
            collector.getInstanceStatus(instance, instance_id)
            collector.getSecurityGroupDetails(instance, instance_id)
            collector.getIamDetails(ec2_client, instance_id)
            

        print(tabulate(df, headers='keys', tablefmt='psql'))
        # Writing Data to S3 Bukcet 
        collector.writeData(df,bucket,objectname, s3_resource)
    except Exception as e:
        print("Oops!", e, ". Check the __main__() Function")

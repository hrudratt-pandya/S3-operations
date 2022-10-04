'''
User_name: hrudratt
Password
Access key ID: AKIAVPGG4ULMA6FI5L6W
Secret access key: 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
Console login link: https://keyur-patel.signin.aws.amazon.com/console
reagion - ap-south-1
s3 path - s3://ocius-s3-bucket/temp/developer/
'''

'''
Points from trainner:
    
'''

import boto3
import pandas as pd

# class name should be in todo: UpperCamelCase
# Funcation name and parameters should be in todo: snake_case

class S3Operations:

    def __init__(self, region_name, aws_access_key_id, aws_secret_access_key):
        self.service_name = 's3'
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3 = boto3.resource(
            service_name=self.service_name,
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

    def create_bucket(self, bucket_name, block_public_acls: bool, ignore_public_acls: bool, block_public_policy: bool,
                      restrict_public_buckets: bool):
        try:
            # link for doc: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-s3-bucket-publicaccessblockconfiguration.html#cfn-s3-bucket-publicaccessblockconfiguration-blockpublicacls
            self.s3.create_bucket(Bucket=bucket_name,
                                  CreateBucketConfiguration={'LocationConstraint': self.region_name})
            response_public = self.s3.put_public_access_block(
                Bucket=bucket_name,
                PublicAccessBlockConfiguration={
                    'BlockPublicAcls': block_public_acls,
                    'IgnorePublicAcls': ignore_public_acls,
                    'BlockPublicPolicy': block_public_policy,
                    'RestrictPublicBuckets': restrict_public_buckets
                },
            )
            return response_public
        except Exception as e:
            return e

    def get_bucket_names(self):
        try:
            # todo: Get all bucket names
            bucket_name_list = [bucket.name for bucket in self.s3.buckets.all()]
            return bucket_name_list
        except Exception as e:
            return e

    def upload_file(self, bucket_name, local_file_full_path, s3_file_name):
        try:
            # todo: Upload files to S3 bucket
            self.s3.Bucket(bucket_name).upload_file(Filename=local_file_full_path, Key=s3_file_name)
            '''
            self.s3.Bucket('bucket_name').upload_file(Filename='LOCAL FILE NAME', Key='FILENAME YOU"LL SEE IN S3')
            If you get an error like 301 Moved Permanently, 
            it most likely means that something’s gone wrong with regards to your region. 
            It could be that
            
            1. You’ve misspelled or inserted the wrong region name for the environment variable AWS_DEFAULT_REGION (if you’re using environment vars)
            2. You’ve misspelled or inserted the wrong region name for the region_name parameter of boto3.resource() (if you aren’t using environment vars)
            3. You’ve incorrectly set up your user’s permissions
            '''
        except Exception as e:
            return e

    def get_file_name_from_s3(self, bucket_name):
        try:
            # todo: list all the objects in our bucket.

            # for obj in self.s3.Bucket(bucket_name).objects.all():
            #     print(obj)
            bucket_file_list = [obj for obj in self.s3.Bucket(bucket_name).objects.all()]
            return bucket_file_list
            # output like: # s3.ObjectSummary(bucket_name='cheez-willikers', key='FILENAME OF S3')
        except Exception as e:
            return e

    def get_data_from_s3_file(self, bucket_name, s3_file_name):
        try:
            # todo: Load csv file directly from S3 into python
            obj = self.s3.Bucket(bucket_name).Object(s3_file_name).get()
            return pd.read_csv(obj['Body'], index_col=0)
        except Exception as e:
            return e

    def download_file_from_s3(self, bucket_name, s3_file_name, local_file_full_path):
        try:
            # todo: Download file and read from disc
            self.s3.Bucket(bucket_name).download_file(Key=s3_file_name, Filename=local_file_full_path)
            pd.read_csv(local_file_full_path, index_col=0)
            return "File Saved"
        except Exception as e:
            return e


s3_class_object = S3Operations(
                                region_name='ap-south-1',
                                aws_access_key_id='AKIAVPGG4ULMA6FI5L6W',
                                aws_secret_access_key='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')

create_bucket_obj = s3_class_object.create_bucket('new_bucket_name', True, True, True, True)
get_bucket_names_obj = s3_class_object.get_bucket_names()
upload_file_obj = s3_class_object.upload_file('bucket_name', 'local_file.csv', 's3_file_name.csv')
get_file_name_from_s3_obj = s3_class_object.get_file_name_from_s3('bucket_name')
get_data_from_s3_file_obj = s3_class_object.get_data_from_s3_file('bucket_name', 's3_file_name.csv')
download_file_from_s3_obj = s3_class_object.download_file_from_s3('bucket_name', 's3_file_name.csv',
                                                                  'local_file_path.csv')

# testing & backup purpose
# s3 = boto3.resource(
#     service_name='s3',
#     region_name='ap-south-1',
#     aws_access_key_id='AKIAVPGG4ULMA6FI5L6W',
#     aws_secret_access_key='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'
#         )
# for i in s3.Bucket('sangramtest2').objects.all():
#     print(i)
# ==============================================================================================
# # todo: create bucket
# s3_client = boto3.client('s3', region_name='ap-south-1')
# bucket_name = 'ohfihfhfeuhehfuhfeih'
#
# s3_bucket = s3_client.create_bucket(Bucket=bucket_name,
#                                     CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'})

# response_public = s3_client.put_public_access_block(
#     Bucket=bucket_name,
#     PublicAccessBlockConfiguration={
#         'BlockPublicAcls': True,
#         'IgnorePublicAcls': True,
#         'BlockPublicPolicy': True,
#         'RestrictPublicBuckets': True
#     },
# )
# ==============================================================================================
# #todo: create csv file
# foo = pd.DataFrame({'x': [1, 2, 3], 'y': ['a', 'b', 'c']})
# foo.to_csv('foo.csv')

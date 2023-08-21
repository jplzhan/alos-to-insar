"""Defines a class for uploading artifacts to AWS S3 buckets."""
import logging
import boto3
import shutil
from botocore.exceptions import ClientError
import os

class AWS:
    def __init__(self, key: str, secret: str, token: str, region: str, configdir=None):
        if configdir is None:
            self.session = boto3.Session(
                aws_access_key_id=key,
                aws_secret_access_key=secret,
                aws_session_token=token,
                region_name=region
            )
            self.client = self.session.client('s3')
        else:
            if isinstance(configdir, str) and os.path.isdir(configdir):
                shutil.copytree(configdir, os.path.join(os.path.expanduser('~'), '.aws'))
            self.session = None
            self.client = boto3.client('s3')

    def upload_file(self, file_name: str, bucket: str, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        try:
            response = self.client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def upload_dir(self, dirname: str, bucket: str, path: str):
        """Recursively uploads a directory to an S3 bucket."""
        if not os.path.isdir(dirname):
            print('{} is not a directory!'.format(dirname))

        if not dirname.endswith('/'):
            dirname += '/'
        prefix_len = len(dirname)
        for parent, dirs, filenames in os.walk(dirname):
            for fname in filenames:
                obj_name = os.path.join(path, fname)
                if len(parent) > prefix_len:
                    obj_name = os.path.join(parent[prefix_len:], obj_name)
                obj_name = obj_name.lstrip('/')
                self.upload_file(os.path.join(parent, fname), bucket, obj_name)

from tempfile import NamedTemporaryFile
import boto3
import datetime
from typing import List
import json
from .writer_interface import WriterInterface
import os
from dotenv import load_dotenv

load_dotenv()


class MinioWriter(WriterInterface):
    def __init__(self, bucket_name: str):
        self.bucket = bucket_name
        self.client = boto3.client(
            "s3",
            endpoint_url=os.environ["MINIO_URL"],
            aws_access_key_id="minio-root-user",
            aws_secret_access_key="minio-root-password",
            aws_session_token=None,
            config=boto3.session.Config(signature_version="s3v4"),
            verify=False,
        )

    def write(self, file_path: str, json_data: [List, dict]):
        file_name = f"{file_path}.json"
        bytes_json = self._json_data_to_bytes(json_data)
        self.client.put_object(Body=bytes_json, Bucket=self.bucket, Key=file_name)

    def _json_data_to_bytes(self, json_data: [List, dict]) -> bytes:
        return bytes(json.dumps(json_data).encode("UTF-8"))

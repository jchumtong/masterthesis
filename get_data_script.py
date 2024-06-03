import os
import pandas as pd
import boto3
import json
import tarfile
import tempfile
from s3helper import S3Helper
from parser_generator import ParserGenerator
import random

s3_client = boto3.client('s3import os
import boto3
import traceback
import botocore
import nltk

s3_client = boto3.client("s3", region_name="eu-west-1")

logging.basicConfig(level=logging.INFO)
nltk.download("punkt", quiet=True)


def tokenize_receipt(receipt_text):
    tokens = nltk.word_tokenize(receipt_text)
    return tokens


def get_all_receipts_for_venue(bucket_name, venue_id):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    receipts = []

    try:
        for obj in bucket.objects.filter(Prefix=f"{venue_id}/"):
            if "error" not in obj.key:
                receipts.append(obj.key)

    except botocore.exceptions.ClientError as e:
        traceback.print_exc()
        print("Error Message:", e)

    return receipts


def open_saved_receipt(receipt_path):
    with open(receipt_path) as f:
        receipt = f.read()
    return receipt


def download_parse_save_receipts(venue_ids, receipts_bucket):
    results = []

    for venue_id in venue_ids:
        print("starting another venue:\n")
        receipts = get_all_receipts_for_venue(receipts_bucket, venue_id)
        parser_generator = ParserGenerator(venue_id=venue_id)

        receipts = receipts[:20]

        with parser_generator:
            for receipt_key in receipts:
                with tempfile.TemporaryDirectory() as temp_dir:
                    tar_file_path = os.path.join(
                        temp_dir, os.path.basename(receipt_key)
                    )
                    s3_client.download_file(receipts_bucket, receipt_key, tar_file_path)

                    with tarfile.open(tar_file_path, "r:gz") as tar:
                        tar.extractall(path=temp_dir)

                    for item in os.listdir(temp_dir):
                        item_path = os.path.join(temp_dir, item)
                        if os.path.isfile(item_path) and item.endswith(".tar.gz"):
                            with tempfile.TemporaryDirectory() as inner_temp_dir:
                                with tarfile.open(item_path, "r:gz") as inner_tar:
                                    inner_tar.extractall(path=inner_temp_dir)

                                data_preprocessed_path = os.path.join(
                                    inner_temp_dir, "data", "receipts_processed"
                                )
                                if os.path.isdir(data_preprocessed_path):
                                    for receipt_file in os.listdir(
                                        data_preprocessed_path
                                    ):
                                        receipt_file_path = os.path.join(
                                            data_preprocessed_path, receipt_file
                                        )
                                        if os.path.isfile(
                                            receipt_file_path
                                        ) and receipt_file.endswith(".txt"):
                                            receipt = open_saved_receipt(
                                                receipt_file_path
                                            )
                                            tokens = tokenize_receipt(receipt)

                                            try:
                                                parsed_receipt = (
                                                    parser_generator.parse_receipt(
                                                        receipt_file_path
                                                    )
                                                )
                                                results.append(
                                                    {
                                                        "venue_id": venue_id,
                                                        "receipt_key": receipt_key,
                                                        "receipt_file": receipt,
                                                        "tokenized_receipt": tokens,
                                                        "parsed_receipt": parsed_receipt,
                                                    }
                                                )
                                                print("saved")

                                            except Exception as e:
                                                print(
                                                    f"Error parsing receipt {receipt_file}: {e}"
                                                )
                                        else:
                                            print(
                                                f"Found non-txt file in data_preprocessed: {receipt_file}"
                                            )
                                else:
                                    print(
                                        f"'data/data_preprocessed' directory does not exist in {receipt_key}"
                                    )
            parser_generator.delete_parser_generator()

    df = pd.DataFrame(results)
    return df


receipts_bucket = "receiptsproduction"

selected_venue_ids = ["5531", "5546"]

df = download_parse_save_receipts(selected_venue_ids, receipts_bucket)

print(df.head())

output_csv_path = "parsed_receipts.csv"
df.to_csv(output_csv_path, index=False)

print(f"DataFrame saved as CSV: {output_csv_path}")


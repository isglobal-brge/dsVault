import json
import os
import tempfile
import unittest
from unittest.mock import patch

from click.testing import CliRunner
import yaml
try:
    import boto3
    from moto import mock_aws
    HAS_MOTO = True
except Exception:
    boto3 = None
    mock_aws = None
    HAS_MOTO = False

from dsimaging_admin.cli import main
from dsimaging_admin.s3 import provision_aws_store


@unittest.skipUnless(HAS_MOTO, "moto is not installed")
class AwsProvisioningTests(unittest.TestCase):
    def test_provision_bucket_versioning_queue_and_notification_idempotently(self):
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            sqs = boto3.client("sqs", region_name="us-east-1")

            first = provision_aws_store(
                None, "imaging-data", "us-east-1", s3_client=s3, sqs_client=sqs
            )
            second = provision_aws_store(
                None, "imaging-data", "us-east-1", s3_client=s3, sqs_client=sqs
            )

            self.assertTrue(first["ok"])
            self.assertTrue(second["ok"])
            self.assertEqual(
                s3.get_bucket_versioning(Bucket="imaging-data").get("Status"),
                "Enabled",
            )
            rule = s3.get_bucket_encryption(Bucket="imaging-data")[
                "ServerSideEncryptionConfiguration"
            ]["Rules"][0]["ApplyServerSideEncryptionByDefault"]
            self.assertEqual(rule["SSEAlgorithm"], "AES256")
            queue_url = sqs.get_queue_url(
                QueueName="dsimaging-store-imaging-data-events"
            )["QueueUrl"]
            attrs = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["QueueArn", "Policy"],
            )["Attributes"]
            self.assertIn("arn:aws:s3:::imaging-data", attrs["Policy"])
            notification = s3.get_bucket_notification_configuration(
                Bucket="imaging-data"
            )
            queues = notification["QueueConfigurations"]
            self.assertEqual(len(queues), 1)
            self.assertEqual(queues[0]["QueueArn"], attrs["QueueArn"])
            self.assertIn("s3:ObjectCreated:*", queues[0]["Events"])
            self.assertIn("s3:ObjectRemoved:*", queues[0]["Events"])
            second_steps = {step["name"]: step["status"] for step in second["steps"]}
            self.assertEqual(second_steps["bucket"], "skipped")
            self.assertEqual(second_steps["versioning"], "skipped")
            self.assertEqual(second_steps["sqs_queue"], "skipped")
            self.assertEqual(second_steps["bucket_notification"], "skipped")

    def test_provision_non_us_east_region_and_kms(self):
        with mock_aws():
            s3 = boto3.client("s3", region_name="eu-west-1")
            sqs = boto3.client("sqs", region_name="eu-west-1")
            kms_key = (
                "arn:aws:kms:eu-west-1:111122223333:key/"
                "00000000-0000-0000-0000-000000000000"
            )

            report = provision_aws_store(
                None,
                "imaging-data-eu",
                "eu-west-1",
                kms_key=kms_key,
                s3_client=s3,
                sqs_client=sqs,
            )

            self.assertTrue(report["ok"])
            location = s3.get_bucket_location(Bucket="imaging-data-eu")[
                "LocationConstraint"
            ]
            self.assertEqual(location, "eu-west-1")
            rule = s3.get_bucket_encryption(Bucket="imaging-data-eu")[
                "ServerSideEncryptionConfiguration"
            ]["Rules"][0]["ApplyServerSideEncryptionByDefault"]
            self.assertEqual(rule["SSEAlgorithm"], "aws:kms")
            self.assertEqual(rule["KMSMasterKeyID"], kms_key)

    def test_queue_policy_merges_existing_statement(self):
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            sqs = boto3.client("sqs", region_name="us-east-1")
            queue_url = sqs.create_queue(
                QueueName="dsimaging-store-imaging-data-events"
            )["QueueUrl"]
            existing_policy = {
                "Version": "2012-10-17",
                "Statement": [{
                    "Sid": "ExistingAllow",
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "SQS:GetQueueAttributes",
                    "Resource": "*",
                }],
            }
            sqs.set_queue_attributes(
                QueueUrl=queue_url,
                Attributes={"Policy": json.dumps(existing_policy)},
            )

            provision_aws_store(
                None, "imaging-data", "us-east-1", s3_client=s3, sqs_client=sqs
            )

            attrs = sqs.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=["Policy"]
            )["Attributes"]
            policy = json.loads(attrs["Policy"])
            sids = {statement["Sid"] for statement in policy["Statement"]}
            self.assertIn("ExistingAllow", sids)
            self.assertTrue(any(
                statement.get("Condition", {})
                .get("ArnEquals", {})
                .get("aws:SourceArn") == "arn:aws:s3:::imaging-data"
                for statement in policy["Statement"]
            ))

    def test_notification_configuration_merges_unrelated_queue(self):
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            sqs = boto3.client("sqs", region_name="us-east-1")
            s3.create_bucket(Bucket="imaging-data")
            other_url = sqs.create_queue(QueueName="unrelated-events")["QueueUrl"]
            other_arn = sqs.get_queue_attributes(
                QueueUrl=other_url, AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"]
            sqs.set_queue_attributes(
                QueueUrl=other_url,
                Attributes={"Policy": json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Sid": "AllowS3",
                        "Effect": "Allow",
                        "Principal": {"Service": "s3.amazonaws.com"},
                        "Action": "SQS:SendMessage",
                        "Resource": other_arn,
                        "Condition": {
                            "ArnEquals": {"aws:SourceArn": "arn:aws:s3:::imaging-data"}
                        },
                    }],
                })},
            )
            s3.put_bucket_notification_configuration(
                Bucket="imaging-data",
                NotificationConfiguration={
                    "QueueConfigurations": [{
                        "Id": "unrelated",
                        "QueueArn": other_arn,
                        "Events": ["s3:ObjectCreated:*"],
                    }],
                },
            )

            report = provision_aws_store(
                None, "imaging-data", "us-east-1", s3_client=s3, sqs_client=sqs
            )

            self.assertTrue(report["ok"])
            config = s3.get_bucket_notification_configuration(Bucket="imaging-data")
            queue_arns = {
                item["QueueArn"] for item in config.get("QueueConfigurations", [])
            }
            self.assertIn(other_arn, queue_arns)
            self.assertIn(report["sqs_queue_arn"], queue_arns)

    def test_cli_store_init_aws_persists_queue_url_idempotently(self):
        with mock_aws(), tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "dsimaging.yaml")
            runner = CliRunner()
            args = [
                "--backend", "aws",
                "--bucket", "imaging-data-cli",
                "store", "init",
            ]
            env = {
                "AWS_ACCESS_KEY_ID": "testing",
                "AWS_SECRET_ACCESS_KEY": "testing",
                "AWS_DEFAULT_REGION": "us-east-1",
            }
            with patch("dsimaging_admin.cli.CONFIG_PATH", config_path):
                first = runner.invoke(main, args, env=env)
                second = runner.invoke(main, args, env=env)

            self.assertEqual(first.exit_code, 0, first.output)
            self.assertEqual(second.exit_code, 0, second.output)
            self.assertIn("AWS store provisioning", first.output)
            self.assertIn("SKIPPED bucket", second.output)
            with open(config_path, encoding="utf-8") as f:
                config = yaml.safe_load(f)
            queue_url = config["profiles"]["default"]["aws"]["sqs_queue_url"]
            self.assertIn("dsimaging-store-imaging-data-cli-events", queue_url)


if __name__ == "__main__":
    unittest.main()

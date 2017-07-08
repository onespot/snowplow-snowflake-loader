#!/usr/bin/env python

import argparse
from datetime import datetime, timedelta
import sys
import time

import boto3
from snowplow_analytics_sdk.run_manifests import *

# -----------------------------------------------------------------------------
#  CONSTANTS
# -----------------------------------------------------------------------------

TIME_FORMAT = '%Y-%m-%d-%H-%M-%S'
SCRIPT_VERSION = 'backfill-script-0.1.0'


# -----------------------------------------------------------------------------
#  COMMANDS
# -----------------------------------------------------------------------------

def backfill(dynamodb, s3, args):
    """Resets manifests table and backfills it with archived/processed items"""
    run_manifests = RunManifests(dynamodb, args.manifest_table_name)
    added = 0
    skipped = 0

    for run_id in list_runids(s3, args.enriched_archive):
        if should_skip(run_id, args.startdate):
            if run_manifests.contains(run_id):
                print("Run manifest already contains {}. Do nothing".format(run_id))
            else:
                added = added + 1
                add(dynamodb, args.manifest_table_name, run_id)
        else:
            skipped = skipped + 1
            pass

    print("{} run ids added to manifest as skipped".format(added))
    print("{} run ids will be processed".format(skipped))


def should_skip(run_id, startdate):
    """Predicate allowing to skip run ids older than two weeks
    and already processed
    """
    RUN_ID_TIME_LENGTH = 'YYYY-mm-dd-HH-MM-SS/'   # 20
    startdate = datetime.strptime(startdate, TIME_FORMAT)
    date = run_id[-len(RUN_ID_TIME_LENGTH):]
    d = datetime.strptime(date, '%Y-%m-%d-%H-%M-%S/')
    if d < startdate:
        return True
    else:
        return False


def add(dynamodb, table_name, run_id):
    """Adds run id to manifest in strawberry-compatible way"""
    dynamodb.put_item(
        TableName=table_name,
        Item={
            DYNAMODB_RUNID_ATTRIBUTE: {
                'S': run_id
            },
            'AddedBy': {
                'S': SCRIPT_VERSION
            },
            'AddedAt': {
                'N': str(int(time.time()))
            },
            'ToSkip': {
                'BOOL': True
            }
        }
    )


if __name__ == "__main__":
    # Initialize

    parser = argparse.ArgumentParser(description='Backfill strawberry run manifest')

    parser.add_argument('--aws-access-key-id', type=str, required=True,
                        help="AWS access key id DynamoDB and S3")
    parser.add_argument('--aws-secret-access-key', type=str, required=True,
                        help="AWS secret access key id DynamoDB and S3")
    parser.add_argument('--region', type=str, required=True,
                        help="AWS Region to access DynamoDB and S3")
    parser.add_argument('--manifest-table-name', required=True,
                        help="DynamoDB Process manifest table name")
    parser.add_argument('--startdate', type=str, required=True,
                        help="Date since when run ids should be loaded")
    parser.add_argument('--enriched-archive', type=str, required=True,
                        help="Path to enriched archive S3 bucket")

    args = parser.parse_args()

    session = boto3.Session(aws_access_key_id=args.aws_access_key_id, aws_secret_access_key=args.aws_secret_access_key)
    s3 = session.client('s3')
    dynamodb = session.client('dynamodb', region_name=args.region)


    try:
        datetime.strptime(args.startdate, TIME_FORMAT)
    except ValueError:
        print("--startdate must confirm {} format".format(TIME_FORMAT))
        sys.exit(1)

    backfill(dynamodb, s3, args)

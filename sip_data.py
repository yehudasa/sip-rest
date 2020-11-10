import boto3

from boto3.dynamodb.conditions import Key

from datetime import datetime as dt


from env import *


class SIPDataFull:
    def __init__(self, env):
        self.env = env
        self.num_shards = 1
        self.stage_id='data.full'

    def info(self):
        result = {
                'data_type' : 'data',
                'first_stage' : 'data.full',
                'last_stage' : 'data.full',
                'name' : 'data.full',
                'stages' : [
                    {
                        'disabled' : 'false',
                        'num_shards' : 1,
                        'sid' : 'data.full',
                        'type' : 'full'
                        }
                    ]
                }

        return (200, result)

    def status(self, stage_id, shard_id):
        if (stage_id and (stage_id != self.stage_id)) or (shard_id >= self.num_shards):
            return (416, {})   # invalid range

        result = {
                "markers" : {
                    "current" : "",
                    "start" : ""
                    }
                }

        return (200, result)


    def fetch(self, stage_id, shard_id, marker, max_entries):
        if (stage_id and (stage_id != self.stage_id)) or (shard_id >= self.num_shards):
            return (416, {})   # invalid range

        s3 = boto3.resource('s3')

        entries = []

        aws_bucket = env_params.aws_bucket

        prefix = '' # self.env.instance + '/'
        bucket = s3.Bucket(aws_bucket)
        paginator = bucket.meta.client.get_paginator('list_objects')
        for resp in paginator.paginate(Bucket=aws_bucket, Delimiter='/', Marker=marker, Prefix=prefix):
            for cp in resp.get('CommonPrefixes', []):
                prefix = cp['Prefix']
                b = prefix.rstrip('/')
                entry_info = {
                        'key': b,
                        'num_shards': env_params.bilog_num_shards,
                        'shard_id': -1,
                        'timestamp': dt.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                        }

                entry = {
                        'info': entry_info,
                        'key': prefix
                        }

                entries.append(entry)

                if len(entries) == max_entries:
                    break
        
        more = len(entries) == max_entries
        done = not more
        
        result = {
            'more': more,
            'done': done,
            'entries': entries
        }

        return (200, result)


class SIPDataInc:
    def __init__(self, env):
        self.env = env
        self.num_shards = env_params.datalog_num_shards
        self.stage_id='data.inc'

    def info(self):
        result = {
                "data_type" : "data",
                "first_stage" : "data.inc",
                "last_stage" : "data.inc",
                "name" : "data.inc",
                "stages" : [
                    {
                        "disabled" : "false",
                        "num_shards" : self.num_shards,
                        "sid" : "data.inc",
                        "type" : "inc"
                        }
                    ]
                }

        return (200, result)

    def get_table(self):
        dynamodb = boto3.resource('dynamodb')
        return dynamodb.Table(env_params.datalog_table_name)

    def status(self, stage_id, shard_id):
        if (stage_id and (stage_id != self.stage_id)) or (shard_id >= self.num_shards):
            return (416, {})   # invalid range

        datalog_table = self.get_table()
        response = datalog_table.query( KeyConditionExpression=Key('shard_id').eq(shard_id), ScanIndexForward=False, Limit=1)

        current = ''

        items = response['Items']
        if len(items) >  0:
            entry = items[-1]
            current = entry['entry_id']

        result = {
                "markers" : {
                    "current" : current,
                    "start" : ""
                    }
                }

        return (200, result)


    def fetch(self, stage_id, shard_id, marker, max_entries):
        if (stage_id and (stage_id != self.stage_id)) or (shard_id >= self.num_shards):
            return (416, {})   # invalid range

        s3 = boto3.resource('s3')

        entries = []

        datalog_table = self.get_table()

        cond = Key('shard_id').eq(shard_id)
        if marker:
            cond &= Key('entry_id').gt(marker)

        response = datalog_table.query(KeyConditionExpression=cond,
                                       ScanIndexForward=True, Limit=max_entries)

        for item in response['Items']:
            entry_info = {
                    'key': item['bucket'],
                    'num_shards': int(item['num_shards']),
                    'shard_id': int(item['bucket_shard_id']),
                    'timestamp': item['timestamp']
                    }

            entry = {
                    'info': entry_info,
                    'key': item['entry_id']
                    }

            entries.append(entry)

        more = len(entries) == max_entries
        done = False
        
        result = {
            'more': more,
            'done': done,
            'entries': entries
        }



        return (200, result)



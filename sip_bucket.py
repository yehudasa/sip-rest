import boto3

from boto3.dynamodb.conditions import Key


from env import *
from tools import ceph_str_hash_linux


class SIPBucketFull:
    def __init__(self, env, instance):
        self.env = env
        self.instance = instance
        self.num_shards = env_params.bilog_num_shards
        self.stage_id='bucket.full'

    def info(self):
        result = {
                'data_type' : 'bucket',
                'first_stage' : 'bucket.full',
                'last_stage' : 'bucket.full',
                'name' : 'bucket.full',
                'stages' : [
                    {
                        'disabled' : 'false',
                        'num_shards' : self.num_shards,
                        'sid' : 'bucket.full',
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

    def obj_in_shard(self, obj, shard_id):
      return (ceph_str_hash_linux(obj) % self.num_shards) == shard_id


    def fetch(self, stage_id, shard_id, marker, max_entries):
        if (stage_id and (stage_id != self.stage_id)) or (shard_id >= self.num_shards):
            return (416, {})   # invalid range

        s3 = boto3.resource('s3')

        entries = []

        aws_bucket = env_params.aws_bucket

        prefix = self.instance + '/'
        bucket = s3.Bucket(aws_bucket)
        paginator = bucket.meta.client.get_paginator('list_objects')
        for resp in paginator.paginate(Bucket=aws_bucket, Marker=marker, Prefix=prefix):
            for entry in resp.get('Contents', []):
                k = entry['Key']
                obj = k.lstrip(prefix)

                if not self.obj_in_shard(obj, shard_id):
                    continue

                owner = entry['Owner']
                entry_info = {
                        'bilog_flags': 0,
                        'object': obj,
                        'instance': '', # FIXME
                        'op': 'write',
                        'owner': owner['ID'],
                        'state': 'complete',
                        'timestamp': entry['LastModified'].strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                        'ver' : {
                            'epoch': 0,
                            'pool': -1,
                            },
                        'versioned': False,
                        'zones_trace': [],
                        }

                display_name = owner.get('DisplayName')
                if display_name:
                    entry_info['owner_display_name'] = display_name

                entry = {
                        'info': entry_info,
                        'key': entry['Key']
                        }

                entries.append(entry)

                if len(entries) == max_entries:
                    break

        return (200, entries)


class SIPBucketInc:
    def __init__(self, env, instance):
        self.env = env
        self.instance = instance
        self.num_shards = env_params.bilog_num_shards
        self.stage_id='bucket.inc'

    def info(self):
        result = {
                "data_type" : "bucket",
                "first_stage" : "bucket.inc",
                "last_stage" : "bucket.inc",
                "name" : "bucket.inc",
                "stages" : [
                    {
                        "disabled" : "false",
                        "num_shards" : self.num_shards,
                        "sid" : "bucket.inc",
                        "type" : "inc"
                        }
                    ]
                }

        return (200, result)

    def get_table(self):
        dynamodb = boto3.resource('dynamodb')
        return dynamodb.Table(env_params.bilog_table_name)

    def status(self, stage_id, shard_id):
        if (stage_id and (stage_id != self.stage_id)) or (shard_id >= self.num_shards):
            return (416, {})   # invalid range

        bucket_shard_id = self.instance + '.' + str(shard_id)

        datalog_table = get_table()
        response = datalog_table.query(KeyConditionExpression=Key('bucket_shard_id').eq(bucket_shard_id), ScanIndexForward=False, Limit=1)

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

    def convert_op(self, op):
        if op == 'ObjectRemoved:Delete':
            return 'del'

        return 'write'

    def fetch(self, stage_id, shard_id, marker, max_entries):
        if (stage_id and (stage_id != self.stage_id)) or (shard_id >= self.num_shards):
            return (416, {})   # invalid range

        s3 = boto3.resource('s3')

        entries = []

        bilog_table = self.get_table()

        bucket_shard_id = self.instance + '.' + str(shard_id)

        cond = Key('bucket_shard_id').eq(bucket_shard_id)
        if marker:
            cond &= Key('entry_id').gt(marker)

        response = bilog_table.query(KeyConditionExpression=cond,
                                     ScanIndexForward=True, Limit=max_entries)

        for item in response['Items']:
            entry_info = {
                    'bilog_flags': 0,
                    'object': item['obj'],
                    'instance': '', # FIXME
                    'state': 'complete',
                    'op': self.convert_op(item['op']),
                    # 'owner': owner['ID'],
                    'timestamp': item['timestamp'],
                    'ver' : {
                        'epoch': 0,
                        'pool': -1,
                        },
                    'versioned': False,
                    'zones_trace': [],
                    }

            entry = {
                    'info': entry_info,
                    'key': item['entry_id']
                    }

            entries.append(entry)

        return (200, entries)



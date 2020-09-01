import boto3

from boto3.dynamodb.conditions import Key


from env import *


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
                owner = entry['Owner']
                entry_info = {
                        'bilog_flags': 0,
                        'object': k.lstrip(prefix),
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



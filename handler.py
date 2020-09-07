import json
import boto3

from sip_data import SIPDataFull, SIPDataInc
from sip_bucket import SIPBucketFull, SIPBucketInc

from env import *


class SIPEnv:
    def __init__(self, event):
        self.event = event
        self.params = event['queryStringParameters'] or {}

    def list_providers(self):
        return [ 'data.full', 'data.inc', 'bucket.full', 'bucket.inc' ]

    def find_provider(self, provider, instance):
        if provider == 'data.full':
            return SIPDataFull(self)

        if provider == 'data.inc':
            return SIPDataInc(self)

        if provider == 'bucket.full':
            return SIPBucketFull(self, instance)

        if provider == 'bucket.inc':
            return SIPBucketInc(self, instance)

        return None



class SIPGetInfo:
    def __init__(self, env, provider):
        self.env = env
        self.provider = provider

    def exec(self):
        params = self.env.params
        opt_instance = params.get('instance')

        pvd = self.env.find_provider(self.provider, opt_instance)
        if not pvd:
            return (404, {})

        return pvd.info()

class SIPGetStatus:
    def __init__(self, env, provider):
        self.env = env
        self.provider = provider

    def exec(self):
        params = self.env.params
        opt_instance = params.get('instance')
        opt_stage_id = params.get('stage-id')
        shard_id = int(params.get('shard-id', '0'))

        pvd = self.env.find_provider(self.provider, opt_instance)
        if not pvd:
            return (404, {})

        return pvd.status(opt_stage_id, shard_id)

class SIPFetch:
    def __init__(self, env, provider):
        self.env = env
        self.provider = provider

    def exec(self):
        params = self.env.params
        opt_instance = params.get('instance')
        opt_stage_id = params.get('stage-id')
        marker = params.get('marker', '')
        max_entries = int(params.get('max', '1000'))
        shard_id = int(params.get('shard-id', '0'))

        pvd = self.env.find_provider(self.provider, opt_instance)
        if not pvd:
            return (404, {})

        return pvd.fetch(opt_stage_id, shard_id, marker, max_entries)



class HttpGet:
    def __init__(self, env):
        self.env = env

    def exec(self):
        params = self.env.params

        opt_provider = params.get('provider')
        opt_info = params.get('info')
        opt_status = params.get('status')
    
        if not opt_provider:
            return (200, self.env.list_providers())

        if opt_info is not None:
            op = SIPGetInfo(self.env, opt_provider)
        elif opt_status is not None:
            op = SIPGetStatus(self.env, opt_provider)
        else:
            op = SIPFetch(self.env, opt_provider)

        return op.exec()
        
        return (405, {})
    

def lambda_handler(event, context):

    if not env_params.valid():
        return {
        'statusCode': 500,
        'body': json.dumps({'status': 'misconfigured'})
        }

    method = event['httpMethod']

    env = SIPEnv(event)
    
    result = {}
    
    if method == 'GET':
        handler = HttpGet(env)
    else:
        handler = None
    
    if handler:
        status, body = handler.exec()
    else:
        status = 405
        body = {}
        
    
    
    return {
        'statusCode': status,
        'body': json.dumps(body)
    }


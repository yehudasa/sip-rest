import logging
import os
import sys

logger = logging.getLogger()
# logger.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


dynamodb_url='http://localhost:8100'



class Params:
   def __init__(self):
        self.datalog_num_shards = int(os.environ.get('DATALOG_NUM_SHARDS') or '16')
        self.bilog_num_shards = int(os.environ.get('BILOG_NUM_SHARDS') or '16')
        self.aws_bucket = os.environ.get('AWS_BUCKET')
        self.datalog_table_name = 'datalog'
        self.bilog_table_name = 'bilog'

   def valid(self):
       if not self.aws_bucket:
           return False

       return True
        

env_params = Params()

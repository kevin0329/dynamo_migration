from __future__ import print_function

import json
import boto3
from botocore.exceptions import ClientError

TARGET_TABLE="encrypt table"
session = boto3.session.Session()
db = session.resource('dynamodb', endpoint_url="https://dynamodb.ap-southeast-1.amazonaws.com", region_name='ap-southeast-1')

def lambda_handler(event, context):
    for record in event['Records']:
        en = record['eventName']
        table = db.Table(TARGET_TABLE)
        
        if en in ["MODIFY","INSERT"]:
            try:
                item = _parse_dynamo_item(record['dynamodb']['NewImage'])
                table.put_item(Item=item)
            except Exception as e:
                print("insert or update error "+str(e))
        elif en == "REMOVE":
            try:
                key = _parse_dynamo_item(record['dynamodb']['Keys'])
                table.delete_item(Key=key)
            except Exception as e:
                print("delete error"+str(e))

        
    return 'Successfully processed {} records.'.format(len(event['Records']))
        

def _parse_dynamo_item(item):
    resp = {}
    if type(item) in [str,int,bool,unicode]:
        return item

    for key,struct in item.iteritems():
        if not isinstance(struct, dict):
            if key == 'I':
                return int(struct)
            elif key == 'B':
                return bool(str)
            elif key == 'N':
                return int(struct)
            elif key == "NULL":
                return None
            else:
                return struct
        else:
            for k,v in struct.iteritems():
                if k == 'L':
                    value = []
                    for i in v:
                        value.append(_parse_dynamo_item(i))
                elif k == 'S':
                    value = str(v)
                elif k == 'I':
                    value = int(v)
                elif k == 'B':
                    value = bool(v)
                elif k =='BOOL':
                    value = bool(v)
                elif k == 'N':
                    value = int(v)
                elif k == 'NULL':
                    value = None
                elif k == 'M':
                    value = {}
                    for a,b in v.iteritems():
                        value[a] = _parse_dynamo_item(b)
                else:
                    key = k
                    value = _parse_dynamo_item(v)

                resp[key] = value

    return resp

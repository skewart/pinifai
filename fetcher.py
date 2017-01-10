import json

import boto3

def _dynamo_table():
    table_name = 'badger_urlsTable' # TODO Get this from the context!
    return boto3.resource('dynamodb').Table(table_name)

def _sns_topic():
    return boto3.client('sns')

def fetch_classes(urls):
    cached, uncached = [], []
    table = _dynamo_table()
    for url in urls:
        item = table.get_item(Keys={ 'url': url })
        if item:
            # From the docs - https://boto3.readthedocs.io/en/latest/guide/dynamodb.html#using-an-existing-table
            # it seems like get_item will just return a hash of the row keys and values.
            cached.append(item)
        else:
            uncached.append(url)
    return cached, uncached

def add_to_classifying_queue(urls):
    _sns_topic().publish(
        TopicArn='classify-topic',
        Message=json.dumps({ 'urls': urls })
    )

def main(event, context):
    urls = event["urls"]

    cached, uncached = fetch_saved_classes(urls)
    add_to_classifying_queue(uncached)

    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event,
        "classes": cached
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

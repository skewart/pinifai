import os

from clarifai.rest import ClarifaiApp
from clarifai.rest import Image as ClImage

MODEL_NAME = os.environ["MODEL_NAME"]

def _dynamo_table():
    table_name = 'badger_urlsTable' # TODO Get this from the context!
    return boto3.resource('dynamodb').Table(table_name)

def get_model():
    return ClarifaiApp().models().get(MODEL_NAME)

def classify(img_urls):
    return { input_url(output): parse_result(output) for output in get_results(img_urls)}

def get_results(img_urls):
    imgs = [ClImage(url=url) for url in img_urls]
    results = model.predict(imgs)
    return results["outputs"]

def input_url(result):
    return result["input"]["data"]["image"]["url"]

def parse_result(result):
    concepts = result["data"]["concepts"]
    return [concept["name"]: concept["value"] for concept in concepts]

def save_to_dynamo(classifications):
    table = _dynamo_table()
    for url, classes in classifications.iteritems():
        # TODO Maybe create an 'image' model, which could hold this logic for
        # translating itself to and from a DynamoDB row.
        table.put_item(
            Item={
                'url': url,
                'classes': classes
            }
        )

def main(event, context):
    classifications = classify(event['urls'])
    save_to_dynamo(classifications)

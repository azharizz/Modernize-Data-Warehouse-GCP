import os
from google.cloud import firestore
from google.cloud import pubsub_v1


# Initialize Firestore and Pub/Sub clients
db = firestore.Client()
publisher = pubsub_v1.PublisherClient()


def hello_firestore(event, context):

    print("GET FUNCTION RUN")

    topic_name = 'projects/project-azhar-385817/topics/customer-clustering-topic'

    # Get the Firestore document data
    document_id = context.resource.split('/')[-1]
    document_ref = db.collection('customers').document(document_id)
    document_data = document_ref.get().to_dict()

    print('DOCUMENT DATA : ', document_data)

    # Publish the document data to Pub/Sub
    message_data = str(document_data).encode('utf-8')

    print('MESSAGE DATA : ', message_data)

    future = publisher.publish(topic_name, message_data)
    print('Message published to Pub/Sub successfully:', future.result())

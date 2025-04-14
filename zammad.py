import os
from zammad_py import ZammadAPI
from nossl import no_ssl_verification


def upload_zammad_data():
    with no_ssl_verification():
        client = ZammadAPI(url='https://ssc-support.iwr.uni-heidelberg.de/api/v1/',
                           username='liam.keegan@iwr.uni-heidelberg.de', http_token=os.environ.get("ZAMMAD_TOKEN"))
        client.ticket.create({
            "title": "Test ticket from API",
            "group": "SSC Lead",
            "customer_id": "guess:captainkeggins@gmail.com",
            "created_at": "2023-10-01T12:00:00+02:00",
            "article": {
                "subject": "My subject",
                "body": "I am a message from the past!",
                "type": "note",
                "internal": False,
                "content_type": "text/html",
                "sender": "Customer",
            }}
        )

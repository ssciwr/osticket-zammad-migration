import os
from zammad_py import ZammadAPI
from nossl import no_ssl_verification


def upload_zammad_data():
    with no_ssl_verification():
        client = ZammadAPI(url='https://ssc-support.iwr.uni-heidelberg.de/api/v1/',
                           username='liam.keegan@iwr.uni-heidelberg.de', http_token=os.environ.get("ZAMMAD_TOKEN"))
        this_page = client.user.all()
        for user in this_page:
            print(user)

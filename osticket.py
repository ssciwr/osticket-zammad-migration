import mysql.connector
import pandas as pd
import os

def get_osticket_data() -> pd.DataFrame:
    cnx = mysql.connector.connect(
        user="liam",
        password=os.environ.get("OSTICKET_PASSWORD"),
        database="osticket")

    cur = cnx.cursor(buffered=True)

    cur.execute("SELECT ticket_id, number, user_id, user_email_id, email_id, lastupdate, created, updated FROM ost_ticket limit 2")
    df = pd.DataFrame(cur.fetchall(), columns=[i[0] for i in cur.description])
    df.head()

    cur.execute("SELECT user_id, address FROM ost_user_email limit 2")

    for x in cur:
        print(x)

    cur.execute("SELECT * FROM ost_user_email limit 2")

    for x in cur:
        print(x)

    return df
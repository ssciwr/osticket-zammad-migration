import mysql.connector
import pandas as pd
import os

def get_osticket_data() -> pd.DataFrame:
    cnx = mysql.connector.connect(
        user="liam",
        password=os.environ.get("OSTICKET_PASSWORD"),
        database="osticket")

    cur = cnx.cursor(buffered=True)

    # tickets
    cur.execute("SELECT ticket_id, number, user_id, user_email_id, email_id, lastupdate, created, updated FROM ost_ticket limit 20")
    df = pd.DataFrame(cur.fetchall(), columns=[i[0] for i in cur.description])
    df.head()

    # email addresses
    cur.execute("SELECT user_id, address FROM ost_user_email")
    df_email = pd.DataFrame(cur.fetchall(), columns=cur.description)
    df_email.set_index('user_id', inplace=True)
    df_email.head()
    df.join(df_email, on='user_id', how='left')
    df.drop('user_id', axis=1, inplace=True)
    df.head()

    # users
    cur.execute("SELECT id, name FROM ost_user")
    df_user = pd.DataFrame(cur.fetchall(), columns=cur.description)
    df_user.set_index('id', inplace=True)
    df_user.head()
    df.join(df_user, on='user_id', how='left')
    df.drop('user_id', axis=1, inplace=True)
    df.head()

    return df
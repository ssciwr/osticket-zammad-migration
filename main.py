import mysql.connector
import pandas as pd
import os, json
import base64
from zammad_py import ZammadAPI
from nossl import no_ssl_verification
from pprint import pprint
from mysql.connector.abstracts import MySQLCursorAbstract


def select_to_df(cur: MySQLCursorAbstract, table: str, columns: list[str]) -> pd.DataFrame:
    """
    Execute a SQL query to return the selected columns from the selected table as a pandas DataFrame, using first column as index.
    """
    query = f"SELECT {', '.join(columns)} FROM {table}"
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall(), columns=[i[0] for i in cur.description])
    df.set_index(columns[0], inplace=True)
    return df


def join(df_left: pd.DataFrame, df_right: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Left join the specified column in the left DataFrame, using the index of the right DataFrame.
    """
    return df_left.merge(df_right, left_on=column, right_index=True, how='left', validate="many_to_one")


def get_tickets(cur: MySQLCursorAbstract) -> pd.DataFrame:
    """
    Construct a dataframe with a row for each ticket
    """
    # ost_ticket: tickets
    df = select_to_df(cur, "ost_ticket", ["ticket_id", "number", "user_id", "status_id", "created"])

    # ost_ticket_status: status_id -> state
    df_status = select_to_df(cur, "ost_ticket_status", ["id", "state"])
    df = join(df, df_status, "status_id")

    # ost_user_email: user_id -> address
    df_email = select_to_df(cur, "ost_user_email", ["user_id", "address"])
    df = join(df, df_email, 'user_id')

    # ost_user: user_id -> name
    df_user = select_to_df(cur, "ost_user", ["id", "name"])
    df = join(df, df_user, 'user_id')

    # drop unnecessary columns
    df.drop(['user_id', 'status_id'], axis=1, inplace=True)
    return df


def get_threads(cur: MySQLCursorAbstract) -> pd.DataFrame:
    """
    Construct a dataframe with a row for each thread (message within a ticket), including any attachments
    """

    # ost_thread_entry: thread_id -> body, email recipients, etc
    # note: there can be multiple rows with the same thread_id since this is apparently actually the ticket_id
    df = select_to_df(cur, "ost_thread_entry", ["id", "thread_id", "user_id", "poster", "title", "body", "created", "format", "recipients"])
    return df


def get_attachments(cur: MySQLCursorAbstract, thread_id: int) -> list[dict]:
    dct = []
    # get file ids of any attachments for this thread
    cur.execute(f"SELECT file_id FROM ost_attachment where object_id = {thread_id}")
    file_ids = cur.fetchall()
    print(thread_id, file_ids)
    for (file_id,) in file_ids:
        cur.execute(f"SELECT type, name FROM ost_file where id = {file_id}")
        mimetype, filename = cur.fetchone()
        cur.execute(f"SELECT filedata FROM ost_file_chunk where file_id = {file_id} ORDER BY chunk_id")
        chunks = cur.fetchall()
        # concatenate chunks and convert from hex to base64
        data = base64.b64encode(b''.join([chunk[0] for chunk in chunks])).decode('utf-8')
        dct.append({
            "mime-type": mimetype,
            "filename": filename,
            "data": data
        })
    return dct


def get_article(cur, ticket_id, article_row) -> dict:
    [thread_id, user_id, poster, title, body, created, content_type, recipients] = article_row
    # recipients is a JSON encoded string of dict -> list[str] or dict[str,str] where the value is the email address
    emails = {"to": "", "cc": ""}
    if recipients:
        rec = json.loads(recipients)
        for key, value in rec.items():
            emails[key] = ", ".join(value.values() if isinstance(value, dict) else value)
    pprint(emails)
    return {
        "ticket_id": ticket_id,
        "subject": title,
        "body": body,
        "type": "email",
        "internal": False,
        "content_type": "text/html" if content_type=="html" else "text/plain",
        "sender": "Agent" if user_id==0 else "Customer",
        "created_at": f"{created}",
        "attachments": get_attachments(cur, thread_id),
        "from": "Scientific Software Center <ssc@iwr.uni-heidelberg.de>" if user_id==0 else f"{poster} <TODO@TODO>",
        "to": emails["to"],
        "cc": emails["cc"],
    }

def create_ticket(cur, ticket_id, ticket_row, article_row) -> int:
    with no_ssl_verification():
        client = ZammadAPI(url='https://ssc-support.iwr.uni-heidelberg.de/api/v1/',
                           username='liam.keegan@iwr.uni-heidelberg.de', http_token=os.environ.get("ZAMMAD_TOKEN"))
        [number, created, state, address, name] = ticket_row
        article = get_article(cur, ticket_id, article_row)
        print(article)
        res = client.ticket.create({
            "title": article["subject"],
            "group": "SSC Lead",
            "state": state,
            "number": f"{number}",
            "customer_id": f"guess:{address}",
            "created_at": f"{created}",
            "article": article,
        }
        )
        pprint(res)
        ticket_id = res["id"]
        return ticket_id

def create_article(cur, ticket_id, article_row):
    with no_ssl_verification():
        client = ZammadAPI(url='https://ssc-support.iwr.uni-heidelberg.de/api/v1/',
                           username='liam.keegan@iwr.uni-heidelberg.de', http_token=os.environ.get("ZAMMAD_TOKEN"))
        res = client.ticket_article.create(get_article(cur, ticket_id, article_row))
        pprint(res)


def delete_all_zammad_tickets():
    with no_ssl_verification():
        client = ZammadAPI(url='https://ssc-support.iwr.uni-heidelberg.de/api/v1/',
                           username='liam.keegan@iwr.uni-heidelberg.de', http_token=os.environ.get("ZAMMAD_TOKEN"))
        page = client.ticket.all()
        while page:
            print(page)
            for ticket in page:
                print(f"Deleting ticket {ticket['id']}")
                client.ticket.destroy(ticket['id'])
            page = page.next_page()


def main():
    # connect to osticket mysql database
    cnx = mysql.connector.connect(
        user="liam",
        password=os.environ.get("OSTICKET_PASSWORD"),
        database="osticket")
    cur = cnx.cursor(buffered=True)

    delete_all_zammad_tickets()

    # get tickets and threads
    df_tickets = get_tickets(cur)
    df_threads = get_threads(cur)

    print(df_tickets.head(10000))

    print(df_threads.head(10000))

    for ticket_id, ticket_row in df_tickets.head(5).iterrows():
        print(f"ID {ticket_id}")
        print(f"ticket_row:\n {ticket_row}")
        threads = df_threads.loc[df_threads["thread_id"] == ticket_id]
        print(threads)
        zammad_ticket_id = create_ticket(cur, ticket_id, ticket_row, threads.iloc[0])
        print(zammad_ticket_id)
        for thread_id, thread_row in threads.iloc[1:].iterrows():
            print(f"THREAD ID {thread_id}")
            print(f"THREAD TUPLE {thread_row}")
            create_article(cur, zammad_ticket_id, thread_row)



if __name__ == '__main__':
    main()
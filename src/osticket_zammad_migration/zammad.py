import pandas as pd
import os
import json
import logging
from zammad_py import ZammadAPI
from .nossl import no_ssl_verification
from pprint import pformat
from .osticket import get_threads, get_tickets, get_attachments
import mysql.connector
from mysql.connector.abstracts import MySQLCursorAbstract


def _article_from_thread(
    cur: MySQLCursorAbstract,
    zammad_ticket_id: int,
    thread_id: int,
    thread_row: pd.Series,
) -> dict[str, str | bool | int | list]:
    """
    Convert a thread from osticket to a Zammad article
    """
    emails = {
        "from": f"{thread_row['poster']} <{thread_row['address']}>",
        "to": "",
        "cc": "",
    }
    # recipients is a JSON encoded string of either dict[str,list[str]] or dict[str,dict[str,str]],
    # I think depending on if the email addresses are also users in osticket?
    # Here we just ignore any dict keys (osticket user ids) and just take the values (email addresses)
    if thread_row["recipients"]:
        rec = json.loads(thread_row["recipients"])
        for key, value in rec.items():
            emails[key] = ", ".join(
                value.values() if isinstance(value, dict) else value
            )
    return {
        "ticket_id": zammad_ticket_id,
        "subject": thread_row["title"],
        "body": thread_row["body"],
        "type": "email",
        "internal": False,
        "content_type": "text/html" if thread_row["format"] == "html" else "text/plain",
        "sender": "Agent" if thread_row["user_id"] == 0 else "Customer",
        "created_at": f"{thread_row['created']}",
        "attachments": get_attachments(cur, thread_id),
        "from": emails["from"],
        "to": emails["to"],
        "cc": emails["cc"],
    }


def _create_ticket(
    cur: MySQLCursorAbstract,
    ticket_id: int,
    ticket_row: pd.Series,
    threads: pd.DataFrame,
    dry_run: bool,
):
    """
    Create a Zammad ticket from an osticket ticket, its threads and any of their attachments
    """
    with no_ssl_verification():
        client = ZammadAPI(
            url="https://ssc-support.iwr.uni-heidelberg.de/api/v1/",
            username="liam.keegan@iwr.uni-heidelberg.de",
            http_token=os.environ.get("ZAMMAD_TOKEN"),
        )
        ticket = {
            "title": ticket_row["subject"],
            "group": "SSC Lead",
            "state": ticket_row["state"],
            "number": f"{ticket_row['number']}",
            "customer_id": f"guess:{ticket_row['address']}",
            "created_at": f"{ticket_row['created']}",
            "article": _article_from_thread(
                cur, ticket_id, threads.index[0], threads.iloc[0]
            ),
        }
        logging.debug(f"Creating ticket:\n{pformat(ticket)}")
        if not dry_run:
            res = client.ticket.create(ticket)
            logging.debug(f"Created ticket:\n{pformat(res)}")
        else:
            res = {"id": 1}
        # for each subsequent thread in this ticket, create an article
        zammad_ticket_id = res["id"]
        for thread_id, thread_row in threads.iloc[1:].iterrows():
            logging.debug(
                f"Creating article for zammad ticket {zammad_ticket_id} from osticket thread {thread_id}"
            )
            article = _article_from_thread(cur, zammad_ticket_id, thread_id, thread_row)
            logging.debug(f"Creating article:\n{pformat(article)}")
            if not dry_run:
                res = client.ticket_article.create(article)
                logging.debug(f"Created article:\n {pformat(res)}")


def _delete_all_existing_tickets(dry_run: bool):
    """
    DANGER!! Deletes all existing tickets in Zammad.
    """
    logging.warning("Deleting all existing Zammad tickets")
    if dry_run:
        return
    with no_ssl_verification():
        client = ZammadAPI(
            url="https://ssc-support.iwr.uni-heidelberg.de/api/v1/",
            username="liam.keegan@iwr.uni-heidelberg.de",
            http_token=os.environ.get("ZAMMAD_TOKEN"),
        )
        page = client.ticket.all()
        while page:
            for ticket in page:
                logging.warning(f"  - deleting ticket {ticket['id']}")
                client.ticket.destroy(ticket["id"])
            page = client.ticket.all()


def migrate_tickets(delete_all_existing_tickets: bool, dry_run: bool):
    """
    Migrate all osticket tickets to Zammad
    """
    # connect to osticket mysql database
    cnx = mysql.connector.connect(
        user="liam", password=os.environ.get("OSTICKET_PASSWORD"), database="osticket"
    )
    cur = cnx.cursor(buffered=True)

    # delete all existing tickets in zammad
    if delete_all_existing_tickets:
        _delete_all_existing_tickets(dry_run)

    # get osticket tickets and threads
    df_tickets = get_tickets(cur)
    df_threads = get_threads(cur)

    # migrate each ticket
    for ticket_id, ticket_row in df_tickets.iterrows():
        logging.info(f"Migrating osticket {ticket_id} (#{ticket_row['number']})")
        # get all threads for this ticket
        threads = df_threads.loc[df_threads["ticket_id"] == ticket_id]
        if len(threads) == 0:
            logging.warning(f"Ticket {ticket_id} has no threads - ignoring")
            continue
        logging.info(f"  - found {len(threads)} thread(s) for this ticket")
        _create_ticket(cur, ticket_id, ticket_row, threads, dry_run)

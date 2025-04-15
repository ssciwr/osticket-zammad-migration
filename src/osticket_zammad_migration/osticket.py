import pandas as pd
import base64
from mysql.connector.abstracts import MySQLCursorAbstract


def select_to_df(
    cur: MySQLCursorAbstract, table: str, columns: list[str]
) -> pd.DataFrame:
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
    return df_left.merge(
        df_right, left_on=column, right_index=True, how="left", validate="many_to_one"
    )


def get_tickets(cur: MySQLCursorAbstract) -> pd.DataFrame:
    """
    Construct a dataframe with a row for each ticket
    """
    # ost_ticket: ticket id, number, user_id, status_id, created
    df = select_to_df(
        cur, "ost_ticket", ["ticket_id", "number", "user_id", "status_id", "created"]
    )

    # ost_ticket_status: status_id -> state
    df_status = select_to_df(cur, "ost_ticket_status", ["id", "state"])
    df = join(df, df_status, "status_id")

    # ost_user_email: user_id -> address
    df_email = select_to_df(cur, "ost_user_email", ["user_id", "address"])
    df = join(df, df_email, "user_id")

    # ost_user: user_id -> name
    df_user = select_to_df(cur, "ost_user", ["id", "name"])
    df = join(df, df_user, "user_id")

    # ost_ticket__cdata: ticket id -> subject
    df_subject = select_to_df(cur, "ost_ticket__cdata", ["ticket_id", "subject"])
    df = df.merge(
        df_subject, left_index=True, right_index=True, how="left", validate="one_to_one"
    )

    # drop unnecessary columns
    df.drop(["user_id", "status_id"], axis=1, inplace=True)
    return df


def get_threads(cur: MySQLCursorAbstract) -> pd.DataFrame:
    """
    Construct a dataframe with a row for each thread (message within a ticket), including any attachments
    """
    # ost_thread_entry: the `id` column is the (unique) id of the thread
    # NOTE: there is also a `thread_id` column which is apparently actually the corresponding ticket_id! (not unique)
    df = select_to_df(
        cur,
        "ost_thread_entry",
        [
            "id",
            "thread_id",
            "user_id",
            "poster",
            "title",
            "body",
            "created",
            "format",
            "recipients",
        ],
    )
    # rename confusing `thread_id` column to `ticket_id`
    df.rename(columns={"thread_id": "ticket_id"}, inplace=True)

    # ost_user_email: user_id -> address
    df_email = select_to_df(cur, "ost_user_email", ["user_id", "address"])
    # no entry for user_id 0, which is the ticketing system
    df_email.loc[0] = {
        "address": "Scientific Software Center <ssc@iwr.uni-heidelberg.de>"
    }
    df = join(df, df_email, "user_id")
    return df


def get_attachments(cur: MySQLCursorAbstract, thread_id: int) -> list[dict[str, str]]:
    """
    Construct a list of attachments for this thread, with filename, mimetype and base64-encoded binary data for each.
    """
    attachments = []
    # get file ids of any attachments for this thread
    cur.execute(f"SELECT file_id FROM ost_attachment where object_id = {thread_id}")
    file_ids = cur.fetchall()
    for (file_id,) in file_ids:
        # get filename and mime type
        cur.execute(f"SELECT type, name FROM ost_file where id = {file_id}")
        mimetype, filename = cur.fetchone()
        # get binary chunks in order
        cur.execute(
            f"SELECT filedata FROM ost_file_chunk where file_id = {file_id} ORDER BY chunk_id"
        )
        # concatenate chunks
        hex_data = b"".join([chunk[0] for chunk in cur.fetchall()])
        # convert from hex to base64
        data = base64.b64encode(hex_data).decode("utf-8")
        attachments.append({"mime-type": mimetype, "filename": filename, "data": data})
    return attachments

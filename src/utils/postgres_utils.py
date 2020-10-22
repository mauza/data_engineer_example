


def run_sql(conn, query):
    with conn.cursor() as cur:
        cur.execute(query)

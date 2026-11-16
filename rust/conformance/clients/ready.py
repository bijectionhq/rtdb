import os
import time

import psycopg


deadline = time.monotonic() + 90
while True:
    try:
        with psycopg.connect(host=os.environ["PG_HOST"], port=os.environ["PG_PORT"],
                            user="xtdb", dbname="xtdb", connect_timeout=3, autocommit=True) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1", prepare=False)
                if cursor.fetchall() != [(1,)]:
                    raise ValueError("packaged server returned an invalid readiness result")
        break
    except psycopg.OperationalError:
        if time.monotonic() >= deadline:
            raise
        time.sleep(0.25)

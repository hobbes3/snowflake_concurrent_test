#!/usr/bin/env python
import snowflake.connector
import credentials as creds

# Gets the version
ctx = snowflake.connector.connect(
    user=creds.USER,
    password=creds.PASSWORD,
    account=creds.ACCOUNT
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()

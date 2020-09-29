#!/usr/bin/env python
import snowflake.connector
import credentials as creds

import time
import logging
import logging.handlers
import string
import random
import argparse
from secrets import token_urlsafe
from multiprocessing.dummy import Pool

def create_user(user):
    username = user["user"]
    password = user["password"]

    logger.info("Creating user {}...".format(username))
    cursor.execute("create user if not exists {}".format(username))

    logger.info("Setting new password and making user {} temporary (expires in 24 hours)...".format(username))
    cursor.execute("alter user {} set password='{}' days_to_expiry=1".format(username, password))

    logger.info("Assigning role readonly to user {}...".format(username))
    cursor.execute("grant role readonly to user {}".format(username))

    run_searches(username, password)

def run_searches(username, password):
    logger.info("Connecting to Snowflake as user {} with password '{}'...".format(username, password))
    user_conn = snowflake.connector.connect(
        user=username,
        password=password,
        account=creds.ACCOUNT,
        role="readonly",
        warehouse="xsmall",
        database="snowflake_sample_data",
        # Schema is case sensitive???
        #schema="tcpds_sf10tcl",
        schema="TPCDS_SF10TCL",
    )
    user_cursor = user_conn.cursor()
    user_cursor.execute("set USE_CACHED_RESULT = false")

    #print(user_cursor.execute("select current_role(), current_warehouse(), current_database(), current_schema()").fetchall()[0])

    time_end = time.time() + args.seconds

    logger.info("Running sample searches for user {} for the next {} seconds...".format(username, args.seconds))
    while time.time() < time_end:
        search_time_start = time.time()
        # This search takes between 1-5 seconds to run normally.
        user_cursor.execute("select * from store_sales limit 100000")
        logger.info("Search done! Took {} seconds.".format(time.time()-search_time_start))

    user_conn.close()

def delete_user(user):
    username = user["user"]

    logger.info("Deleting user {}...".format(username))
    cursor.execute("drop user if exists {}".format(username))

def delete_users(users):
    delete_pool = Pool(len(users))
    delete_pool.imap_unordered(delete_user, users)
    delete_pool.close()
    delete_pool.join()

if __name__ == '__main__':
    script_time_start = time.time()

    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        prog='concurrent_test',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Create temporary users to continously run searches for some time."
    )
    parser.add_argument("-u", "--users", type=int, default=3, help="Number of users.")
    parser.add_argument("-s", "--seconds", type=int, default=10, help="How long to run the script in seconds.")
    args = parser.parse_args()

    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)-7s] (%(threadName)-10s) %(message)s"))
    logger.addHandler(handler)

    logger.info("Connecting to Snowflake...")

    conn = snowflake.connector.connect(
        user=creds.USER,
        password=creds.PASSWORD,
        account=creds.ACCOUNT,
        # Only accountadmin and secrityadmin can create users (by default)
        role="accountadmin",
    )
    # Main cursor
    cursor = conn.cursor()

    user_count = args.users

    # Generate a password that complies with Snowflake default password policy.
    def generate_password():
        all_letters = string.ascii_letters
        lowers = all_letters[:26]
        uppers = all_letters[26:]

        # Guarantees at least one upper letter, one lower letter, and one number.
        password = token_urlsafe(16) + random.choice(lowers) + random.choice(uppers) + str(random.randint(0, 9))

        return "".join(random.sample(password, len(password)))

    users = [{
        "user": "concurrent_"+str(i+1),
        "password": generate_password(),
    } for i in range(user_count)]

    print("Press ctrl-c to cancel at anytime starting now.")

    pool = Pool(user_count)

    try:
        pool.imap_unordered(create_user, users)
        pool.close()
        pool.join()
    except KeyboardInterrupt:
        logger.warning("\nCaught KeyboardInterrupt! Terminating workers and cleaing up. Please wait...")
        pool.terminate()
        pool.join()
    finally:
        delete_users(users)
        conn.close()
        logger.info("Done. Total elapsed time: {} seconds".format(time.time() - script_time_start))

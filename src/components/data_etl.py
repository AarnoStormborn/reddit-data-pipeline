import sys
import praw
import pathlib
import sqlite3

from datetime import datetime
from src.logger import logging
from src.exception import CustomException

def run_data_etl():

    logging.info("Starting Data Pipeline")

    reddit = praw.Reddit(
        client_id="667E_M0MuHPwV0ohjDgyDg",
        client_secret="mYP4CYNWH65j_W4pPB3ZbP0eHgVcyQ",
        user_agent="u/AarnoStormborn"
    )

    reddit_posts = []

    logging.info("Data Extracted")
    for submission in reddit.subreddit("movies").hot(limit=15):

        created_at = datetime.utcfromtimestamp(submission.created_utc).strftime('%d-%m-%Y %H:%M:%S')

        reddit_posts.append((
            str(submission.author), submission.title, submission.score, submission.upvote_ratio,
            int(submission.over_18), int(submission.spoiler), submission.url, created_at
        ))

    print(type(created_at), created_at)
    print(reddit_posts[0])

    logging.info("Connecting to Database")
    db_name = 'reddit.db'

    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()

    try:
        cursor.execute("""CREATE TABLE post (author TEXT, title TEXT, upvotes INTEGER, upvote_ratio REAL,
                                         is_nsfw INTEGER, is_spoiler INTEGER, post_url TEXT, created_at TEXT)""")
        logging.info("Database Table Created")
    except:
        logging.info("Table already exists")

    
    try:
        cursor.executemany("INSERT INTO post VALUES(?,?,?,?,?,?,?,?)", reddit_posts)
        connection.commit()
        logging.info("Data loaded to Database")
    except Exception as e:
        logging.error(CustomException(e, sys))        
    
    connection.close()
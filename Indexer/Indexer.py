import re
import pymysql
import json
from collections import defaultdict
import os
import logging
import boto3
from mpi4py import MPI
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - Indexer - %(levelname)s - %(message)s")

class BasicIndexerNode:
    def __init__(self, index_dir="whoosh_index", use_sql=True):
        self.index_dir = index_dir
        self.use_sql = use_sql

        if use_sql:
            self._init_sql_connection()
            self._create_table()

        schema = Schema(url=ID(stored=True, unique=True), content=TEXT(stored=True))

        if not os.path.exists(index_dir):
            os.mkdir(index_dir)
            self.index = create_in(index_dir, schema)
        else:
            self.index = open_dir(index_dir)

    def _init_sql_connection(self):
        self.conn = pymysql.connect(
            host="database.cgvouae4ojwr.us-east-1.rds.amazonaws.com",
            port=3306,
            user="admin",
            password="database",
            database="database12",
            autocommit=True
        )
        self.cursor = self.conn.cursor()

    def _create_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS inverted_index2 (
                word VARCHAR(255) PRIMARY KEY,
                url TEXT
            )
        """)

    def insert_sql(self, word, url):
        try:
            self.cursor.execute("SELECT url FROM inverted_index2 WHERE word = %s", (word,))
            result = self.cursor.fetchone()
            if result:
                existing_urls = result[0].split(',')
                if url not in existing_urls:
                    existing_urls.append(url)
                    updated_url_list = ','.join(existing_urls)
                    self.cursor.execute(
                        "UPDATE inverted_index2 SET url = %s WHERE word = %s", (updated_url_list, word)
                    )
            else:
                self.cursor.execute("INSERT INTO inverted_index2 (word, url) VALUES (%s, %s)", (word, url))
        except Exception as e:
            logging.error(f"Failed to insert ({word}, {url}) into SQL: {e}")

    def ingest_from_crawler(self, url: str, text: str):
        writer = self.index.writer()
        writer.update_document(url=url, content=text)
        writer.commit()

        if self.use_sql:
            words = re.findall(r'\w+', text.lower())
            for word in words:
                self.insert_sql(word, url)

    def search(self, keyword: str):
        qp = QueryParser("content", schema=self.index.schema)
        q = qp.parse(keyword)

        results_list = []
        with self.index.searcher() as searcher:
            results = searcher.search(q, limit=10)
            for r in results:
                results_list.append(r['url'])
        return results_list

def fetch_from_sqs(queue_url, max_messages=5, wait_time=2):
    sqs = boto3.client('sqs', region_name='us-east-1')
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=max_messages,
        WaitTimeSeconds=wait_time
    )
    return response.get('Messages', [])

def indexer_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    status = MPI.Status()

    indexer = BasicIndexerNode(use_sql=True)
    logging.info(f"Indexer Node (rank {rank}) started.")

    SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/696726802797/ScrapeQueue"

    while True:
        # 1. Poll messages from SQS
        items = fetch_from_sqs(SQS_QUEUE_URL)
        for item in items:
            try:
                message_body = json.loads(item['Body'])
                url = message_body.get('url')
                text = message_body.get('text')
                if url and text:
                    logging.info(f"Ingesting content from {url}")
                    indexer.ingest_from_crawler(url, text)
                    # Delete message from queue after processing
                    sqs.delete_message(
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=item['ReceiptHandle']
                    )
            except Exception as e:
                logging.error(f"Error processing SQS message: {e}")

        # 2. Handle incoming MPI messages
        if comm.Iprobe(source=0, tag=MPI.ANY_TAG, status=status):
            try:
                source = status.Get_source()
                tag = status.Get_tag()

                if tag == 100:  # Ping
                    comm.recv(source=source, tag=tag)  # Consume ping
                    comm.send("pong", dest=source, tag=101)
                    logging.info("Responded to ping from master.")

                elif tag == 10:  # Search query
                    keyword = comm.recv(source=source, tag=tag)
                    logging.info(f"Received search query: {keyword}")
                    results = indexer.search(keyword)
                    comm.send(results, dest=source, tag=11)
                    logging.info(f"Sent search results to master: {results}")

            except Exception as e:
                logging.error(f"Error during MPI message handling: {e}")

        time.sleep(0.1)

if __name__ == '__main__':
    indexer_process()

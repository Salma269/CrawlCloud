import boto3
import pymysql
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import signals
from bs4 import BeautifulSoup
import json
import time
import logging
import mpi4py.MPI
import threading

logging.basicConfig(level=logging.INFO, format="%(asctime)s - Crawler - %(levelname)s - %(message)s")

class CrawlingSpider(CrawlSpider):
    name = "mycrawler"
    allowed_domains = []

    custom_settings = {
        'ROBOTSTXT_OBEY': True,
    }

    sqs = boto3.client('sqs', region_name='us-east-1')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/696726802797/ScrapeQueue'
    input_queue_url = 'https://sqs.us-east-1.amazonaws.com/696726802797/TaskQueueForCrawlers'

    rules = (
        Rule(LinkExtractor(), callback="parse_item", follow=True),
    )

    def __init__(self, *args, **kwargs):
        super(CrawlingSpider, self).__init__(*args, **kwargs)

        self.db_conn = pymysql.connect(
            host="database.cgvouae4ojwr.us-east-1.rds.amazonaws.com",
            port=3306,
            user="admin",
            password="database",
            database="database12",
            autocommit=True)
        self.db_cursor = self.db_conn.cursor()

        self.db_cursor.execute("""CREATE TABLE IF NOT EXISTS crawled_pages(
                                  id INT AUTO_INCREMENT PRIMARY KEY,
                                  url TEXT,
                                  title TEXT,
                                  html MEDIUMTEXT)""")

        self.comm = mpi4py.MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()

        self.exported_urls = set()
        self.urls_parsed = 0
        self.sent_urls = 0
        self.message_id = ""
        self.logger.info("Received 'start' signal. Proceeding with task fetching.")
       
        self.fetch_initial_task()

    def fetch_initial_task(self):
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.input_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )

            if 'Messages' in response:
                message = response['Messages'][0]
                body = json.loads(message['Body'])

                self.message_id = message['MessageId']

                self.start_urls = [body.get('url')]
                self.allowed_domains = body.get('allowed_domains', [])
                self.custom_settings['DEPTH_LIMIT'] = body.get('depth', 0)
                self.custom_settings['DOWNLOAD_DELAY'] = body.get('delay', 1.0)

                receipt_handle = message['ReceiptHandle']
                self.sqs.delete_message(
                    QueueUrl=self.input_queue_url,
                    ReceiptHandle=receipt_handle
                )

                self.logger.info(f"Fetched task: URL: {body.get('url')}, Depth: {self.custom_settings['DEPTH_LIMIT']}")
                self.logger.info(f"Sending {self.message_id} signal to master")
                self.comm.send(self.message_id, dest=0, tag=2)
                
                self.start_crawl()
            else:
                self.logger.info("No messages available in the input queue.")
                
        except Exception as e:
            self.logger.error(f"Error fetching task from input queue: {e}")

    def start_crawl(self):
        self.logger.info("Starting the crawl process.")
        super().start_requests()

    def parse_item(self, response):
        #try:
        #    status = MPI.Status()
        #    if self.comm.iprobe(source=0, tag=100, status=status):
        #        message = self.comm.recv(source=0, tag=100)
        #        if message == "ping":
        #            self.comm.send("pong", dest=0, tag=101)
        #            self.logger.info("SENDING PONG.")
        #    else:
        #        self.logger.info("NO PING RECIEVEDDDD")
        #except Exception as e:
        #    self.logger.info("NO PING RECIEVEDDDD")
        #    pass

        self.logger.info(f"Processing page: {response.url}")
        self.urls_parsed += 1
        
        extractor = LinkExtractor()
        links = extractor.extract_links(response)

        #other_links = []
        #if links:
        #    first_link = links[0].url
        #    other_links = [link.url for link in links[1:]]
        #else:
        #    first_link = None

        self.process_and_send_to_sqs(response)

        if links:
            self.logger.info(f"Sending {len(links)} URLs to master")
            self.comm.send(links, dest=0, tag=99)
            self.sent_urls += len(links)
        
        #if first_link and first_link not in self.exported_urls:
        #    self.exported_urls.add(first_link)
        #    yield response.follow(first_link, callback=self.parse_item)

    def process_and_send_to_sqs(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')

        title_tag = soup.find('h1')
        title = title_tag.get_text(strip=True) if title_tag else 'No title'

        html_content = response.text

        try:
            self.db_cursor.execute(
                "INSERT INTO crawled_pages (url, title, html) VALUES (%s, %s, %s)",
                (response.url, title, html_content)
            )
            self.logger.info(f"Inserted page into SQL: {response.url}")
        except Exception as e:
            self.logger.error(f"Error inserting into SQL: {e}")

        message_body = response.text  # Entire page as body of the message
        message_attributes = {
            'url': {
                'DataType': 'String',
                'StringValue': response.url
            },
            'title': {
                'DataType': 'String',
                'StringValue': title
            },
            'content': {
                'DataType': 'String',
                'StringValue': html_content[:1000]
            }
        }

        try:
            response_sqs = self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=html_content,
                MessageAttributes=message_attributes
            )
            self.logger.info(f"Sent message to SQS, Message ID: {response_sqs['MessageId']}")
        except Exception as e:
            self.logger.error(f"Error sending message to SQS: {e}")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Connect the spider's closed signal."""
        spider = super(CrawlingSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        """Handle actions when the spider closes."""
        self.logger.info("Spider closed. Sending DONE signal to master.")
        result = {
            "status": "done",
            "task_id": self.message_id
        }
        #stop_event.set()
        #self.ping_thread.join()
        self.comm.send(result, dest=0, tag=1)  # Send to master (rank 0) with tag 2
        self.logger.info(f"Spider closed after parsing {self.urls_parsed} and URLs and sending {self.sent_urls} URLs. Sending DONE signal to master.")

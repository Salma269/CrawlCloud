import boto3
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

def ping_listener(comm, logger):
    status = MPI.Status()
    while not stop_event.is_set():
        if comm.iprobe(source=0, tag=100, status=status):
            message = comm.recv(source=0, tag=100)
            if message == "ping":
                comm.send("pong", dest=0, tag=101)
                logger.info("SENDING PONG.")
        else:
            logger.info("FDS")
        time.sleep(0.1)  # prevent busy waiting

stop_event = threading.Event()

class CrawlingSpider(CrawlSpider):
    name = "mycrawler"
    allowed_domains = []  # Initially empty, will be set dynamically

    custom_settings = {
        'ROBOTSTXT_OBEY': True,
    }

    sqs = boto3.client('sqs', region_name='us-east-1')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/696726802797/ScrapeQueue'
    input_queue_url = 'https://sqs.us-east-1.amazonaws.com/696726802797/TaskQueueForCrawlers'

    rules = (
        Rule(LinkExtractor(), callback="parse_item", follow=False),
    )


    def __init__(self, *args, **kwargs):
        super(CrawlingSpider, self).__init__(*args, **kwargs)

        # MPI setup for communication between master and crawler (worker)
        self.comm = mpi4py.MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()

        # Track already sent or followed URLs
        self.exported_urls = set()
        self.urls_parsed = 0
        self.sent_urls = 0
        self.message_id = ""
        self.logger.info("Received 'start' signal. Proceeding with task fetching.")
        #self.ping_thread = threading.Thread(target=ping_listener, args=(self.comm, self.logger))
        #self.ping_thread.start()
        self.fetch_initial_task()


        #self.wait_for_start_signal()

    
    def wait_for_start_signal(self):
        self.logger.info(f"Crawler {self.rank} waiting for 'start' signal from master...")
        while True:
            message = self.comm.recv(source=0, tag=1)  # Master sends "start" signal
            if message == "start":
                self.logger.info("Received 'start' signal. Proceeding with task fetching.")
                self.fetch_initial_task()
                break

    def fetch_initial_task(self):
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.input_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20  # Long poll for 20 seconds
            )

            if 'Messages' in response:
                message = response['Messages'][0]
                body = json.loads(message['Body'])

                self.message_id = message['MessageId']

                # Dynamically set the start URLs, allowed domains, and other settings
                self.start_urls = [body.get('url')]
                self.allowed_domains = body.get('allowed_domains', [])
                #self.start_urls = ["http://books.toscrape.com/"]
                #self.allowed_domains = ["toscrape.com"]
                self.custom_settings['DEPTH_LIMIT'] = body.get('depth', 0)  # Set depth limit
                self.custom_settings['DOWNLOAD_DELAY'] = body.get('delay', 1.0)  # Set download delay

                #self.custom_settings['DEPTH_LIMIT'] = 1
                #self.custom_settings['DOWNLOAD_DELAY'] = 1.0

                # Delete the message from the queue after processing
                receipt_handle = message['ReceiptHandle']
                self.sqs.delete_message(
                    QueueUrl=self.input_queue_url,
                    ReceiptHandle=receipt_handle
                )

                self.logger.info(f"Fetched task: URL: {body.get('url')}, Depth: {self.custom_settings['DEPTH_LIMIT']}")
                
                comm.send(self.message_id, dest=0, tag=2)
                # Start crawling after receiving the task
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
        #   pass
        
        #while True:
        #    pass

        self.logger.info(f"Processing page: {response.url}")
        self.urls_parsed += 1
        # Step 1: Extract all links
        extractor = LinkExtractor()
        links = extractor.extract_links(response)

        # Step 2: Keep the first link, store the rest
        other_links = []
        if links:
            first_link = links[0].url
            other_links = [link.url for link in links[1:]]
        else:
            first_link = None

        # Step 3: Prevent crawling of other links by ignoring them
        for url in other_links:
            self.exported_urls.add(url)

        # Step 4: Process and send scraped data to SQS
        self.process_and_send_to_sqs(response)

        # Step 5: Send the list of other links to the master (rank 0)
        if other_links:
            self.logger.info(f"Sending {len(other_links)} URLs to master")
            self.comm.send(other_links, dest=0, tag=99)
            self.sent_urls += len(other_links)

        # Step 6: Follow the first link if it exists and hasn't been exported
        if first_link and first_link not in self.exported_urls:
            self.exported_urls.add(first_link)
            yield response.follow(first_link, callback=self.parse_item)


    def process_and_send_to_sqs(self, response):
        """Scrape the page, prepare data, and send it to SQS."""
        soup = BeautifulSoup(response.text, 'html.parser')

        # Example of extracting data using BeautifulSoup
        title_tag = soup.find('h1')
        title = title_tag.get_text(strip=True) if title_tag else 'No title'

        # Prepare the message to be sent to SQS
        message_body = response.text  # Entire HTML page as the body of the message
        message_attributes = {
            'url': {
                'DataType': 'String',
                'StringValue': response.url
            },
            'title': {
                'DataType': 'String',
                'StringValue': title
            }
        }

#        try:
#            # Send the message to SQS queue
#            response = self.sqs.send_message(
#                QueueUrl=self.queue_url,
#                MessageBody=message_body,
#                MessageAttributes=message_attributes
#            )
#            self.logger.info(f"Sent message to SQS, Message ID: {response['MessageId']}")
#        except Exception as e:
#            self.logger.error(f"Error sending message to SQS: {e}")

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
        
        



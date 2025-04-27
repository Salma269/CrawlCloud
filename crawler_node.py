import boto3
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import signals
from bs4 import BeautifulSoup
import json
import time

class CrawlingSpider(CrawlSpider):
    name = "mycrawler"
    allowed_domains = ["toscrape.com"]

    custom_settings = {
        'ROBOTSTXT_OBEY': True,        
        'DOWNLOAD_DELAY': 1.0,          
        'DEPTH_LIMIT': 1,               # Default depth limit (this can be overridden via spider argument)
    }

    temp_data = []  # Temporary storage


    sqs = boto3.client('sqs', region_name='us-east-1')  
    queue_url = 'https://sqs.us-east-1.amazonaws.com/696726802797/ScrapeQueue'  
    input_queue_url = 'https://sqs.us-east-1.amazonaws.com/696726802797/TaskQueueForCrawlers'  

    rules = (
        Rule(LinkExtractor(), callback="parse_item", follow=True),  
    )

    def __init__(self, *args, **kwargs):
        super(CrawlingSpider, self).__init__(*args, **kwargs)
        self.depth = 1  # Default depth value

        self.fetch_initial_task()

    def fetch_initial_task(self):
        """
        Fetches a task from the input queue. This task contains the URL and depth to start crawling from.
        """
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.input_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20  # Long poll for 20 seconds
            )

            if 'Messages' in response:
                message = response['Messages'][0]
                body = json.loads(message['Body'])

                # Extract URL and depth from the message
                url = body.get('url')
                depth = body.get('depth', 1)  # Default to 1 if not provided

                # Set the start_urls and depth dynamically
                self.start_urls = [url]
                self.depth = depth
                self.custom_settings['DEPTH_LIMIT'] = depth  # Update depth limit

                # Delete the message from the queue after processing
                receipt_handle = message['ReceiptHandle']
                self.sqs.delete_message(
                    QueueUrl=self.input_queue_url,
                    ReceiptHandle=receipt_handle
                )

                self.logger.info(f"Fetched task: URL: {url}, Depth: {depth}")
            else:
                self.logger.info("No messages available in the input queue.")

        except Exception as e:
            self.logger.error(f"Error fetching task from input queue: {e}")

    def parse_item(self, response):
        # If it's the first page (seed page), scrape the links, then stop following them
        if response.meta.get('depth', 0) == self.depth:
            self.logger.info(f"Processing page: {response.url} (No further crawling)")

            # Scrape all links on the seed page and process them
            self.scrape_links_on_seed_page(response)

            # Send the page content to SQS
            self.process_and_send_to_sqs(response)

            # Mark the depth for links not to be followed further
            return  # Stop further crawling from this page

        # For other pages, process them but don't follow links (depth is managed by the rule)
        self.logger.info(f"Processing page: {response.url} (No further crawling)")

        # Send the page content to SQS
        self.process_and_send_to_sqs(response)

    def scrape_links_on_seed_page(self, response):
        # Only scrape the links on the seed page, not further ones
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', href=True)
        
        # Log or process these links as needed
        for link in links:
            self.logger.info(f"Found link on seed page: {link.get('href')}")

    def process_and_send_to_sqs(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')  # Parse page with BeautifulSoup

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

        # Send the message to the SQS queue
        self.send_message_to_queue(message_body, message_attributes)

    def send_message_to_queue(self, message_body, message_attributes):
        """
        Send the crawled HTML page and its attributes (url, title) to the SQS queue.
        """
        try:
            # Send the message to SQS queue
            response = self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=message_body,  # HTML content as the message body
                MessageAttributes=message_attributes  # URL and Title as message attributes
            )

            # Log message send success
            self.logger.info(f"Sent message to SQS, Message ID: {response['MessageId']}")
        except Exception as e:
            self.logger.error(f"Error sending message to SQS: {e}")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        # Allow depth parameter to be passed when running the spider
        spider = super(CrawlingSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        # Optionally, you can handle any final actions when the spider finishes
        self.logger.info(f"Spider closed. Crawled {len(self.temp_data)} pages.")

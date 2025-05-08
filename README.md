# Crawl Cloud
A Cloud-Based Web Crawler

This project is a distributed web crawler and search engine built on AWS. Each node in the system runs on a separate AWS EC2 instance. The system includes a web-based interface for starting crawls and performing keyword searches.

## How to Start the System

1. **Launch the System:**

   Run the following command in the master node to start all nodes and the web client:

   ```bash
   python3 launcher.py
   ```

   This will:
   - Initialize the master, crawler, and indexer nodes.
   - Launch the web-based client interface.

## Using the Web Interface

Once the system is running, open the web interface in your browser. You can perform two main actions from this interface:

### 1. Start a Web Crawl

To crawl a new website:

- In the input box, type:

  ```
  url http://example.com
  ```

- Click the **Enter Command** button.

- A message will appear:
  ```
  The URL sent successfully.
  ```

This command sends the URL to the crawler system for processing and indexing.

### 2. Search for a Keyword

To search for a keyword in the crawled data:

- In the input box, type:

  ```
  search your_keyword
  ```

- Click the **Enter Command** button.

- The interface will display the search results retrieved from the indexer.


## Notes

- Ensure your AWS credentials and resources (EC2, SQS, RDS) are properly configured before launching.


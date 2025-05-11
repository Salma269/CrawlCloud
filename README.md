# Crawl Cloud  
A Cloud-Based Web Crawler

This project is a distributed web crawler and search engine built on AWS. Each node in the system runs on a separate AWS EC2 instance. The system includes a user-friendly web interface for starting crawl jobs and performing keyword searches.

---

## How to Start the System

1. **Launch the System**

   Run the following command on the master node to initialize all components and the web interface:

   ```bash
   python3 launcher.py
   ```

   This will:
   - Start the master, crawler, and indexer nodes.
   - Launch the web client on port 5000 (e.g., `http://<public-server-ip>:5000`).

---

## Using the Web Interface

Once the system is up, open the client interface in your browser. The dashboard provides two main functions:

### 1. Add URL to Crawl

To initiate a crawl:

- Enter the full URL in the **Enter URL** field (e.g., `https://example.com`).
- Enter the **Allowed Domain** to restrict the crawler scope (e.g., `example.com`).
- Set the desired **Crawl Depth** (e.g., `1`).
- Click the **Start Crawl** button.

A message like this will confirm the task was added:

```
URL task added successfully.
```

### 2. Search the Index

To search the indexed content:

- Enter a keyword or phrase in the **Search the Index** field.
- Click the **Search** button.

Matching results from previously crawled data will be displayed below.

---

## Notes

- Ensure your AWS credentials and services (EC2, SQS, RDS) are correctly set up before launching the system.
- The system is accessible via a browser on the server IP and port `5000`.

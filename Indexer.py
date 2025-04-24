import json
from queue import Queue
from collections import defaultdict
from typing import Dict, List, Set
import os

class BasicIndexerNode:
    def __init__(self, use_file_storage: bool = False, index_file: str = "index.json"):
        """
        Initialize the indexer node.

        Args:
            use_file_storage: If True, persist index to disk
            index_file: File to store/load the index
        """
        self.index: Dict[str, Set[str]] = defaultdict(set)  # keyword -> set of URLs
        self.crawler_queue = Queue()  # Simple queue for incoming crawler data
        self.use_file_storage = use_file_storage
        self.index_file = index_file

        if use_file_storage and os.path.exists(index_file):
            self.load_index()

    def ingest_from_crawler(self, url: str, text: str):
        """
        Ingest data from a crawler node.

        Args:
            url: The URL of the crawled page
            text: The text content of the page
        """
        # Simple tokenization - split by whitespace and remove punctuation
        words = [word.strip('.,!?()[]{}"\'') for word in text.lower().split()]

        # Add to index
        for word in words:
            if word:  # skip empty strings
                self.index[word].add(url)

        # If using file storage, save after each update
        if self.use_file_storage:
            self.save_index()

    def search(self, keyword: str) -> List[str]:
        """
        Search for a keyword in the index (exact match).

        Args:
            keyword: The keyword to search for

        Returns:
            List of URLs containing the keyword
        """
        normalized_keyword = keyword.lower()
        return list(self.index.get(normalized_keyword, set()))

    def save_index(self):
        """Save the index to a file."""
        # Convert sets to lists for JSON serialization
        serializable_index = {k: list(v) for k, v in self.index.items()}
        with open(self.index_file, 'w') as f:
            json.dump(serializable_index, f)

    def load_index(self):
        """Load the index from a file."""
        try:
            with open(self.index_file, 'r') as f:
                loaded_index = json.load(f)
            # Convert lists back to sets
            self.index = defaultdict(set, {k: set(v) for k, v in loaded_index.items()})
        except (FileNotFoundError, json.JSONDecodeError):
            self.index = defaultdict(set)

    def process_crawler_queue(self):
        """Process all items in the crawler queue."""
        while not self.crawler_queue.empty():
            url, text = self.crawler_queue.get()
            self.ingest_from_crawler(url, text)
            self.crawler_queue.task_done()

    def add_to_queue(self, url: str, text: str):
        """Add crawled data to the processing queue."""
        self.crawler_queue.put((url, text))


# Example usage
if __name__ == "__main__":
    # Initialize indexer
    indexer = BasicIndexerNode(use_file_storage=True)

    # Simulate data coming from crawler nodes
    crawler_data = [
        ("http://example.com/page1", "This is a sample page about Python programming."),
        ("http://example.com/page2", "Programming in Python is fun and easy."),
        ("http://example.com/page3", "Machine learning with Python is popular."),
    ]

    # Ingest data (could also use add_to_queue and process_crawler_queue)
    for url, text in crawler_data:
        indexer.ingest_from_crawler(url, text)

    # Perform searches
    print("Results for 'python':", indexer.search("python"))
    print("Results for 'programming':", indexer.search("programming"))
    print("Results for 'nonexistent':", indexer.search("nonexistent"))
    print("Results for 'sample':", indexer.search("sample"))

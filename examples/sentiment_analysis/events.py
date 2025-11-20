from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path

from volnux import EventBase
import json
from typing import List, Tuple, Dict, Any, Union
from textblob import TextBlob
import matplotlib.pyplot as plt
from volnux.base import RetryPolicy
from .custom_exceptions import DataFileError, EmailNotificationError, JsonDataError
import yagmail
import traceback

# Get the directory of the current module
module_dir = Path(__file__).parent
# Construct the path to posts_comments.json
comments_file_path = module_dir / "posts_comments.json"


class LoadData(EventBase):
    """
    Event that loads data from a file and returns the data to other events in the pipeline.
    """

    executor = ThreadPoolExecutor
    retry_policy = RetryPolicy(
        max_attempts=3,
        backoff_factor=0.005,
        max_backoff=100,
        retry_on_exceptions=[JsonDataError, DataFileError],
    )

    def process(
        self, file_path: Path = comments_file_path
    ) -> Union[Tuple[bool, List[Dict[str, Any]]], Tuple[bool, str]]:
        """
        Reads posts data from a local JSON file.

        Args:
            file_path (str): Path to the JSON file containing post data.

        Returns:
            Tuple[bool, List[Dict[str, Any]]]: A success flag and the list of post dictionaries.
        """
        if not file_path.exists():
            self.goto(
                descriptor=2,
                reason="Failed to read data file, no file was provided.",
                result_status=False,
                result=None,
            )
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                data: List[Dict[str, Any]] = json.load(file)
        except json.JSONDecodeError:
            error_trace = traceback.format_exc()
            return False, error_trace

        return True, data


class ProcessData(EventBase):
    """
    Class that processes the data returned by LoadData.

    This class is responsible for filtering out posts to only include the postId and body.
    """

    executor = ProcessPoolExecutor

    def process(self) -> Tuple[bool, List[Dict]]:
        """
        Filters posts to include only postId and body, and returns them.

        Returns:
            Tuple[bool, List[Dict]]: A success flag and the filtered list of post dictionaries.
        """
        posts = self.previous_result[0].content  # List[Dict]
        filtered_posts_with_bodies = [
            {"postId": item["postId"], "body": item["body"]} for item in posts
        ]
        return True, filtered_posts_with_bodies


class AnalyzeSentiment(EventBase):
    """
    Event that performs sentiment analysis on posts using TextBlob.

    This event is a container for other events that analyze sentiment.
    """

    executor = ProcessPoolExecutor

    def process(self) -> Tuple[bool, Dict[int, Dict[str, int]]]:
        """
        Performs sentiment analysis on post bodies using TextBlob and groups results by postId.

        Returns:
            Tuple[bool, Dict[int, Dict[str, int]]]: A success flag and a dictionary with postId as keys
            and counts of positive and negative sentiments as values.
        """
        posts = self.previous_result[0].content  # List[Dict] with postId and body
        sentiment_results = {
            post["postId"]: {"positive": 0, "negative": 0} for post in posts
        }

        for post in posts:
            post_id = post["postId"]
            body = post["body"]
            analysis = TextBlob(body)
            polarity = analysis.sentiment.polarity
            # Consider polarity > 0 as positive, <= 0 as negative
            sentiment_results[post_id]["positive" if polarity > 0 else "negative"] += 1

        return True, sentiment_results


class PlotStackedSentiments(EventBase):
    """
    Event that creates a stacked bar chart of positive and negative sentiment counts for each postId.
    """

    executor = ProcessPoolExecutor

    def process(self) -> Tuple[bool, None]:
        """
        Creates a stacked bar chart of positive and negative sentiment counts for each postId.

        Returns:
            Tuple[bool, None]: A success flag and None (plot is displayed).
        """
        sentiment_data = self.previous_result[0].content  # Dict[int, Dict[str, int]]

        post_ids = list(sentiment_data.keys())
        positive_counts = [sentiment_data[pid]["positive"] for pid in post_ids]
        negative_counts = [sentiment_data[pid]["negative"] for pid in post_ids]

        fig, ax = plt.subplots(figsize=(12, 6))

        ax.bar(post_ids, positive_counts, label="Positive", color="green")
        ax.bar(
            post_ids,
            negative_counts,
            bottom=positive_counts,
            label="Negative",
            color="red",
        )

        ax.set_xlabel("Post ID")
        ax.set_ylabel("Sentiment Count")
        ax.set_title("Stacked Sentiment Counts by Post ID")
        ax.set_xticks(post_ids)
        ax.set_xticklabels(post_ids, rotation=45)
        ax.legend()

        plt.tight_layout()
        plt.show()

        return True, None


class NotifyDataFileMissing(EventBase):
    """
    Event that sends an email notification when the data file is missing.
    """

    executor = ThreadPoolExecutor
    retry_policy = RetryPolicy(
        max_attempts=3,
        backoff_factor=0.005,
        max_backoff=100,
        retry_on_exceptions=[EmailNotificationError],
    )

    def process(
        self,
        sender_email: str,
        sender_password: str,
        recipient_email: str,
    ) -> Tuple[bool, str]:
        """
        Sends an email notification if the data file is not found.

        Args:
            sender_email (str): The email address to send the message from.
            sender_password (str): The email password or app-specific password.
            recipient_email (str): The recipient email address.

        Returns:
            Tuple[bool, str]: A flag indicating if the email was sent, and a message.
        """
        subject = f"Data file not found"
        body = (
            "The data file required for Sentiment Analysis could not be found.\n\n"
            "Please provide a correct data file and run the pipeline again.\n\n"
            "Thank you."
        )

        try:
            yag = yagmail.SMTP(user=sender_email, password=sender_password)
            yag.send(to=recipient_email, subject=subject, contents=body)
        except Exception as e:
            raise EmailNotificationError(f"Failed to send notification email: {e}")

        return True, "Data file found. No email sent."


class DataFileJsonError(EventBase):
    """
    Event that handles a JSONDecodeError when reading the data file.

    This event is used to handle a JSONDecodeError when reading the data file.
    The event will retry the operation up to 3 times with a short backoff period.
    """

    executor = ThreadPoolExecutor
    retry_policy = RetryPolicy(
        max_attempts=3,
        backoff_factor=0.005,
        max_backoff=100,
        retry_on_exceptions=[EmailNotificationError],
    )

    def process(
        self,
        sender_email: str,
        sender_password: str,
        recipient_email: str,
    ) -> Tuple[bool, str]:
        """
        Sends an email notification if the data file is not found.

        Args:
            file_path (Path): The path to the expected data file.
            sender_email (str): The email address to send the message from.
            sender_password (str): The email password or app-specific password.
            recipient_email (str): The recipient email address.

        Returns:
            Tuple[bool, str]: A flag indicating if the email was sent, and a message.
        """
        error_trace = self.previous_result[0].content
        subject = f"Json Data Error in  File"
        body = (
            "The data file contains invalid json format or json errors.\n\n"
            "Please check the error trace below and correct the file and run the pipeline again.\n\n"
            f"Error Trace: {error_trace}\n\n"
            "Thank you."
        )
        try:
            yag = yagmail.SMTP(user=sender_email, password=sender_password)
            yag.send(to=recipient_email, subject=subject, contents=body)
        except Exception as e:
            raise EmailNotificationError(f"Failed to send notification email: {e}")

        return True, "Data file found. No email sent."

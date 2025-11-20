import os

from volnux.pipeline import Pipeline
from volnux.fields import FileInputDataField, InputDataField


class SentimentAnalysisPipeline(Pipeline):
    """
    A pipeline for running sentiment analysis on data.


    Attributes:
        posts_comments_file (FileInputDataField): The path to the JSON file containing post comments.
        sender_email (InputDataField): The email address to send the email notification.
        recipient_email (InputDataField): The recipient email address for the email notification.
        sender_password (InputDataField): The email password or app-specific password.


    """

    posts_comments_file = FileInputDataField(
        path="posts_comments.json",
        required=False,
        help_text="The path to the JSON file containing post comments.",
    )
    sender_email = InputDataField(
        required=False,
        default_factory=lambda: os.getenv("SENDER_EMAIL"),
        help_text="The email address to send the email notification.",
    )
    sender_password = InputDataField(
        required=False,
        default_factory=lambda: os.getenv("SENDER_PASSWORD"),
        help_text="The email password or app-specific password.",
    )
    recipient_email = InputDataField(
        required=False,
        default_factory=lambda: os.getenv("RECIPIENT_EMAIL"),
        help_text="The recipient email address for the email notification.",
    )

    class Meta:
        pointy = "LoadData ( 1 |-> ProcessData |-> AnalyzeSentiment |-> PlotStackedSentiments, 0 |-> DataFileJsonError, 2 |-> NotifyDataFileMissing )"
        # Path to the pointy file, if u choose to execute with it
        # file = "sentiment_analysis/pointy_file.pty"

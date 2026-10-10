from .sentiment_analysis_pipeline import SentimentAnalysisPipeline
from dotenv import load_dotenv

load_dotenv()


pipeline = SentimentAnalysisPipeline()
pipeline.draw_ascii_graph()  # visualize the ASCII graph
pipeline.draw_graphviz_image()  # visualize the graph using graphviz
pipeline.start()

import logging
import os

# Create logs folder if it doesn't exist
os.makedirs("logs", exist_ok=True)

# Configure global logging settings for the entire project
logging.basicConfig(
    level=logging.INFO,  # Capture INFO, WARNING, ERROR, CRITICAL
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    # Send logs to both file and terminal
    handlers=[
        logging.FileHandler("logs/pipeline.log"),  # Save logs to file
        logging.StreamHandler(),  # Print logs to terminal during runtime
    ],
)

# Create a logger instance for this module
# __name__ helps identify which file produced the log
logger = logging.getLogger(__name__)

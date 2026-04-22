# run_pipeline.py
import sys
from pipelines.ingest.csv_to_bronze import main as ingest
from pipelines.transform.bronze_to_silver import main as to_silver
from pipelines.transform.silver_to_gold import main as to_gold
from utils.logger import get_logger

logger = get_logger(__name__)

STEPS = [
    ("Bronze → CSV to PostgreSQL", ingest),
    ("Silver → Clean & Validate",  to_silver),
    ("Gold   → Aggregate",         to_gold),
]

def run():
    logger.info("Pipeline started")

    for step_name, step_func in STEPS:
        logger.info("Running: %s", step_name)
        try:
            step_func()
            logger.info("Done: %s", step_name)
        except Exception as e:
            logger.error("FAILED: %s | %s", step_name, e)
            sys.exit(1)  # يوقف كل شيء فوراً

    logger.info("Pipeline completed successfully")

if __name__ == "__main__":
    run()
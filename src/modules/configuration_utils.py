import os
import sys
import json
import logging

logger = logging.getLogger(__name__)

class ConfigurationUtils:

    def _fix_location(filepath: str) -> str:
        if not filepath.startswith("/dbfs"):
            return f"/dbfs{filepath}"
        else:
            return filepath

    def read_config(filepath: str) -> dict:
        filepath = ConfigurationUtils._fix_location(filepath)
        logger.info(f"Trying to read file: {filepath}...")

        with open(filepath, "r") as f:
            data = json.load(f)
            logger.info("Read completed.")

        return data

    def write_config(filepath: str, dictionary: dict) -> str:
        logger.info("Writing dict into a json file...")
        filepath = ConfigurationUtils._fix_location(filepath)

        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w") as outfile:
            json.dump(dictionary, outfile)
            logger.info("Write completed.")

        return filepath

import logging

def logging_config(level=logging.INFO):
    logging.basicConfig(level=level,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
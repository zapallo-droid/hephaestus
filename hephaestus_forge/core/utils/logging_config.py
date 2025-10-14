# --General Libraries-- #
import logging

# --Core Functions-- #
def logging_config(level=logging.INFO):
    logging.basicConfig(level=level,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# --Aux Classes-- #
class StatsLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()

        self.warnings = []
        self.errors = []

    def emit(self, record):
        if record.levelno == logging.WARNING:
            self.warnings.append(self.format(record))
        elif record.levelno >= logging.ERROR:
            self.errors.append(self.format(record))

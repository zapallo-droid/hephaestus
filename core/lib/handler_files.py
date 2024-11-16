import requests
import logging
from typing import Any, Optional, Union

class FileCall:
    def __init__(self, url: str):
        self.url = url
        self.response = None

    def get_data(self) -> Union[dict[str, Any], list[dict[str, Any]]]:
        try:
            self.response = requests.get(self.url)
            self.response.raise_for_status()
            try:
                return self.response.content
            except ValueError as e:
                logging.error(f'Decoding failed from {self.url}: {str(e)}')
                return {}
        except requests.exceptions.RequestException as e:
            logging.error(f'Error {str(e)} raised when fetching data form {self.url}')
            return {}



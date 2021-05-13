import logging

class Logging():

    def __init__(self, user_messages = False) -> None:
        logging.basicConfig(level=logging.INFO)
        self._user_messages = user_messages
    
    def warning(self, message) -> None:
        logging.warning('Warning: ' + message)

    def error(self, message) -> None:
        logging.error('Error: ' + message)

    def exception(self, message) -> None:
        logging.exception('Exception: ' + message)

    def user_message(self, message) -> None:
        if(self._user_messages):
            logging.info('Information: ' + message)
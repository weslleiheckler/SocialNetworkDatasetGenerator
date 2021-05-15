import logging

class Logging():

    def __init__(self, user_messages = False, timer_messages = False) -> None:
        logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
        self._user_messages = user_messages
        self._timer_messages = timer_messages
    
    def warning(self, message) -> None:
        logging.warning(message)

    def error(self, message) -> None:
        logging.error(message)

    def exception(self, message) -> None:
        logging.exception(message)

    def user_message(self, message) -> None:
        if(self._user_messages):
            logging.info(message)

    def timer_message(self, message) -> None:
        if(self._timer_messages):
            logging.info(message)
import logging
from authentication.AuthenticatorInterface import AuthenticatorInterface
import praw
import configparser, os

class RedditAuthenticator(AuthenticatorInterface):

    def __init__(self, logging) -> None:
        super().__init__()
        self._client_id = None
        self._client_secret = None
        self._user_agent = None
        self._username = None
        self._password = None
        self._api = None
        self._logging = logging

    @property
    def api(self):
        return self._api

    def connect(self) -> None:
        self.import_config()

    def import_config(self) -> None:
        # check whether the config file exists
        if not os.path.exists('config/config.ini'): 
            self._logging.error('Configuration file not found. Verify whether the config.ini file is in the config subdirectory.')
        else:
            # read the connection configurations in the ini file
            config = configparser.ConfigParser()
            config.read('config/config.ini')

            # verify the Reddit section
            if 'Reddit' in config:
                try:
                    self._client_id = config['Reddit']['client_id']
                    self._client_secret = config['Reddit']['client_secret']
                    self._user_agent = config['Reddit']['user_agent']
                    self._username = config['Reddit']['username']
                    self._password = config['Reddit']['password']

                    self.authenticate()
                except:
                    self._logging.error('Reddit keys not found or incorrect. Verify the Reddit section in the config file.')               
            else:
                self._logging.error('Reddit section not found. Verify the Reddit section in the config file.')

    def authenticate(self) -> None:
        api = praw.Reddit(client_id = self._client_id,
                            client_secret = self._client_secret,
                            user_agent = self._user_agent, 
                            username = self._username,
                            password = self._password)
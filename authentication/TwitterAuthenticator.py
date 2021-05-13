from authentication.AuthenticatorInterface import AuthenticatorInterface
import tweepy as tw
import configparser, os

class TwitterAuthenticator(AuthenticatorInterface):
    
    def __init__(self, log) -> None:
        super().__init__()
        self._api_key = None
        self._api_secret_key = None
        self._access_token = None
        self._access_token_secret = None
        self._api = None
        self._log = log

    @property
    def api(self):
        return self._api

    def connect(self) -> None:
        self.import_config()
    
    def import_config(self) -> None:
        
        # check whether the config file exists
        if not os.path.exists('config/config.ini'): 
            self._log.error('Configuration file not found. Verify whether the config.ini file is in the config subdirectory.')
        else:
            # read the connection configurations in the ini file
            config = configparser.ConfigParser()
            config.read('config/config.ini')

            # verify the Twitter section
            if 'Twitter' in config:
                try:
                    self._api_key = config['Twitter']['api_key']
                    self._api_secret_key = config['Twitter']['api_secret_key']
                    self._access_token = config['Twitter']['access_token']
                    self._access_token_secret = config['Twitter']['access_token_secret']

                    self.authenticate()
                except:
                    self._log.error('Twitter keys not found or incorrect. Verify the Twitter section in the config file.')               
            else:
                self._log.error('Twitter section not found. Verify the Twitter section in the config file.')               

    def authenticate(self) -> None:
        auth = tw.OAuthHandler(self._api_key, self._api_secret_key)
        auth.set_access_token(self._access_token, self._access_token_secret)
        self._api = tw.API(auth, wait_on_rate_limit=True)

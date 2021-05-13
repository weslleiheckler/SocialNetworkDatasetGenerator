from authentication import TwitterAuthenticator as tt
from authentication import RedditAuthenticator as rd
from query import FilterConfiguration as fc
from util import Logging as log

class Main():

    def main():

        # create a logger
        logging = log.Logging(True)

        # Twitter connection
        # twitter_conn = tt.TwitterAuthenticator(logging)
        # twitter_conn.connect()

        # Reddit connection
        # reddit_conn = rd.RedditAuthenticator(logging)
        # reddit_conn.connect()

        filter_conf = fc.FilterConfiguration(logging)
        filter_conf.import_filters()

    if __name__ == "__main__":
        main()
from authentication import TwitterAuthenticator as tt
from authentication import RedditAuthenticator as rd
from util import Logging as log

class Main():

    def main():

        # create a logger
        logging = log.Logging(True)

        # Twitter connection
        # twitter_conn = tt.TwitterAuthenticator(logging)
        # twitter_conn.connect()

        # Reddit connection
        reddit_conn = rd.RedditAuthenticator(logging)
        reddit_conn.connect()

    if __name__ == "__main__":
        main()
from authentication import TwitterAuthenticator as tt
from authentication import RedditAuthenticator as rd
from query import QueryTweets as qt
from query import FilterConfiguration as fc
from util import Logging as log

class Main():

    def main():

        # create a logger
        logging = log.Logging(user_messages=True,timer_messages=True)

        # Twitter connection
        twitter_conn = tt.TwitterAuthenticator(logging)
        twitter_conn.connect()

        # Reddit connection
        # reddit_conn = rd.RedditAuthenticator(logging)
        # reddit_conn.connect()

        filter_conf = fc.FilterConfiguration(logging)
        filter_conf.import_filters()

        tt_query = qt.QueryTweets(twitter_conn, filter_conf.list_filters, True, logging)
        tt_query.query_manager()

    if __name__ == "__main__":
        main()
from authentication import TwitterAuthenticator as tt
from authentication import RedditAuthenticator as rd
from query import QueryTweets as qt
from query import QueryRedditPosts as qr
from query import FilterConfiguration as fc
from preprocessing import PreprocessingConfiguration as pc
from preprocessing import Preprocessing as pp
from util import Logging as log

class Main():

    def main():

        # create a logger
        logging = log.Logging(user_messages=False,timer_messages=True)

        # Twitter connection
        twitter_conn = tt.TwitterAuthenticator(logging)
        twitter_conn.connect()

        # Reddit connection
        reddit_conn = rd.RedditAuthenticator(logging)
        reddit_conn.connect()

        filter_conf = fc.FilterConfiguration(logging)
        filter_conf.import_filters()

        tt_query = qt.QueryTweets(twitter_conn, filter_conf.list_filters, True, logging)
        tt_query.query_manager()

        rt_query = qr.QueryRedditPosts(reddit_conn, filter_conf.list_filters, True, logging)
        rt_query.query_manager()

        pp_config = pc.PreprocessingConfiguration(logging)
        pp_config.config()
        preprocessing = pp.Preprocessing(pp_config, tt_query.dict_df_posts, logging)
        preprocessing.preprocessing()

    if __name__ == "__main__":
        main()
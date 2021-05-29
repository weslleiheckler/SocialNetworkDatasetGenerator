from authentication import TwitterAuthenticator as tt
from authentication import RedditAuthenticator as rd
from query import QueryTweets as qt
from query import QueryTweetsV2 as qt2
from query import QueryRedditPosts as qr
from query import FilterConfiguration as fc
from preprocessing import PreprocessingConfiguration as pc
from preprocessing import Preprocessing as pp
from save import SaveConfiguration as sc
from save import Save as sv
from util import Logging as log

class Main():

    def main():

        # create a logger
        logging = log.Logging(user_messages=False,timer_messages=True)

        # filter configurations
        filter_conf = fc.FilterConfiguration(logging)
        filter_conf.import_filters()

        # preprocessing configurations
        pp_config = pc.PreprocessingConfiguration(logging)
        pp_config.config()

        # save configurations
        save_config = sc.SaveConfiguration(logging)
        save_config.config()

        # Twitter connection
        twitter_conn = tt.TwitterAuthenticator(logging)
        twitter_conn.connect()

        # Twitter query with tweepy
        tt_query_tweepy = qt.QueryTweets(twitter_conn, filter_conf.list_filters, True, logging)
        tt_query_tweepy.query_manager()        

        # Twitter preprocessing
        preprocessing = pp.Preprocessing(pp_config, tt_query_tweepy.dict_df_posts, logging)
        preprocessing.preprocessing()

        # Twitter save
        save = sv.Save(save_config, tt_query_tweepy.dict_df_posts, logging)
        save.save()

        # Twitter query with twint
        tt_query_twint = qt2.QueryTweetsV2(twitter_conn, filter_conf.list_filters, True, logging)
        tt_query_twint.query_manager()

        # Twitter preprocessing
        preprocessing = pp.Preprocessing(pp_config, tt_query_twint.dict_df_posts, logging)
        preprocessing.preprocessing()

        # Twitter save
        save = sv.Save(save_config, tt_query_twint.dict_df_posts, logging)
        save.save()

        # Reddit connection
        reddit_conn = rd.RedditAuthenticator(logging)
        reddit_conn.connect()

        # Reddit query
        rt_query = qr.QueryRedditPosts(reddit_conn, filter_conf.list_filters, True, logging)
        rt_query.query_manager()

        # Reddit preprocessing
        preprocessing = pp.Preprocessing(pp_config, rt_query.dict_df_posts, logging)
        preprocessing.preprocessing()

        # Reddit save
        save = sv.Save(save_config, rt_query.dict_df_posts, logging)
        save.save()

    if __name__ == "__main__":
        main()
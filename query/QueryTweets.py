from query.QueryPostsInterface import QueryPostsInterface
from multiprocessing import Process, Queue
import time
import datetime as dt
import tweepy as tw
import pandas as pd

class QueryTweets(QueryPostsInterface):

    def __init__(self, authenticator, list_filters, parallelize, log) -> None:
        super().__init__()
        self._authenticator = authenticator
        self._list_filters = list_filters
        self._parallelize = parallelize
        self._log = log
        self._dict_df_posts = {}

    @property
    def dict_df_posts(self):
        return self._dict_df_posts

    def set_dict_df_posts(self, key, df) -> None:
        if(len(df) > 0):
            now = dt.datetime.now()
            dt_string = now.strftime("%d_%m_%Y_%H_%M_%S")
            k = 'twitter_tweepy_' + key + '_' + dt_string
            self._dict_df_posts[k] = df
    
    def query(self, twitter_filter) -> pd.DataFrame:
        df = pd.DataFrame()

        try:
            # query tweets according to the filters
            tweets = tw.Cursor(self._authenticator.api.search, **twitter_filter.query_params).items(twitter_filter.items)

            # create a pandas dataframe with specific columns
            twitter_data = [[tweet.id,   
                             tweet.created_at,
                             tweet.text,
                             tweet.in_reply_to_status_id,
                             tweet.in_reply_to_user_id,
                             tweet.in_reply_to_screen_name,
                             tweet.geo,
                             tweet.coordinates,
                             tweet.place,
                             tweet.is_quote_status,
                             tweet.retweet_count,
                             tweet.favorite_count,
                             tweet.lang,
                             tweet.user.id,
                             tweet.user.name,
                             tweet.user.screen_name, 
                             tweet.user.location,
                             tweet.user.followers_count,
                             tweet.user.friends_count,
                             tweet.user.listed_count,
                             tweet.user.created_at,
                             tweet.user.favourites_count,
                             tweet.user.time_zone,
                             tweet.user.verified,
                             tweet.user.lang,
                             tweet.user.profile_background_image_url_https,
                             tweet.user.profile_image_url_https,
                             tweet.user.default_profile,
                             tweet.user.default_profile_image] 
                             for tweet in tweets]

            df = pd.DataFrame(data=twitter_data, 
                                columns=['id',
                                         'created_at',
                                         'text',
                                         'in_reply_to_status_id',
                                         'in_reply_to_user_id',
                                         'in_reply_to_screen_name',
                                         'geo',
                                         'coordinates',
                                         'place',
                                         'is_quote_status',
                                         'retweet_count',
                                         'favorite_count',
                                         'lang',
                                         'user_id',
                                         'user_name',
                                         'user_screen_name', 
                                         'user_location',
                                         'user_followers_count',
                                         'user_friends_count',
                                         'user_listed_count',
                                         'user_created_at',
                                         'user_favourites_count',
                                         'user_time_zone',
                                         'user_verified',
                                         'user_lang',
                                         'user_profile_background_image_url_https',
                                         'user_profile_image_url_https',
                                         'user_default_profile',
                                         'user_default_profile_image'])

            # create a column with the value from the 'label' filter parameter
            if(twitter_filter.label is not None):
                df['label'] = twitter_filter.label
        except:
            self._log.exception('Fail to query tweets.')

        # return the dataframe
        return df
        
    def query_par(self, twitter_filter, queue) -> None:
        # call query function to query tweets and create a dataframe
        df = self.query(twitter_filter)

        # put the pandas dataframe in the queue 
        queue.put(df)

    def query_timeline(self, user, twitter_filter) -> pd.DataFrame:
        df = pd.DataFrame()
        
        try:
            # query tweets according to the filters
            if(twitter_filter.items > 0):
                tweets = tw.Cursor(self._authenticator.api.user_timeline, user_id = user, **twitter_filter.query_params).items(twitter_filter.items)
            else:
                tweets = tw.Cursor(self._authenticator.api.user_timeline, user_id = user, **twitter_filter.query_params).items()
            
            # create a pandas dataframe with specific columns
            twitter_data = [[tweet.id,
                             tweet.created_at,
                             tweet.text,
                             tweet.in_reply_to_status_id,
                             tweet.in_reply_to_user_id,
                             tweet.in_reply_to_screen_name,
                             tweet.geo,
                             tweet.coordinates,
                             tweet.place,
                             tweet.is_quote_status,
                             tweet.retweet_count,
                             tweet.favorite_count,
                             tweet.lang,
                             tweet.user.id,
                             tweet.user.name,
                             tweet.user.screen_name,
                             tweet.user.location,
                             tweet.user.followers_count,
                             tweet.user.friends_count,
                             tweet.user.listed_count,
                             tweet.user.created_at,
                             tweet.user.favourites_count,
                             tweet.user.time_zone,
                             tweet.user.verified,
                             tweet.user.lang,
                             tweet.user.profile_background_image_url_https,
                             tweet.user.profile_image_url_https,
                             tweet.user.default_profile,
                             tweet.user.default_profile_image]  
                             for tweet in tweets]

            df = pd.DataFrame(data=twitter_data, 
                                columns=['id',
                                         'created_at',
                                         'text',
                                         'in_reply_to_status_id',
                                         'in_reply_to_user_id',
                                         'in_reply_to_screen_name',
                                         'geo',
                                         'coordinates',
                                         'place',
                                         'is_quote_status',
                                         'retweet_count',
                                         'favorite_count',
                                         'lang',
                                         'user_id',
                                         'user_name',
                                         'user_screen_name',
                                         'user_location',
                                         'user_followers_count',
                                         'user_friends_count',
                                         'user_listed_count',
                                         'user_created_at',
                                         'user_favourites_count',
                                         'user_time_zone',
                                         'user_verified',
                                         'user_lang',
                                         'user_profile_background_image_url_https',
                                         'user_profile_image_url_https',
                                         'user_default_profile',
                                         'user_default_profile_image'])

            # create a column with the value from the 'label' filter parameter
            if(twitter_filter.label is not None):
                df['label'] = twitter_filter.label
        except:
            self._log.exception('Fail to query timeline from users.')

        # return the dataframe
        return df

    def query_timeline_par(self, user, twitter_filter, queue) -> None:
        # call query_timeline function to query tweets and create a dataframe
        df = self.query_timeline(user, twitter_filter)

        # put the pandas dataframe in the queue 
        queue.put(df)

    def query_favorites(self, user, twitter_filter) -> pd.DataFrame:
        df = pd.DataFrame()

        try:
            # query tweets according to the filters
            if(twitter_filter.items > 0):
                tweets = tw.Cursor(self._authenticator.api.favorites, user_id = user, **twitter_filter.query_params).items(twitter_filter.items)
            else:
                tweets = tw.Cursor(self._authenticator.api.favorites, user_id = user, **twitter_filter.query_params).items()
            
            # create a pandas dataframe with specific columns
            twitter_data = [[tweet.id,
                             tweet.created_at,
                             tweet.text,
                             tweet.in_reply_to_status_id,
                             tweet.in_reply_to_user_id,
                             tweet.in_reply_to_screen_name,
                             tweet.geo,
                             tweet.coordinates,
                             tweet.place,
                             tweet.is_quote_status,
                             tweet.retweet_count,
                             tweet.favorite_count,
                             tweet.lang,
                             tweet.user.id,
                             tweet.user.name,
                             tweet.user.screen_name,
                             tweet.user.location,
                             tweet.user.followers_count,
                             tweet.user.friends_count,
                             tweet.user.listed_count,
                             tweet.user.created_at,
                             tweet.user.favourites_count,
                             tweet.user.time_zone,
                             tweet.user.verified,
                             tweet.user.lang,
                             tweet.user.profile_background_image_url_https,
                             tweet.user.profile_image_url_https,
                             tweet.user.default_profile,
                             tweet.user.default_profile_image] 
                             for tweet in tweets]
                             
            df = pd.DataFrame(data=twitter_data, 
                              columns=['id',
                                       'created_at',
                                       'text',
                                       'in_reply_to_status_id',
                                       'in_reply_to_user_id',
                                       'in_reply_to_screen_name',
                                       'geo',
                                       'coordinates',
                                       'place',
                                       'is_quote_status',
                                       'retweet_count',
                                       'favorite_count',
                                       'lang',
                                       'user_id',
                                       'user_name',
                                       'user_screen_name',
                                       'user_location',
                                       'user_followers_count',
                                       'user_friends_count',
                                       'user_listed_count',
                                       'user_created_at',
                                       'user_favourites_count',
                                       'user_time_zone',
                                       'user_verified',
                                       'user_lang',
                                       'user_profile_background_image_url_https',
                                       'user_profile_image_url_https',
                                       'user_default_profile',
                                       'user_default_profile_image'])

            # create a column with the value from the 'label' filter parameter
            if(twitter_filter.label is not None):
                df['label'] = twitter_filter.label
        except:
            self._log.exception('Fail to query favorites from users.')

        return df

    def query_favorites_par(self, user, twitter_filter, queue) -> None:
        # call query_favorites function to query tweets and create a dataframe
        df = self.query_favorites(user, twitter_filter)

        # put the pandas dataframe in the queue 
        queue.put(df)

    def query_manager(self) -> None:
        self._log.timer_message('Collecting Twitter data with the tweepy package.')
        
        # select only the Twitter filters
        list_twitter_filters = list(filter(lambda x: (x.key == 'Twitter' and x.library == 'tweepy'), self._list_filters))

        # both methods perform the same task using a parallel or sequential strategy
        if(self._parallelize):
            # query tweets parallelized
            self.query_parallel(list_twitter_filters)
        else:
            # query tweets sequentially
            self.query_sequential(list_twitter_filters)

    def query_sequential(self, list_filters) -> None:
        start_time_seq = time.time()

        # separate filters by type
        search_filters = list(filter(lambda x: (x.filter_type == 'search'), list_filters))
        user_timeline_filters = list(filter(lambda x: (x.filter_type == 'user_timeline'), list_filters))
        favorites_filters = list(filter(lambda x: (x.filter_type == 'favorites'), list_filters))

        # for each filter, create a query of tweets 
        # concatenate all dataframes of search information
        df_search = pd.DataFrame() # dataframe to store all information from this filter type
        for sf in search_filters:
            df_filter = self.query(sf)
            df_search = pd.concat([df_search, df_filter])
        
        self.set_dict_df_posts('search', df_search)
        self._log.user_message('Tweets query finished.')

        # for each user id from each filter, create a query of tweets
        # concatenate all dataframes of user_timeline information
        df_user_timeline = pd.DataFrame() # dataframe to store all information from this filter type
        for utf in user_timeline_filters:
            for user in utf.users:
                df_filter = self.query_timeline(user, utf)
                df_user_timeline = pd.concat([df_user_timeline, df_filter])

        self.set_dict_df_posts('user_timeline', df_user_timeline)
        self._log.user_message('Timeline query finished.')

        # for each user id from each filter, create a query of tweets
        # concatenate all dataframes of favorites information
        df_favorites = pd.DataFrame()
        for fav in favorites_filters:
            for user in fav.users:
                df_filter = self.query_favorites(user, fav)
                df_favorites = pd.concat([df_favorites, df_filter])

        self.set_dict_df_posts('favorites', df_favorites)
        self._log.user_message('Favorites query finished.')

        final_time_seq = time.time() - start_time_seq
        self._log.timer_message('Sequential Query Time: ' + str(final_time_seq) + ' seconds.')

    def query_parallel(self, list_filters) -> None:
        start_time_par = time.time()

        # separate filters by type
        search_filters = list(filter(lambda x: (x.filter_type == 'search'), list_filters))
        user_timeline_filters = list(filter(lambda x: (x.filter_type == 'user_timeline'), list_filters))
        favorites_filters = list(filter(lambda x: (x.filter_type == 'favorites'), list_filters))
        
        # configure queues
        queue_search = Queue()
        queue_user_timeline = Queue()
        queue_favorites = Queue()

        # for each filter, create a parallelized query of tweets
        processes_search = [Process(target=self.query_par, args=(sf, queue_search)) for sf in search_filters]

        # for each user id from each filter, create a parallelized query of tweets
        processes_user_timeline = []
        for utf in user_timeline_filters:
            processes_user_timeline.extend([Process(target=self.query_timeline_par, args=(user, utf, queue_user_timeline)) for user in utf.users])

        # for each user id from each filter, create a parallelized query of tweets
        processes_favorites = []
        for fav in favorites_filters:
            processes_favorites.extend([Process(target=self.query_favorites_par, args=(user, fav, queue_favorites)) for user in fav.users])

        processes = []
        processes.extend(processes_search + processes_user_timeline + processes_favorites)

        # start the processes
        for p in processes:
            p.start()

        # concatenate all dataframes of search information
        df_search = pd.DataFrame() # dataframe to store all information from this filter type
        for _ in processes_search:
            df_process = queue_search.get()
            df_search = pd.concat([df_search, df_process])

        self.set_dict_df_posts('search', df_search)
        self._log.user_message('Tweets query finished.')

        # concatenate all dataframes of user_timeline information
        df_user_timeline = pd.DataFrame() # dataframe to store all information from this filter type
        for _ in processes_user_timeline:
            df_process = queue_user_timeline.get()
            df_user_timeline = pd.concat([df_user_timeline, df_process])

        self.set_dict_df_posts('user_timeline', df_user_timeline)
        self._log.user_message('Timeline query finished.')

        # concatenate all dataframes of favorites information
        df_favorites = pd.DataFrame() # dataframe to store all information from this filter type
        for _ in processes_favorites:
            df_process = queue_favorites.get()
            df_favorites = pd.concat([df_favorites, df_process])

        self.set_dict_df_posts('favorites', df_favorites)
        self._log.user_message('Favorites query finished.')

        # wait the processes
        for p in processes:
            p.join()

        final_time_par = time.time() - start_time_par
        self._log.timer_message('Parallelized Query Time: ' + str(final_time_par) + ' seconds.')
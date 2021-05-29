from query.QueryPostsInterface import QueryPostsInterface
from multiprocessing import Process, Queue
import time
import twint
import pandas as pd

class QueryTweetsV2(QueryPostsInterface):

    def __init__(self, authenticator, list_filters, parallelize, log) -> None:
        super().__init__()
        self._authenticator = authenticator
        self._list_filters = list_filters
        self._parallelize = parallelize
        self._log = log
        self._set_dict_df_posts = {}
        self._config = twint.Config()

    @property
    def dict_df_posts(self):
        return self._set_dict_df_posts

    def set_dict_df_posts(self, key, df) -> None:
        if(len(df) > 0):
            k = 'twitter_twint_' + key
            self._set_dict_df_posts[k] = df
    
    def query(self, twitter_filter) -> pd.DataFrame:
        df = pd.DataFrame()

        try:
            # set default parameters
            self._config.Pandas = True
            self._config.Hide_output = True

            # set the parameters
            if(twitter_filter.search is not None): self._config.Search = twitter_filter.search
            if(twitter_filter.items > 0): self._config.Limit = twitter_filter.items
            if(twitter_filter.lang is not None): self._config.Lang = twitter_filter.lang
            if(twitter_filter.since is not None): self._config.Since = twitter_filter.since
            if(twitter_filter.until is not None): self._config.Until = twitter_filter.until
            if(twitter_filter.translate):
                self._config.Translate = twitter_filter.translate
                self._config.TranslateDest = twitter_filter.translate_dest

            # run the query
            if(twitter_filter.filter_type == 'search'):
                twint.run.Search(self._config)
            elif(twitter_filter.filter_type == 'profile'):
                twint.run.Profile(self._config)
            elif(twitter_filter.filter_type == 'favorites'):
                twint.run.Favorites(self._config)

            twint.output.clean_lists()

            # store in a dataframe
            columns = ['tweet', 'id', 'conversation_id', 'created_at', 'date', 'timezone', 'place', 'language', 'hashtags', 'cashtags', 'user_id', 'user_id_str',
                        'username', 'name', 'day', 'hour', 'link', 'urls', 'photos', 'video', 'thumbnail', 'retweet', 'nlikes', 'nreplies', 'nretweets', 'quote_url',
                        'search', 'near', 'geo', 'source', 'user_rt_id', 'user_rt', 'retweet_id', 'reply_to', 'retweet_date', 'translate', 'trans_src', 'trans_dest']
            df = twint.storage.panda.Tweets_df[columns].copy()

            # standardize the name of the text column
            columns[0] = 'text' # change 'tweet' to 'text' for preprocessing
            df.columns = columns

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

    def query_timeline_favorites(self, user, twitter_filter) -> pd.DataFrame:        
        # call query function to query tweets and create a dataframe
        self._config.Username = user
        df = self.query(twitter_filter)
        
        # return the dataframe
        return df

    def query_timeline_favorites_par(self, user, twitter_filter, queue) -> None:
        # call query_timeline_favorites function to query tweets and create a dataframe
        df = self.query_timeline_favorites(user, twitter_filter)

        # put the pandas dataframe in the queue 
        queue.put(df)

    def query_manager(self) -> None:
        # select only the Twitter filters
        list_twitter_filters = list(filter(lambda x: (x.key == 'Twitter' and x.library == 'twint'), self._list_filters))

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
        profile_filters = list(filter(lambda x: (x.filter_type == 'profile'), list_filters))
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
        # concatenate all dataframes of profile information
        df_profile = pd.DataFrame() # dataframe to store all information from this filter type
        for pf in profile_filters:
            for user in pf.users:
                df_filter = self.query_timeline_favorites(user, pf)
                df_profile = pd.concat([df_profile, df_filter])

        self.set_dict_df_posts('profile', df_profile)
        self._log.user_message('Timeline query finished.')

        # for each user id from each filter, create a query of tweets
        # concatenate all dataframes of favorites information
        df_favorites = pd.DataFrame()
        for fav in favorites_filters:
            for user in fav.users:
                df_filter = self.query_timeline_favorites(user, fav)
                df_favorites = pd.concat([df_favorites, df_filter])

        self.set_dict_df_posts('favorites', df_favorites)
        self._log.user_message('Favorites query finished.')

        final_time_seq = time.time() - start_time_seq
        self._log.timer_message('Sequential Query Time: ' + str(final_time_seq) + ' seconds.')

    def query_parallel(self, list_filters) -> None:
        start_time_par = time.time()

        # separate filters by type
        search_filters = list(filter(lambda x: (x.filter_type == 'search'), list_filters))
        profile_filters = list(filter(lambda x: (x.filter_type == 'profile'), list_filters))
        favorites_filters = list(filter(lambda x: (x.filter_type == 'favorites'), list_filters))
        
        # configure queues
        queue_search = Queue()
        queue_profile = Queue()
        queue_favorites = Queue()

        # for each filter, create a parallelized query of tweets
        processes_search = [Process(target=self.query_par, args=(sf, queue_search)) for sf in search_filters]

        # for each user id from each filter, create a parallelized query of tweets
        processes_profile = []
        for pf in profile_filters:
            processes_profile.extend([Process(target=self.query_timeline_favorites_par, args=(user, pf, queue_profile)) for user in pf.users])

        # for each user id from each filter, create a parallelized query of tweets
        processes_favorites = []
        for fav in favorites_filters:
            processes_favorites.extend([Process(target=self.query_timeline_favorites_par, args=(user, fav, queue_favorites)) for user in fav.users])

        processes = []
        processes.extend(processes_search + processes_profile + processes_favorites)

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

        # concatenate all dataframes of profile information
        df_profile = pd.DataFrame() # dataframe to store all information from this filter type
        for _ in processes_profile:
            df_process = queue_profile.get()
            df_profile = pd.concat([df_profile, df_process])

        self.set_dict_df_posts('profile', df_profile)
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
from query.QueryPostsInterface import QueryPostsInterface
from multiprocessing import Process, Queue
from pmaw import PushshiftAPI
import time
import datetime as dt
import pandas as pd

class QueryRedditPostsV2(QueryPostsInterface):

    def __init__(self, list_filters, parallelize, log) -> None:
        super().__init__()
        self._list_filters = list_filters
        self._parallelize = parallelize
        self._log = log
        self._dict_df_posts = {}
        self._api = PushshiftAPI()

    @property
    def dict_df_posts(self):
        return self._dict_df_posts

    def set_dict_df_posts(self, key, df) -> None:
        if(len(df) > 0):
            k = 'reddit_pmaw_' + key
            self._dict_df_posts[k] = df

    def query(self, reddit_filter, subreddit) -> pd.DataFrame:
        df_posts = pd.DataFrame()

        try:
            posts = self._api.search_submissions(subreddit = subreddit, limit = reddit_filter.items, **reddit_filter.query_params)

            # create the dataframe
            df_posts = pd.DataFrame(posts)

            # standardize the name of the text column
            df_posts.rename(columns = {'selftext': 'text'}, inplace = True) # change 'selftext' to 'text' for preprocessing

            # format the date
            df_posts['created_utc'] = df_posts['created_utc'].apply(dt.datetime.fromtimestamp)
            
            # create a column with the value from the 'label' filter parameter
            if(reddit_filter.label is not None):
                df_posts['label'] = reddit_filter.label

        except:
            self._log.exception('Fail to query Reddit posts.')

        # return the dataframe
        return df_posts

    def query_par(self, reddit_filter, queue, subreddit) -> None:
        # call query function to query posts and create a dataframe
        df_posts = self.query(reddit_filter, subreddit)

        # put the pandas dataframe in the queue 
        queue.put(df_posts)
    
    def query_manager(self) -> None:
        # select only the Reddit filters
        list_reddit_filters = list(filter(lambda x: (x.key == 'Reddit' and x.library == 'pmaw'), self._list_filters))

        # both methods perform the same task using a parallel or sequential strategy
        if(self._parallelize):
            # query posts parallelized
            self.query_parallel(list_reddit_filters)
        else:
            # query posts sequentially
            self.query_sequential(list_reddit_filters)
    
    def query_sequential(self, list_filters) -> None:
        start_time_seq = time.time()

        # separate filters by type
        search_filters = list(filter(lambda x: (x.filter_type == 'search'), list_filters))

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        df_search_posts = pd.DataFrame()
        for sf in search_filters:
            for subreddit in sf.subreddits:
                df_posts = self.query(sf, subreddit)
                df_search_posts = pd.concat([df_search_posts, df_posts])

        self.set_dict_df_posts('search_posts', df_search_posts)
        self._log.user_message('Reddit posts\' query finished.')

        final_time_seq = time.time() - start_time_seq
        self._log.timer_message('Reddit - Sequential Query Time: ' + str(final_time_seq) + ' seconds.')
    
    def query_parallel(self, list_filters) -> None:
        start_time_par = time.time()

        # separate filters by type
        search_filters = list(filter(lambda x: (x.filter_type == 'search'), list_filters))

        # configure the queue
        queue_search = Queue()

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        processes_search = []
        for sf in search_filters:
            processes_search.extend([Process(target=self.query_par, args=(sf, queue_search, sub)) for sub in sf.subreddits])

        # start the processes
        for p in processes_search:
            p.start()

        # concatenate all dataframes of search information
        df_search_posts = pd.DataFrame() 
        for _ in processes_search:
            df_process_posts = queue_search.get()
            df_search_posts = pd.concat([df_search_posts, df_process_posts])

        self.set_dict_df_posts('search_posts', df_search_posts)
        self._log.user_message('Reddit posts\' query finished.')

        # wait the processes
        for p in processes_search:
            p.join()

        final_time_par = time.time() - start_time_par
        self._log.timer_message('Reddit - Parallelized Query Time: ' + str(final_time_par) + ' seconds.')
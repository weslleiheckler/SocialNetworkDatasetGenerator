from query.QueryPostsInterface import QueryPostsInterface
from multiprocessing import Process, Queue
import time
import datetime as dt
import pandas as pd

class QueryRedditPosts(QueryPostsInterface):

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
            k = 'reddit_' + key
            self._dict_df_posts[key] = df

    def get_date(self, post_date):
        return dt.datetime.fromtimestamp(post_date)

    def create_dataframe_posts(self, list_posts, reddit_filter) -> pd.DataFrame:
        
        posts_dict = {'title': [],       
                        'score': [],   
                        'ups': [],
                        'downs': [],
                        'id': [],        
                        'url': [],               
                        'created': [],   
                        'text': [],
                        'subreddit': [],
                        'category': [],
                        'quarantine': [],
                        'num_comments': [],
                        'num_duplicates': [],
                        'num_reports': [],
                        'num_crossposts': [],
                        'over_18': [],
                        'is_original_content': [],
                        'is_video': [],
                        'edited': [],
                        'archived': [],
                        'author_name': [],
                        'author_fullname': []
                        }  

        comments_dict = {'comment_id': [],
                            'comment_parent_id': [],
                            'text': [],       
                            'comment_link_id': []} 

        # create a dictionary with posts information
        for post in list_posts:
            posts_dict['title'].append(post.title)
            posts_dict['score'].append(post.score)
            posts_dict['ups'].append(post.ups)
            posts_dict['downs'].append(post.downs)
            posts_dict['id'].append(post.id)
            posts_dict['url'].append(post.url)
            posts_dict['created'].append(post.created)
            posts_dict['text'].append(post.selftext)
            posts_dict['subreddit'].append(post.subreddit)
            posts_dict['category'].append(post.category)
            posts_dict['quarantine'].append(post.quarantine)
            posts_dict['num_comments'].append(post.num_comments)
            posts_dict['num_duplicates'].append(post.num_duplicates)
            posts_dict['num_reports'].append(post.num_reports)
            posts_dict['num_crossposts'].append(post.num_crossposts)            
            posts_dict['over_18'].append(post.over_18)
            posts_dict['is_original_content'].append(post.is_original_content)
            posts_dict['is_video'].append(post.is_video)
            posts_dict['edited'].append(post.edited)
            posts_dict['archived'].append(post.archived)
            posts_dict['author_name'].append(post.author.name)
            posts_dict['author_fullname'].append(post.author.fullname)

            # create a dictionary with comments information
            if(reddit_filter.comments):
                if(reddit_filter.comments_limit > 0):
                    post.comments.replace_more(limit = reddit_filter.comments_limit)
                else:
                    post.comments.replace_more()

                if(reddit_filter.comment_sort is not None):
                    post.comment_sort = reddit_filter.comment_sort

                list_comments = post.comments.list()

                if(reddit_filter.comments_items > 0):
                    list_comments = list_comments[0:reddit_filter.comments_items]

                for comment in list_comments:
                    comments_dict['comment_id'].append(comment.id)
                    comments_dict['comment_parent_id'].append(comment.parent_id)
                    comments_dict['text'].append(comment.body)
                    comments_dict['comment_link_id'].append(comment.link_id)
            
        # create the dataframes with posts and comments information
        df_posts = pd.DataFrame(posts_dict)
        df_comments = pd.DataFrame(comments_dict)

        # format the date
        ts = df_posts['created'].apply(self.get_date)
        df_posts = df_posts.assign(timestamp = ts)

        # create a column with the value from the 'label' filter parameter
        if(reddit_filter.label is not None):
            df_posts['label'] = reddit_filter.label
            df_comments['label'] = reddit_filter.label

        # return the dataframes
        return df_posts, df_comments

    def query(self, reddit_filter, subreddit) -> pd.DataFrame:
        df_posts = pd.DataFrame()
        df_comments = pd.DataFrame()

        try:
            # define the subreddit for querying
            sub = self._authenticator.api.subreddit(subreddit)
            
            # query Reddit posts' according to the filters
            if(reddit_filter.filter_type == 'top'):
                if(reddit_filter.items > 0):
                    search_subreddit = sub.top(limit = reddit_filter.items, **reddit_filter.query_params)
                else:
                    search_subreddit = sub.top(**reddit_filter.query_params)
            elif(reddit_filter.filter_type == 'hot'):
                if(reddit_filter.items > 0):
                    search_subreddit = sub.hot(limit = reddit_filter.items, **reddit_filter.query_params)
                else:
                    search_subreddit = sub.hot(**reddit_filter.query_params)
            elif(reddit_filter.filter_type == 'new'):
                if(reddit_filter.items > 0):
                    search_subreddit = sub.new(limit = reddit_filter.items, **reddit_filter.query_params)
                else:
                    search_subreddit = sub.new(**reddit_filter.query_params)
            elif(reddit_filter.filter_type == 'controversial'):
                if(reddit_filter.items > 0):
                    search_subreddit = sub.controversial(limit = reddit_filter.items, **reddit_filter.query_params)
                else:
                    search_subreddit = sub.controversial(**reddit_filter.query_params)
            elif(reddit_filter.filter_type == 'gilded'):
                if(reddit_filter.items > 0):
                    search_subreddit = sub.gilded(limit = reddit_filter.items, **reddit_filter.query_params)
                else:
                    search_subreddit = sub.gilded(**reddit_filter.query_params)
            elif(reddit_filter.filter_type == 'rising'):
                if(reddit_filter.items > 0):
                    search_subreddit = sub.rising(limit = reddit_filter.items, **reddit_filter.query_params)
                else:
                    search_subreddit = sub.rising(**reddit_filter.query_params)
            else: # reddit_filter.filter_type == 'search'
                if(reddit_filter.items > 0):
                    search_subreddit = sub.search(limit = reddit_filter.items, **reddit_filter.query_params)
                else:
                    search_subreddit = sub.search(**reddit_filter.query_params)

            # create the dataframes with specific columns
            df_posts, df_comments = self.create_dataframe_posts(search_subreddit, reddit_filter)

        except:
            self._log.exception('Fail to query Reddit posts.')

        # return the dataframes
        return df_posts, df_comments

    def query_par(self, reddit_filter, queue, subreddit) -> None:
        # call query function to query posts and create a dataframe
        df_posts, df_comments = self.query(reddit_filter, subreddit)

        # put the pandas dataframes in the queue 
        queue.put(df_posts)
        queue.put(df_comments)
    
    def query_manager(self) -> None:
        # select only the Reddit filters
        list_reddit_filters = list(filter(lambda x: (x.key == 'Reddit'), self._list_filters))

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
        top_filters = list(filter(lambda x: (x.filter_type == 'top'), list_filters))
        hot_filters = list(filter(lambda x: (x.filter_type == 'hot'), list_filters))
        new_filters = list(filter(lambda x: (x.filter_type == 'new'), list_filters))
        controversial_filters = list(filter(lambda x: (x.filter_type == 'controversial'), list_filters))
        gilded_filters = list(filter(lambda x: (x.filter_type == 'gilded'), list_filters))
        rising_filters = list(filter(lambda x: (x.filter_type == 'rising'), list_filters))

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        df_search_posts = pd.DataFrame()
        df_search_comments = pd.DataFrame()
        for sf in search_filters:
            for subreddit in sf.subreddits:
                df_posts, df_comments = self.query(sf, subreddit)
                df_search_posts = pd.concat([df_search_posts, df_posts])
                df_search_comments = pd.concat([df_search_comments, df_comments])

        self.set_dict_df_posts('search_posts', df_search_posts)
        self.set_dict_df_posts('search_comments', df_search_comments)
        self._log.user_message('Reddit posts\' query finished.')

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        df_top_posts = pd.DataFrame()
        df_top_comments = pd.DataFrame()
        for tf in top_filters:
            for subreddit in tf.subreddits:
                df_posts, df_comments = self.query(tf, subreddit)
                df_top_posts = pd.concat([df_top_posts, df_posts])
                df_top_comments = pd.concat([df_top_comments, df_comments])

        self.set_dict_df_posts('top_posts', df_top_posts)
        self.set_dict_df_posts('top_comments', df_top_comments)
        self._log.user_message('Reddit top posts\' query finished.')

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        df_hot_posts = pd.DataFrame()
        df_hot_comments = pd.DataFrame()
        for hf in hot_filters:
            for subreddit in hf.subreddits:
                df_posts, df_comments = self.query(hf, subreddit)
                df_hot_posts = pd.concat([df_hot_posts, df_posts])
                df_hot_comments = pd.concat([df_hot_comments, df_comments])

        self.set_dict_df_posts('hot_posts', df_hot_posts)
        self.set_dict_df_posts('hot_comments', df_hot_comments)
        self._log.user_message('Reddit hot posts\' query finished.')

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        df_new_posts = pd.DataFrame()
        df_new_comments = pd.DataFrame()
        for nf in new_filters:
            for subreddit in nf.subreddits:
                df_posts, df_comments = self.query(nf, subreddit)
                df_new_posts = pd.concat([df_new_posts, df_posts])
                df_new_comments = pd.concat([df_new_comments, df_comments])

        self.set_dict_df_posts('new_posts', df_new_posts)
        self.set_dict_df_posts('new_comments', df_new_comments)
        self._log.user_message('Reddit new posts\' query finished.')

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        df_controversial_posts = pd.DataFrame()
        df_controversial_comments = pd.DataFrame()
        for cf in controversial_filters:
            for subreddit in cf.subreddits:
                df_posts, df_comments = self.query(cf, subreddit)
                df_controversial_posts = pd.concat([df_controversial_posts, df_posts])
                df_controversial_comments = pd.concat([df_controversial_comments, df_comments])

        self.set_dict_df_posts('controversial_posts', df_controversial_posts)
        self.set_dict_df_posts('controversial_comments', df_controversial_comments)
        self._log.user_message('Reddit controversial posts\' query finished.')

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        df_gilded_posts = pd.DataFrame()
        df_gilded_comments = pd.DataFrame()
        for gf in gilded_filters:
            for subreddit in gf.subreddits:
                df_posts, df_comments = self.query(gf, subreddit)
                df_gilded_posts = pd.concat([df_gilded_posts, df_posts])
                df_gilded_comments = pd.concat([df_gilded_comments, df_comments])

        self.set_dict_df_posts('gilded_posts', df_gilded_posts)
        self.set_dict_df_posts('gilded_comments', df_gilded_comments)
        self._log.user_message('Reddit gilded posts\' query finished.')

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        df_rising_posts = pd.DataFrame()
        df_rising_comments = pd.DataFrame()
        for rf in rising_filters:
            for subreddit in rf.subreddits:
                df_posts, df_comments = self.query(rf, subreddit)
                df_rising_posts = pd.concat([df_rising_posts, df_posts])
                df_rising_comments = pd.concat([df_rising_comments, df_comments])

        self.set_dict_df_posts('rising_posts', df_rising_posts)
        self.set_dict_df_posts('rising_comments', df_rising_comments)
        self._log.user_message('Reddit rising posts\' query finished.')

        final_time_seq = time.time() - start_time_seq
        self._log.timer_message('Sequential Query Time: ' + str(final_time_seq) + ' seconds.')
    
    def query_parallel(self, list_filters) -> None:
        start_time_par = time.time()

        # separate filters by type
        search_filters = list(filter(lambda x: (x.filter_type == 'search'), list_filters))
        top_filters = list(filter(lambda x: (x.filter_type == 'top'), list_filters))
        hot_filters = list(filter(lambda x: (x.filter_type == 'hot'), list_filters))
        new_filters = list(filter(lambda x: (x.filter_type == 'new'), list_filters))
        controversial_filters = list(filter(lambda x: (x.filter_type == 'controversial'), list_filters))
        gilded_filters = list(filter(lambda x: (x.filter_type == 'gilded'), list_filters))
        rising_filters = list(filter(lambda x: (x.filter_type == 'rising'), list_filters))

        # configure queues
        queue_search = Queue()
        queue_top = Queue()
        queue_hot = Queue()
        queue_new = Queue()
        queue_controversial = Queue()
        queue_gilded = Queue()
        queue_rising = Queue()

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        processes_search = []
        for sf in search_filters:
            processes_search.extend([Process(target=self.query_par, args=(sf, queue_search, sub)) for sub in sf.subreddits])

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        processes_top = []
        for tf in top_filters:
            processes_top.extend([Process(target=self.query_par, args=(tf, queue_top, sub)) for sub in tf.subreddits])

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        processes_hot = []
        for hf in hot_filters:
            processes_hot.extend([Process(target=self.query_par, args=(hf, queue_hot, sub)) for sub in hf.subreddits])

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        processes_new = []
        for nf in new_filters:
            processes_new.extend([Process(target=self.query_par, args=(nf, queue_new, sub)) for sub in nf.subreddits])

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        processes_controversial = [] 
        for cf in controversial_filters:
            processes_controversial.extend([Process(target=self.query_par, args=(cf, queue_controversial, sub)) for sub in cf.subreddits])

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        processes_gilded = []
        for gf in gilded_filters:
            processes_gilded.extend([Process(target=self.query_par, args=(gf, queue_gilded, sub)) for sub in gf.subreddits])

        # for each subreddit from each filter, create a query of posts
        # concatenate all dataframes of posts information
        processes_rising = []
        for rf in rising_filters:
            processes_rising.extend([Process(target=self.query_par, args=(rf, queue_rising, sub)) for sub in rf.subreddits])

        processes = []
        processes.extend(processes_search + processes_top + processes_hot + processes_new + processes_controversial + processes_gilded + processes_rising)

        # start the processes
        for p in processes:
            p.start()

        # concatenate all dataframes of search information
        df_search_posts = pd.DataFrame() 
        df_search_comments = pd.DataFrame()
        for _ in processes_search:
            df_process_posts = queue_search.get()
            df_process_comments = queue_search.get()
            df_search_posts = pd.concat([df_search_posts, df_process_posts])
            df_search_comments = pd.concat([df_search_comments, df_process_comments])

        self.set_dict_df_posts('search_posts', df_search_posts)
        self.set_dict_df_posts('search_comments', df_search_comments)
        self._log.user_message('Reddit posts\' query finished.')

        # concatenate all dataframes of search information
        df_top_posts = pd.DataFrame() 
        df_top_comments = pd.DataFrame()
        for _ in processes_top:
            df_process_posts = queue_top.get()
            df_process_comments = queue_top.get()
            df_top_posts = pd.concat([df_top_posts, df_process_posts])
            df_top_comments = pd.concat([df_top_comments, df_process_comments])

        self.set_dict_df_posts('top_posts', df_top_posts)
        self.set_dict_df_posts('top_comments', df_top_comments)
        self._log.user_message('Reddit top posts\' query finished.')

        # concatenate all dataframes of search information
        df_hot_posts = pd.DataFrame() 
        df_hot_comments = pd.DataFrame()
        for _ in processes_hot:
            df_process_posts = queue_hot.get()
            df_process_comments = queue_hot.get()
            df_hot_posts = pd.concat([df_hot_posts, df_process_posts])
            df_hot_comments = pd.concat([df_hot_comments, df_process_comments])

        self.set_dict_df_posts('hot_posts', df_hot_posts)
        self.set_dict_df_posts('hot_comments', df_hot_comments)
        self._log.user_message('Reddit hot posts\' query finished.')

        # concatenate all dataframes of search information
        df_new_posts = pd.DataFrame() 
        df_new_comments = pd.DataFrame()
        for _ in processes_new:
            df_process_posts = queue_new.get()
            df_process_comments = queue_new.get()
            df_new_posts = pd.concat([df_new_posts, df_process_posts])
            df_new_comments = pd.concat([df_new_comments, df_process_comments])

        self.set_dict_df_posts('new_posts', df_new_posts)
        self.set_dict_df_posts('new_comments', df_new_comments)
        self._log.user_message('Reddit new posts\' query finished.')

        # concatenate all dataframes of search information
        df_controversial_posts = pd.DataFrame() 
        df_controversial_comments = pd.DataFrame()
        for _ in processes_controversial:
            df_process_posts = queue_controversial.get()
            df_process_comments = queue_controversial.get()
            df_controversial_posts = pd.concat([df_controversial_posts, df_process_posts])
            df_controversial_comments = pd.concat([df_controversial_comments, df_process_comments])

        self.set_dict_df_posts('controversial_posts', df_controversial_posts)
        self.set_dict_df_posts('controversial_comments', df_controversial_comments)
        self._log.user_message('Reddit controversial posts\' query finished.')

        # concatenate all dataframes of search information
        df_gilded_posts = pd.DataFrame() 
        df_gilded_comments = pd.DataFrame()
        for _ in processes_gilded:
            df_process_posts = queue_gilded.get()
            df_process_comments = queue_gilded.get()
            df_gilded_posts = pd.concat([df_gilded_posts, df_process_posts])
            df_gilded_comments = pd.concat([df_gilded_comments, df_process_comments])

        self.set_dict_df_posts('gilded_posts', df_gilded_posts)
        self.set_dict_df_posts('gilded_comments', df_gilded_comments)
        self._log.user_message('Reddit gilded posts\' query finished.')

        # concatenate all dataframes of search information
        df_rising_posts = pd.DataFrame() 
        df_rising_comments = pd.DataFrame()
        for _ in processes_rising:
            df_process_posts = queue_rising.get()
            df_process_comments = queue_rising.get()
            df_rising_posts = pd.concat([df_rising_posts, df_process_posts])
            df_rising_comments = pd.concat([df_rising_comments, df_process_comments])

        self.set_dict_df_posts('rising_posts', df_rising_posts)
        self.set_dict_df_posts('rising_comments', df_rising_comments)
        self._log.user_message('Reddit rising posts\' query finished.')

        # wait the processes
        for p in processes:
            p.join()

        final_time_par = time.time() - start_time_par
        self._log.timer_message('Parallelized Query Time: ' + str(final_time_par) + ' seconds.')
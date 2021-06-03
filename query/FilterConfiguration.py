import configparser, os
from query.Filter import Filter
import datetime as dt

class FilterConfiguration():

    def __init__(self, log) -> None:
        self._list_filters = []
        self._tweepy = False
        self._twint = False
        self._praw = False
        self._pmaw = False
        self._log = log

    @property
    def list_filters(self):
        return self._list_filters

    @property
    def tweepy(self):
        return self._tweepy

    @property
    def twint(self):
        return self._twint

    @property
    def praw(self):
        return self._praw

    @property
    def pmaw(self):
        return self._pmaw

    def import_filters(self) -> None:
        
        # check whether the config file exists
        if not os.path.exists('config/config_filter.cfg'): 
            self._log.error('Filter configuration file not found. Verify whether the config_filter.cfg file is in the config subdirectory.')
        else:
            # read the filter configurations in the cfg file
            config = configparser.ConfigParser()
            config.read('config/config_filter.cfg')

            # check whether the config file has sections
            if not config.sections():
                self._log.error('Filter configuration file does not contain filter sections.')
            else:
                # create a filter for each section in the cfg file
                for section in config.sections():
                    sec = section.split('_') # Twitter_Filter_1
                    key = sec[0]             # Twitter
                    id = sec[2]              # 1

                    label = ''
                    filter_type = ''
                    items = 0
                    search = None
                    lang = None
                    since = None
                    until = None
                    translate = False
                    translate_dest = None
                    library = None
                    comments = False
                    comments_limit = 0
                    comments_items = 0
                    comment_sort = None
                    users = []
                    subreddits = []
                    query_params = {}

                    # check whether the section has parameters
                    if not config.items(section):
                        self._log.error('Section ' + '' + section + '' + ' does not contain parameters.')
                    else:
                        # for each section, read the parameters and build a query
                        for param, value in config.items(section):
                            if(param == 'label'):
                                # the 'label' parameter is used for classifying the selected registers
                                label = value
                            elif(param == 'filter_type'):
                                # the 'filter_type' parameter is used for choosing the filter method
                                filter_type = value
                            elif(param == 'items'):
                                # the 'items' parameter is used for choosing the number of items selected
                                items = value
                            elif(param == 'search'):
                                # the 'search' parameter is used to set specific terms to search for tweets
                                search = value
                            elif(param == 'lang' and library == 'twint'):
                                # the 'lang' parameter is used to set the language to search for tweets (twint)
                                # the 'library' parameter must be above the 'lang' parameter in the configuration file
                                # for the tweepy package, 'lang' is an internal parameter stored in the query_params dictionary
                                lang = value
                            elif(param == 'since' and library == 'twint'):
                                # the 'since' parameter is used to set the start date to search for tweets (twint)
                                # the 'library' parameter must be above the 'since' parameter in the configuration file
                                # for the tweepy package, 'since' is an internal parameter stored in the query_params dictionary
                                since = value
                            elif(param == 'until' and library == 'twint'):
                                # the 'until' parameter is used to set the final date to search for tweets (twint)
                                until = value
                            elif(param == 'translate'):
                                # the 'translate' parameter is used to define whether the tweet will be translated
                                if value == 'Yes': translate = True
                            elif(param == 'translate_dest'):
                                # the 'translate_dest' parameter is used to define the language for tweet translation
                                translate_dest = value
                            elif(param == 'library'):
                                # the 'library' parameter is used to define the library for collecting data
                                library = value

                                if(library == 'tweepy'):
                                    self._tweepy = True
                                elif(library == 'twint'):
                                    self._twint = True
                                elif(library == 'praw'):
                                    self._praw = True
                                elif(library == 'pmaw'):
                                    self._pmaw = True
                            elif(param == 'id'):
                                # the 'users' parameter is used for passing a list of users to get users' information
                                users = value.split(',')
                            elif(param == 'subreddits'):
                                # the 'subreddits' parameter is used for passing a list of subreddits to get Reddit information
                                subreddits = value.split(',')  
                            elif(param == 'comments'):
                                # the 'comments' parameter is used for querying comments of Reddit posts
                                if value == 'Yes': comments = True
                            elif(param == 'comments_limit'):
                                # the 'comments_limit' parameter is used for choosing the limit to the 'replace_more' method of the Praw package
                                comments_limit = value
                            elif(param == 'comments_items'):
                                # the 'comments_items' parameter is used for choosing the limit of selected comments for each Reddit post
                                comments_items = value
                            elif(param == 'comment_sort'):
                                # the 'comment_sort' parameter is used for choosing the order of selected comments for each Reddit post
                                # in general, this parameter is combined with the parameter 'comments_items'
                                comment_sort = value
                            else:
                                # build a query params dictionary
                                if((param in ('since','until')) and library == 'pmaw'):
                                    date = dt.datetime.strptime(value, "%Y-%m-%d")
                                    timestamp = int(dt.datetime.timestamp(date))
                                    k = 'after' if(param == 'since') else 'before'
                                    query_params[k] = timestamp
                                else:
                                    query_params[param] = value

                        # create a filter and append to list
                        self.create_filter(key, id, filter_type, query_params, users, subreddits, items, search, lang, since, until, translate, translate_dest, 
                                            library, comments, comments_limit, comments_items, comment_sort, label)
                
    def create_filter(self, key, id, filter_type, query_params, users, subreddits, items, search, lang, since, until, translate, translate_dest, 
                        library, comments, comments_limit, comments_items, comment_sort, label) -> None:
        filter = Filter(key, id, filter_type, query_params, users, subreddits, items, search, lang, since, until, translate, translate_dest,
                        library, comments, comments_limit, comments_items, comment_sort, label, self._log)
        self._list_filters.append(filter)
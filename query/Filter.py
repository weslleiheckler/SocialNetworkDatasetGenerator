class Filter():

    def __init__(self, key, id, filter_type, query_params, users, subreddits, items, comments, comments_limit, comments_items, comment_sort, label, log) -> None:
        self._key = key
        self._id = id
        self._filter_type = filter_type
        self._query_params = query_params
        self._users = users
        self._subreddits = subreddits
        self._items = items
        self._comments = comments
        self._comments_limit = comments_limit
        self._comments_items = comments_items
        self._comment_sort = comment_sort
        self._label = label
        self._log = log

    @property
    def key(self):
        return self._key

    @property
    def id(self):
        return self._id

    @property
    def filter_type(self):
        return self._filter_type

    @property
    def query_params(self):
        return self._query_params

    @property
    def users(self):
        return self._users  

    @property
    def subreddits(self):
        return self._subreddits  
    
    @property
    def items(self):
        return int(self._items)

    @property
    def comments(self):
        return self._comments

    @property
    def comments_limit(self):
        return int(self._comments_limit)

    @property
    def comments_items(self):
        return int(self._comments_items)

    @property
    def comment_sort(self):
        return self._comment_sort

    @property
    def label(self):
        return self._label
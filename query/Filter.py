class Filter():

    def __init__(self, key, id, filter_type, query_params, users, items, label, log) -> None:
        self._key = key
        self._id = id
        self._filter_type = filter_type
        self._query_params = query_params
        self._users = users
        self._items = items
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
    def items(self):
        return int(self._items)

    @property
    def label(self):
        return self._label
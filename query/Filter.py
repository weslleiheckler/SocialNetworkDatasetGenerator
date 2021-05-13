class Filter():

    def __init__(self, key, id, filter_type, query, label, log) -> None:
        self._key = key
        self._id = id
        self._filter_type = filter_type
        self._query = query
        self._label = label
        self._log = log

    
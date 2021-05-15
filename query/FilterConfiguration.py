import configparser, os
from query.Filter import Filter

class FilterConfiguration():

    def __init__(self, log) -> None:
        self._list_filters = []
        self._log = log

    @property
    def list_filters(self):
        return self._list_filters

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
                    query_params = {}
                    users = []

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
                            elif(param == 'id'):
                                # the 'users' parameter is used for passing a list of users to get users' information
                                users = value.split(',')
                            else:
                                # build a query params dictionary
                                query_params[param] = value

                        # create a filter and append to list
                        self.create_filter(key, id, filter_type, query_params, users, items, label)
                        self._log.user_message(key + ' - ' + id + ' - ' + filter_type + ' - ' + items + ' - ' + label)
                
    def create_filter(self, key, id, filter_type, query_params, users, items, label) -> None:
        filter = Filter(key, id, filter_type, query_params, users, items, label, self._log)
        self._list_filters.append(filter)
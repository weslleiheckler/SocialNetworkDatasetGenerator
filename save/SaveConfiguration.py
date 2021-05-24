import configparser, os

class SaveConfiguration():
    
    def __init__(self, log) -> None:
        super().__init__()
        self._source = None
        self._csv_path = None
        self._csv_sep = None
        self._csv_encoding = None
        self._mongodb_url = None
        self._mongodb_database_name = None
        self._log = log

    @property
    def source(self):
        return self._source

    @property
    def csv_path(self):
        return self._csv_path

    @property
    def csv_sep(self):
        return self._csv_sep

    @property 
    def csv_encoding(self):
        return self._csv_encoding

    @property
    def mongodb_url(self):
        return self._mongodb_url

    @property
    def mongodb_database_name(self):
        return self._mongodb_database_name
    
    def config(self) -> None:
        
        # check whether the config file exists
        if not os.path.exists('config/config.ini'): 
            self._log.error('Configuration file not found. Verify whether the config.ini file is in the config subdirectory.')
        else:
            # read the connection configurations in the ini file
            config = configparser.ConfigParser()
            config.read('config/config.ini')

            # verify the Save section
            if 'Save' in config:
                try:
                    self._log.user_message('Trying to import the save configurations.')
                    self._source = config['Save']['source']
                except:
                    self._log.exception('Save configurations not found or incorrect. Verify the Save section in the config file.')               

                if(self._source == 'CSV'):
                    if 'CSV' in config:
                        try:
                            self._csv_path = config['CSV']['path']
                            if self._csv_path[-1:] != '\\' : self._csv_path += '\\'
                            self._csv_sep = config['CSV']['sep']
                            if self._csv_sep == 'semicolon': self._csv_sep = ';'
                            self._csv_encoding = config['CSV']['encoding']

                            self._log.user_message('CSV save configurations imported.')
                        except:
                            self._log.exception('CSV configurations not found or incorrect. Verify the CSV section in the config file.') 
                    else:
                        self._log.error('CSV section not found. Verify the CSV section in the config file.')

                elif(self._source == 'MongoDB'):
                    if 'MongoDB' in config:
                        try:
                            self._mongodb_url = config['MongoDB']['url']
                            self._mongodb_database_name = config['MongoDB']['database_name']

                            self._log.user_message('MongoDB save configurations imported.')
                        except:
                            self._log.exception('MongoDB configurations not found or incorrect. Verify the MongoDB section in the config file.')    
                    else:
                        self._log.error('MongoDB section not found. Verify the MongoDB section in the config file.')

            else:
                self._log.error('Save section not found. Verify the Save section in the config file.')               
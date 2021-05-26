from multiprocessing import Process
import pymongo
import time

class Save():

    def __init__(self, save_config, dict_df_posts, log) -> None:
        self._save_config = save_config
        self._dict_df_posts = dict_df_posts
        self._log = log

    def save_csv(self, key, df) -> None:
        # get the path and file name
        path = self._save_config.csv_path
        name = key + '.csv'
        file_name = path + name

        # separator and encoding
        sep = self._save_config.csv_sep
        enc = self._save_config.csv_encoding

        # save to csv
        df.to_csv(file_name, encoding = enc, sep = sep)

    def save_mongodb(self, key, df) -> None:
        # create the client
        client = pymongo.MongoClient(self._save_config.mongodb_url)

        # configure the collection
        db = client[self._save_config.mongodb_database_name]
        collection = db[key]

        # reset the index
        df.reset_index(inplace=True)

        # insert the data into the collection
        collection.insert_many(df.to_dict('records'))

    def save_manager(self, key, df) -> None:
        # verify the source for saving
        source = self._save_config.source

        # save according to the source
        if(source == 'CSV'):
            self.save_csv(key, df)
        elif(source == 'MongoDB'):
            self.save_mongodb(key, df)

    def save(self) -> None:
        start_time_save = time.time()

        # for each dataframe in the dictionary, create a process for saving
        processes = [Process(target=self.save_manager, args=(key, df)) for key, df in self._dict_df_posts.items()]

        # start the processes
        for p in processes:
            p.start()
        
        # wait the processes
        for p in processes:
            p.join()

        self._log.user_message('Posts saved.')

        final_time_save = time.time() - start_time_save
        self._log.timer_message('Saving Process: ' + str(final_time_save) + ' seconds.')
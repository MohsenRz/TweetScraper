import os
import logging
import json
from scrapy.utils.project import get_project_settings

from TweetScraper.items import Tweet, User
from TweetScraper.utils import mkdirs


logger = logging.getLogger(__name__)
SETTINGS = get_project_settings()


class SaveToFilePipeline(object):
    ''' pipeline that save data to disk '''

    def __init__(self):
        self.saveTweetPath = SETTINGS['SAVE_TWEET_PATH']
        self.saveUserPath = SETTINGS['SAVE_USER_PATH']
        mkdirs(self.saveTweetPath)  # ensure the path exists
        mkdirs(self.saveUserPath)

    def process_item(self, item, spider):
        if isinstance(item, Tweet):
            savePath = os.path.join(self.saveTweetPath, item['id_'])
            if os.path.isfile(savePath):
                pass  # simply skip existing items
                # logger.debug("skip tweet:%s"%item['id_'])
                # or you can rewrite the file, if you don't want to skip:
                # self.save_to_file(item,savePath)
                # logger.debug("Update tweet:%s"%item['id_'])
            else:
                self.save_to_file(item, savePath)
                logger.debug("Add tweet:%s" % item['id_'])

        elif isinstance(item, User):
            savePath = os.path.join(self.saveUserPath, item['id_'])
            if os.path.isfile(savePath):
                pass  # simply skip existing items
                # logger.debug("skip user:%s"%item['id_'])
                # or you can rewrite the file, if you don't want to skip:
                # self.save_to_file(item,savePath)
                # logger.debug("Update user:%s"%item['id_'])
            else:
                self.save_to_file(item, savePath)
                logger.debug("Add user:%s" % item['id_'])

        else:
            logger.info("Item type is not recognized! type = %s" % type(item))

    def save_to_file(self, item, fname):
        ''' input: 
                item - a dict like object
                fname - where to save
        '''
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(dict(item), f, ensure_ascii=False)


class MongoPipeline(object):
    ''' pipeline that save data to database '''

    def __init__(self):
        self.mongo_uri = SETTINGS['MONGO_URI']
        self.mongo_db = SETTINGS['MONGO_DB']
        self.tweets = 'tweets'
        self.users = 'users'

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.db.tweets.create_index([("id_str", pymongo.DESCENDING)])
        self.db.users.create_index([("id_str", pymongo.DESCENDING)])

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        if isinstance(item, Tweet):
            self.db[self.tweets].insert_one(
                ItemAdapter(item['raw_data']).asdict())
            logger.debug("Add tweet:%s" % item['id_'])

        elif isinstance(item, User):
            self.db[self.users].insert_one(
                ItemAdapter(item['raw_data']).asdict())
            logger.debug("Add user:%s" % item['id_'])

        else:
            logger.info("Item type is not recognized! type = %s" % type(item))

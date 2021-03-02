import os
import logging
import json
import pymongo

from itemadapter import ItemAdapter
from datetime import datetime
from scrapy.utils.project import get_project_settings

from TweetScraper.items import Tweet, User
from TweetScraper.utils import mkdirs


logger = logging.getLogger(__name__)
SETTINGS = get_project_settings()


class SaveToFilePipeline(object):
    ''' pipeline that save data to disk '''

    # def __init__(self):

    def open_spider(self, spider):
        self.saveTweetPath = SETTINGS['SAVE_TWEET_PATH'] + spider.id + '/'
        self.saveUserPath = SETTINGS['SAVE_USER_PATH'] + spider.id + '/'

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

    def close_spider(self, spider):
        self.db.tweets.create_index(
            [("id_str", pymongo.DESCENDING)], background=True)
        self.db.users.create_index(
            [("id_str", pymongo.DESCENDING)], background=True)
        self.client.close()

    def process_item(self, item, spider):

        if isinstance(item, Tweet):
            self.db[self.tweets].insert_one(
                ItemAdapter(self.filter_tweet(item['raw_data'])).asdict())
            logger.debug("Add tweet:%s" % item['id_'])

        elif isinstance(item, User):
            self.db[self.users].insert_one(
                ItemAdapter(self.filter_user(item['raw_data'])).asdict())
            logger.debug("Add user:%s" % item['id_'])

        else:
            logger.info("Item type is not recognized! type = %s" % type(item))

    def filter_tweet(self, item):
        new_item = {}
        date = datetime.strptime(
            item['created_at'], '%a %b %d %H:%M:%S %z %Y')
        new_item['created_at'] = date.strftime("%Y-%m-%d %H:%M:%S")
        new_item['id_str'] = item['id_str']
        new_item['full_text'] = item['full_text']
        new_item['favorite_count'] = item['favorite_count']
        new_item['user_id_str'] = item['user_id_str']
        # hashtags

        return new_item

    def filter_user(self, item):
        new_item = {}
        new_item['id_str'] = item['id_str']
        new_item['name'] = item['name']
        new_item['screen_name'] = item['screen_name']
        new_item['description'] = item['description']
        return new_item

# !!! # Crawl responsibly by identifying yourself (and your website/e-mail) on the user-agent
from shutil import which
USER_AGENT = 'test/mohsenrz.shahbakhsh@gmail.com'

# settings for spiders
BOT_NAME = 'TweetScraper'
LOG_LEVEL = 'INFO'

SPIDER_MODULES = ['TweetScraper.spiders']
NEWSPIDER_MODULE = 'TweetScraper.spiders'
ITEM_PIPELINES = {
    # 'TweetScraper.pipelines.SaveToFilePipeline': 100,
    'TweetScraper.pipelines.MongoPipeline': 100,
}

# settings for where to save data on disk
SAVE_TWEET_PATH = './Data/tweet/'
SAVE_USER_PATH = './Data/user/'

# mongodb settings
MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DB = 'tweetscraper'


DOWNLOAD_DELAY = 1.0

# settings for selenium
SELENIUM_DRIVER_NAME = 'firefox'
SELENIUM_BROWSER_EXECUTABLE_PATH = which('firefox')
SELENIUM_DRIVER_EXECUTABLE_PATH = which('geckodriver')
# '--headless' if using chrome instead of firefox
SELENIUM_DRIVER_ARGUMENTS = ['-headless']
DOWNLOADER_MIDDLEWARES = {
    'scrapy_selenium.SeleniumMiddleware': 800
}

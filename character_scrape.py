import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.shell import open_in_browser
import json
import argparse
import logging
import time
import datetime
import mysql.connector
import configparser
import os

topic_list = []
character_list = []

max_log_days = 7
json_path = "jcink_sync/"

# Logging Setup
logging.basicConfig(filename=time.strftime('jcink_sync/logs/jcink_sync-%Y-%m-%d.log'), 
                    level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s", 
                    filemode='a') 
logger = logging.getLogger()

# Configuration options loaded from env.ini file
config = configparser.ConfigParser()
config.read('jcink_sync/env.ini')
creds = config['mysql']
host = creds['host']
database = creds['db']
user = creds['user']
passwd = creds['passwd']

'''
To use the interactive shell, execute: scrapy shell 'http://quotes.toscrape.com/page/1/'
'''

class TopicSpider(scrapy.Spider):
    logging.getLogger('scrapy').propagate = False
    name = 'topic'
    

    def start_requests(self):
        urls = [
            # 'http://drivingtowarddeath.jcink.net/index.php?showforum=6',
            # Aethers
            'http://drivingtowarddeath.jcink.net/index.php?showforum=142',
            # Dryads
            'http://drivingtowarddeath.jcink.net/index.php?showforum=70',
            # Faes
            'http://drivingtowarddeath.jcink.net/index.php?showforum=128',
            # Harpies
            'http://drivingtowarddeath.jcink.net/index.php?showforum=37',
            # Humans
            'http://drivingtowarddeath.jcink.net/index.php?showforum=38',
            # Hydras
            'http://drivingtowarddeath.jcink.net/index.php?showforum=88',
            # Kemuri
            'http://drivingtowarddeath.jcink.net/index.php?showforum=95',
            # Kitsunes
            'http://drivingtowarddeath.jcink.net/index.php?showforum=158',
            # Merfolk
            'http://drivingtowarddeath.jcink.net/index.php?showforum=39',
            # Nuks
            'http://drivingtowarddeath.jcink.net/index.php?showforum=119',
            # Qilins
            'http://drivingtowarddeath.jcink.net/index.php?showforum=157',
            # Shifters
            'http://drivingtowarddeath.jcink.net/index.php?showforum=40',
            # Sphinx
            'http://drivingtowarddeath.jcink.net/index.php?showforum=41',
            # Uraei
            'http://drivingtowarddeath.jcink.net/index.php?showforum=42',
            # Vampires
            'http://drivingtowarddeath.jcink.net/index.php?showforum=43',
            # Werewolves
            'http://drivingtowarddeath.jcink.net/index.php?showforum=44',
            # White Stags
            'http://drivingtowarddeath.jcink.net/index.php?showforum=103',
            # Witches
            'http://drivingtowarddeath.jcink.net/index.php?showforum=45',
        ]
        for url in urls:
            try:
                yield scrapy.Request(url = url, callback=self.parse)
            except:
                logger.error("Could not parse " + url)
    


    def parse(self, response):
        # open_in_browser(response)
        try:
            # Get the name of the subforum we're browsing here
            # maintitle = response.xpath('//div[@class="maintitle"]/text()').extract()

            try:
                # topic urls
                topics = response.xpath('//div[@class="top-title"]//a/@href').extract()
                for topic in topics:
                    # Clean topic url here
                    show_topic_position = topic.find('showtopic=')
                    if show_topic_position != -1:
                        topic_id_start = show_topic_position + 10
                        # topic_id = a_link[topic_id_start:(topic_id_start+4)]
                        topic_id = topic[topic_id_start:(topic_id_start+4)]
                        clean_url = 'http://drivingtowarddeath.jcink.net/index.php?' + 'showtopic=' + topic_id
                        
                    topic_list.append(clean_url)
            except:
                logger.error("This topic link doesn't follow normal pattern: " + topic)
        except:
            logger.error("Error retrieving topics from this url: " + str(response.request.url))

        # Handle pagination
        current_page = response.xpath('//span[@class="pagination_current"]/b/text()').extract_first()

        if current_page == str(1):
            page_list = []
            
            # Get other page URLs & add to page_list
            other_pages = response.xpath('//a[@class="pagination_page"]/@href').extract()
            for page in other_pages:
                if page not in page_list:
                    page_list.append(page)
            
            # Follow each page in the list
            for i in page_list:
                yield response.follow(i)

class CharacterSpider(scrapy.Spider):
    logging.getLogger('scrapy').propagate = False
    name = 'character'
    
    def start_requests(self):
        if os.path.exists(json_path + 'new_urls.json'):
            with open(json_path + 'new_urls.json') as f:
                urls = json.load(f)

            for url in urls:
                try:
                    yield scrapy.Request(url = url, callback=self.parse)
                except:
                    logger.error("Could not parse " + url)
        else:
            logger.info(json_path + 'new_urls.json does not exist.  CharacterSpider cannot run.')
    
    def parse(self, response):
        # maintitle = response.xpath("//div[@class='maintitle']/text()").get()
        # print(maintitle)
        url = response.request.url

        # Get Full Name first li tag with b value of "full name"
        full_name = response.xpath("//div[@class='tabs']/div[2]//li[1]/text()").get().lstrip()

        # Get the faceclaim
        faceclaim = response.xpath("//div[@class='tabs']/div[2]//li[8]/text()").get().lstrip()

        # Get species, the id of the div under 
        species = response.xpath("//div[@class='hundredeuro']/div/@id").get()
        # Get OOC name
        player = response.xpath("//label[@title='ooc']/../div//li[1]/text()").get().lstrip()
        thischaracter = {
            "url": url,
            "name": full_name,
            "species": species,
            "faceclaim": faceclaim,
            "player": player,
        }
        character_list.append(thischaracter)

def connect_to_DB():
    mydb = mysql.connector.connect(
            host = host,
            database = database,
            user = user,
            passwd = passwd,
            auth_plugin = "mysql_native_password")

    try:
        if (mydb):
            status = "connection successful"
        else:
            status = "connection failed"

        if status == "connection successful":
            return mydb
    except Exception as e:
        status = "Failure %s" % str(e)
        logger.error("Failure %s" % str(e))

def get_all_characters_from_db(active):
    character_url_list = []
    # Statements
    select_statement = """SELECT url FROM characters"""

    # Inactive Characters
    if active == 'N':
        select_statement = """SELECT url FROM characters WHERE active = 'N'"""
    # Active Characters
    elif active == 'Y':
        select_statement = """SELECT url FROM characters WHERE active = 'Y'"""
    # All
    else:
        select_statement = """SELECT url FROM characters"""

    mydb = connect_to_DB()

    try:
        if mydb is None:
            connect_to_DB()
        else:
            mydb.ping(True)
            my_cursor = mydb.cursor()
            my_cursor.execute(select_statement)
            all_chars = my_cursor.fetchall()
            mydb.close()
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(str(e))
        mydb.rollback()
        logger.error("Failure in get_all_characters_from_db()")
        return False
    
    for character in all_chars:
        character_url_list.append(character[0])
    return character_url_list

def insert_characters():
    # Desc: insert characters from json file into db

    if os.path.exists(json_path + 'characters.json'):
        with open(json_path + 'characters.json') as c:
            characters = json.load(c)

            if len(characters) > 0:
                temp_list = []
                row_list = []

                for i in characters:
                    temp_list.clear()
                    url = i["url"].lower()
                    name = i["name"].lower()
                    species = i["species"].lower()
                    faceclaim = i["faceclaim"].lower()
                    player = i["player"].lower()
                    temp_list = [url, name, species, faceclaim, player] 
                    row_list.append(temp_list.copy())

                mydb = connect_to_DB()

                # Statements
                insert_statement = """INSERT INTO characters (url, name, species, faceclaim, player_name, active, updated) VALUES (%s, %s, %s, %s, %s, 'Y', now())"""

                try:
                    if mydb is None:
                        connect_to_DB()
                    else:
                        mydb.ping(True)
                        my_cursor = mydb.cursor()
                        my_cursor.executemany(insert_statement,row_list)
                        mydb.commit()
                        logger.info(str(my_cursor.rowcount) + " records inserted successfully into character table")
                        mydb.close()
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    logger.error(str(e))
                    mydb.rollback()
                    logger.error("Failure in insert_database()")
                    return False
            else:
                logger.info("No new characters to insert.")
    else:
        logger.info("characters.json does not exist")

def update_inactive_chars(urls_to_archive):
    # Verify list is not blank
    if len(urls_to_archive) > 1:
        mydb = connect_to_DB()
        update_statement = """UPDATE characters SET active = 'N' WHERE url = '""" 

        try:
            if mydb is None:
                connect_to_DB()
            else:
                mydb.ping(True)

            for x in urls_to_archive:
                my_cursor = mydb.cursor()
                my_cursor.execute(update_statement + x + "'")
                mydb.commit()
                logger.info(x + " successfully marked as inactive")
                time.sleep(5)
            
            mydb.close()
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error(str(e))
            mydb.rollback()
            logger.error("Failure executing update statement in update_inactive_chars")
            return False
    else:
         logger.info("No characters to archive in list :: update_inactive_chars()")

def log_cleanup(days_to_keep):
    path = 'jcink_sync/logs'
    earliest_date = datetime.datetime.today() - datetime.timedelta(days_to_keep)

    if os.path.exists(path):
        files = os.listdir(path)

        for f in files:
            f.find('.log')
            file_date = datetime.datetime.strptime(f[11:-4], '%Y-%m-%d')
            if earliest_date > file_date:
                logger.info(file_date.strftime('%Y-%m-%d') + " has been removed from /logs")
    else:
        logger.error("Cannot locate " + path)

def cleanup():
    # new_urls.json cleanup
    if os.path.exists("jcink_sync/new_urls.json"):
        os.remove("jcink_sync/new_urls.json")
        logger.info("Removed jcink_sync/new_urls.json")

    # active file cleanup
    if os.path.exists("jcink_sync/active.json"):
        os.remove("jcink_sync/active.json")
        logger.info("Removed jcink_sync/active.json")
    
    # characters.json cleanup
    if os.path.exists("jcink_sync/characters.json"):
        os.remove("jcink_sync/characters.json")
        logger.info("Removed jcink_sync/characters.json")

    log_cleanup(max_log_days)

def get_character_details(name_of_spider):
    # Desc: call spider to process application urls & write to json
    if os.path.exists(json_path + 'new_urls.json'):
        process = CrawlerProcess()
        process.crawl(name_of_spider)
        process.start()

        with open(json_path + 'characters.json', 'w') as fout:
            json.dump(character_list, fout)
    else:
        logger.info(json_path + 'new_urls.json does not exist.  CharacterSpider cannot run :: get_character_details()')

def determine_new_active_characters():
    # Desc: compares json active characters to db active characters
    urls_of_new_active_chars = []
    urls_to_archive = []

    # active.json into urls list
    with open(time.strftime(json_path + 'active.json')) as f:
        urls = json.load(f)

    database_urls = get_all_characters_from_db('Y')

    # Archived urls
    for db_char in database_urls:
        if db_char not in urls:
            urls_to_archive.append(db_char)

    if urls_to_archive:
        with open(json_path + 'inactive_characters.json', 'w') as fout:
            json.dump(urls_to_archive, fout) 

    # New Active Characters
    for url in urls:
        if url in database_urls:
            pass
        else:
            urls_of_new_active_chars.append(url)
    
    if not urls_of_new_active_chars:                    
        logger.info("No new urls")
    else:
        with open(json_path + 'new_urls.json', 'w') as fout:
            json.dump(urls_of_new_active_chars, fout)
    
def get_active_characters(name_of_spider):
    # Desc: runs scraper against forum for all active character applications
    process = CrawlerProcess()
    process.crawl(name_of_spider)
    process.start()

    with open(time.strftime(json_path + 'active.json'), 'w') as json_file:
        json.dump(topic_list, json_file)
        logger.info("active characters have been written to json")




if __name__ == "__main__":
    # Initialize parser
    my_parser = argparse.ArgumentParser()
    my_parser.add_argument('-a', '--active', action='store_true', help='execute the active character script')
    my_parser.add_argument('-d', '--detail', action='store_true', help='insert details into database')
    my_parser.add_argument('-t', '--test', action='store_true', help='test')

    # Read arguments from command line
    args = my_parser.parse_args()

    # -----------------------------------------------------------
    # Active Characters Run
    # -----------------------------------------------------------
    if args.active:
        logger.info("-----------------------------------------------------------")
        logger.info("-----------------------------------------------------------")
        logger.info("Start Active Characters Run")
        logger.info("-----------------------------------------------------------")
        logger.info("-----------------------------------------------------------")
        try:
            get_active_characters(TopicSpider)
        except:
            logger.error("Failure in get_active_characters()")
        
        try:
            determine_new_active_characters()
        except:
            logger.error("Failure in determine_new_active_characters()")
            
    elif args.detail:
        logger.info("-----------------------------------------------------------")
        logger.info("-----------------------------------------------------------")
        logger.info("Start detailed run")
        logger.info("-----------------------------------------------------------")
        logger.info("-----------------------------------------------------------")
        try:
            get_character_details(CharacterSpider)
        except:
            logger.error("Failure in get_character_details()")   

        try:
            if os.path.exists(json_path + 'characters.json'):
                insert_characters() 
            else:
                logger.error(json_path + 'characters.json does not exist for insert_characters')
        except:
            logger.error("Failure in insert_characters()")
        
        try:
            if os.path.exists(json_path + 'inactive_characters.json'):
                with open(json_path + 'inactive_characters.json') as f:
                    urls_to_archive = json.load(f)
                update_inactive_chars(urls_to_archive)
            else:
                logger.error(json_path + 'inactive_characters.json does not exist for update_inactive_chars()')
        except:
            logger.error("Failure in update_inactive_chars")
        logger.info("Begin clean up")
        cleanup()   
    elif args.test:
        log_cleanup(max_log_days)
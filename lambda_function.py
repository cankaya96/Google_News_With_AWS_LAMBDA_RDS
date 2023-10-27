import sys
import logging
import json
import os
import requests
import pymysql
import xml.etree.ElementTree as ET # built in library
import configparser
config = configparser.ConfigParser()
config.read('config.ini')


logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    return json.dumps(News(), default=str)

class News():

    def __init__ (self):

        self.user_name = config['RDS_SETTINGS']['USER_NAME']
        self.password = config['RDS_SETTINGS']['PASSWORD']
        self.rds_host = config['RDS_SETTINGS']['RDS_HOST']
        self.db_name = config['RDS_SETTINGS']['DB_NAME']
        
        self.lambda_handler_news()

    # clear the description
    def get_text(self, x):
        start = x.find('" target="_blank">')+18
        end = x.find('</a>')
        return x[start:end]

    def connection_db(self):
        try:
            self.conn = pymysql.connect(host=self.rds_host, user=self.user_name, db=self.db_name, passwd=self.password,  connect_timeout=35)
           

        except pymysql.MySQLError as e:
            logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
            logger.error(e)
            sys.exit(1)

        return logger.info("SUCCESS: Connection to RDS for MySQL instance succeeded")

    def lambda_handler_news(self):
        self.connection_db()
        """
        Search through Google News with the "search_term" and get the headlines 
        and the contents of the news that was released today, this week, this month, 
        or this year ("date_filter"). 
        """
        
        #self.conn.cursor().execute("""create table if not exists most_viewed_news (  
        #                    Title varchar(255) NOT NULL,
        #                    Link varchar(255) NOT NULL,
        #                    Description varchar(255) NOT NULL,
        #                   PubDate DATETIME,
        #                    Source varchar(255) NOT NULL)""")
            
        url = 'https://news.google.com/rss?hl=tr&gl=TR&ceid=TR:tr'
        response = requests.get(url)
        # get the root directly as we have text file of string now
        root= ET.fromstring(response.text)
        #get the required data
        titles = [i.text.replace("'",'"' ) for i in root.findall('.//channel/item/title') ]
        links = [i.text.replace("'",'"' ) for i in root.findall('.//channel/item/link') ]
        descriptions = [i.text.replace("'",'"' ) for i in root.findall('.//channel/item/description') ]
        pubDates = [i.text.replace("'",'"' ) for i in root.findall('.//channel/item/pubDate') ]
        sources = [i.text.replace("'",'"' ) for i in root.findall('.//channel/item/source') ]
        # clear the description
        short_descriptions = list(map(self.get_text,descriptions))
        #for i in len(range(title)):
        #sql_string = f"insert into most_viewed_news ( Title, Link, Description, PubDate, Source) values( '{titles}', '{links}', '{short_descriptions}', '{pubDates}', '{sources}')"
    
        with self.conn.cursor() as cur:
            cur.execute("""create table if not exists most_viewed_news (  
                        Title varchar(255) NOT NULL,
                        Link varchar(255) NOT NULL,
                        Description varchar(255) NOT NULL,
                        PubDate DATETIME,
                        Source varchar(255) NOT NULL)""")
            for i in range(len(titles)):

                sql_string = f"insert into most_viewed_news ( Title, Link, Description, PubDate, Source) values( '{titles[i]}', '{links[i]}', '{short_descriptions[i]}', '{pubDates[i]}', '{sources[i]}')"
                cur.execute(sql_string)
            self.conn.commit()
            logger.info("The following items have been added to the database:")

        self.conn.commit()
        return "Added items to RDS for MySQL table"

          

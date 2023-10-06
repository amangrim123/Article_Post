import re
from dateutil import parser, tz
from ntpath import join
import scrapy
from datetime import datetime,timedelta
from urllib.parse import urlparse
import os
import logging
import csv
import mysql.connector
import sys
import subprocess
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerProcess
import time
current_working_file = __file__
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

Script_Path = os.path.dirname(os.path.abspath(__file__))
log_files = os.path.join(Script_Path,"logs_files_of_automation_script")
if not os.path.exists(log_files):
    os.mkdir(log_files)

Error_file = os.path.join(log_files,"Error_in_publish.log")
Success_file = os.path.join(log_files,"Successfull.log")

#################### Logs Files ###################
error_log = setup_logger("Error_files",Error_file)
success_log = setup_logger("Successful_files",Success_file)

import logging
current_working_folder = (os.path.realpath(os.path.dirname(__file__)))

class QuotesSpider(scrapy.Spider):
    mydb = mysql.connector.connect(
    host="18.189.108.83",
    user="wp_raj1",
    password="rajPassword95$",
    database="Article_Post"
    )
    name = "quotes"
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM last_feed_date where category='feed' limit 1")
    myresult = mycursor.fetchone()

    print("This is myresult = ",myresult)
    if not myresult:
        sql = "insert into last_feed_date(last_update,category) values(now(),'feed')"
        mycursor.execute(sql)
        mycursor.execute("SELECT now();")
        currentdate = mycursor.fetchone()
        SavedDate = currentdate[0]
    else:
        SavedDate=myresult[1]

    LatestDate = datetime.now()
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM destination_website where status = 1 ")
    myresult = mycursor.fetchall()
    al_web=[]
    for ress in myresult:
        mycursor.execute("SELECT * FROM bulk_feed_website where des_id=(%s)" %  (ress[0]))

        websites = mycursor.fetchall()
        print("this is websites data = ",websites)
        al_web.extend(websites)
    def start_requests(self):
        for url in self.al_web:
           if "feed" in url[1]:

                yield scrapy.Request(url=url[1], meta={'bfw_id':url[0],"Destination_id":url[6],"title12":url[2],"feature_imgg":url[4],"catego":url[5],'contte':url[3]}, callback=self.parse)
           else:
               yield scrapy.Request(url=url[1], meta={'bfw_id':url[0],"Destination_id":url[6],"title12":url[2],"feature_imgg":url[4],"catego":url[5],'contte':url[3],'date_path':url[8],'article_link_path':url[7]}, callback=self.parsehtml)



    def parse(self, response):
        domain = urlparse(response.url).netloc
        response.selector.register_namespace('content','http://purl.org/rss/1.0/modules/content/')
        for quote in response.xpath('//item'):
            pubDate=quote.xpath('.//pubDate/text()').get().replace(" +0000","")
            date_time_obj = datetime.strptime(pubDate, '%a, %d %b %Y %H:%M:%S')
            if self.SavedDate<date_time_obj:
                title00 = quote.xpath('.//title/text()').get()
                print("this is title",title00)
                content=quote.xpath('.//content:encoded/text()').get()
                catte=response.meta['catego']
                if catte is not None and '/' not in catte:
                    category00=catte
                else:
                    category00 = quote.xpath('.//category/text()').get()
                url=quote.xpath('.//link/text()').get()
                bfwid=(response.meta['bfw_id'])
                dest_id=(response.meta['Destination_id'])
                yield scrapy.Request(url=url,meta={'bfw_id':bfwid,"uurl":url,"Destination_id":dest_id,"title13":title00,"feature_imgg13":response.meta['feature_imgg'],"catego13":category00,"cantnt":response.meta['contte']}, callback=self.find_feature_image)

    def find_feature_image(self, response):
        aa=response.meta['title13']
        bb=response.meta['feature_imgg13']
        cc=response.meta['catego13']
        dd=response.meta['cantnt']
        urll=response.meta['uurl']
        bfwid1=(response.meta['bfw_id'])
        dest_id1=(response.meta['Destination_id'])
        try:
            if '/' in dd:
                content_of_article= response.xpath(f'{dd}').get()
            else:
                content_of_article= response.css(f'{dd}').get()
            if '/' in bb:
                feature_image_of_article=response.xpath(f'{bb}').get()
            else:
                feature_image_of_article=response.css(f'{bb}').get()
            sql = "insert into bulk_feed_content(bfw_id,url,title,content,featured_image,Category,Destination_id) values(%s,%s,%s,%s,%s,%s,%s)"
            val = (bfwid1,urll,aa,content_of_article,feature_image_of_article,cc, dest_id1)
            self.mycursor.execute(sql, val)
            self.mydb.commit()
        except Exception as e:
                error_log.exception(f"{datetime.datetime.now(),urll} - Error: {str(e)}\n")

    def parsehtml(self, response):
        bfw_id12=response.meta['bfw_id']
        Destination_id12=response.meta['Destination_id']
        date_path=response.meta['date_path']
        link_path=response.meta['article_link_path']
        feature_image_path=response.meta['feature_imgg']
        article_links = response.css(link_path).getall()
        feat=response.css(feature_image_path).getall()
        for link,fea in zip(article_links,feat):
            if 'https' not in link:
                ful_url = re.search(r"(https?://[\w.-]+)/?", response.url).group(1)
                link = ful_url + link
            if 'https' not in fea:
                ful_url = re.search(r"(https?://[\w.-]+)/?", response.url).group(1)
                fea = ful_url + fea
            yield scrapy.Request(link,meta={'date_path12':date_path,'article_url':link,'bfw_id121':bfw_id12,'Destination_id121':Destination_id12,'feature_images':fea,"title_path":response.meta['title12'],"cantnt":response.meta['contte'],"category_path":response.meta['catego']}, callback=self.parse_article)

    def parse_article(self, response):
        date_path121=response.meta['date_path12']
        if '/' in date_path121:
            article_date=(response.xpath(date_path121).get())
        else:
            article_date=(response.css(date_path121).get())

        tzinfos = {"PT": tz.tzoffset("PT", -28800),
           "EST": tz.tzoffset("EST", -18000)}
        if '@' in article_date:
            article_date=article_date.replace('@','')
        date = parser.parse(article_date, tzinfos=tzinfos)
        date_formatted = date.strftime("%Y-%m-%d %H:%M:%S")
        date_object = datetime.strptime(date_formatted, "%Y-%m-%d %H:%M:%S")
        if self.SavedDate<date_object:
            title_path12=response.meta['title_path']
            content_path12=response.meta['cantnt']
            category_path12=response.meta['category_path']
            if '/' in title_path12:
                article_title = response.xpath(title_path12).get().strip()
            else:
                article_title = response.css(title_path12).get().strip()

            feature_image=response.meta['feature_images']
            if "/" and ':' not in category_path12:
                category_is=category_path12
            elif '/' in category_path12:
                category_is=response.xpath(content_path12).get()
            else:
                category_is=response.css(content_path12).get()

            if '/' in content_path12:
                content_is=response.xpath(content_path12).get()
            else:
                content_is=response.css(content_path12).get()



            try:
                sql = "insert into bulk_feed_content(bfw_id,url,title,content,featured_image,Category,Destination_id) values(%s,%s,%s,%s,%s,%s,%s)"
                val = (response.meta['bfw_id121'],response.meta['article_url'],article_title,content_is,feature_image,category_is, response.meta['Destination_id121'])
                self.mycursor.execute(sql, val)
                self.mydb.commit()
            except Exception as e:
                error_log.exception(f"{datetime.datetime.now(),article_title} - Error: {str(e)}\n")

        else:
            pass

    def closed(self, reason):
        sql = "update last_feed_date set last_update=now() where ldf_id=2"
        self.mycursor.execute(sql)
        self.mydb.commit()
        self.mydb.close()

if __name__ == '__main__':
    while True:
        try:
            process = subprocess.call(['scrapy', 'runspider', current_working_file])
            print("==== Program is Sleep for 10 minutes ==== ")
            time.sleep(600)
        except Exception as eaa:
                error_log.exception(f"{datetime.datetime.now()} - Error: {str(eaa)}\n")

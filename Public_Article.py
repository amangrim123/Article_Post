import time
import requests
import time
from datetime import date,datetime
import mysql.connector 
import os
import logging
import json

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
current_working_file = __file__
current_working_folder = (os.path.realpath(os.path.dirname(__file__)))

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

def Total_Post(mydb,destination_id,destination_site,total_posts):
    current=time.strftime("%Y-%m-%d",time.localtime())
    mycursor = mydb.cursor()
    sql = "INSERT INTO Total_posts (Destination_id, Destination, Total_article, last_update) VALUES (%s, %s, %s, %s)"
    val = (destination_id,destination_site,total_posts,current)
    mycursor.execute(sql, val)

    mydb.commit()

def Content_Public(mydb,dir,all_content,token):
    dest__id = dir[0]
    post_count=[]
    mycursor = mydb.cursor()
    status_is = dir[6]
    print("Site Article status is ",status_is)
    post_count.append(len(all_content))
    ii = 0
    for a_content in all_content:
        domain = dir[1]
        try:
            url = domain+"wp-json/wp/v2/posts"
            category_url = domain+"wp-json/wp/v2/categories/"
            imageURL=a_content[5]
            category = a_content[7]
            header = {'Authorization': 'Bearer ' + token}
            current=time.strftime("%Y-%m-%dT%H:%M:%S",time.localtime())

            title = a_content[3]
            print(title)

            post = {
            'title'    : title,
            'status'   : 'draft',
            'content'  : a_content[6],
            'categories': 1
            }

            #'date'   : '2022-04-07T10:00:00'
            responce = requests.post(url , headers=header, json=post)
            time.sleep(2)
            img_data = requests.get(imageURL).content

            uploadImageFilename = "661b943654f54bd4b2711264eb275e1b.jpg"
            curHeaders = {
                'Authorization': 'Bearer ' + token,
                "Content-Type": "image/jpeg",
                "Accept": "application/json",
                'Content-Disposition': "attachment; filename=%s" % uploadImageFilename ,
                }

            resp = requests.post(domain+"wp-json/wp/v2/media",
            headers=curHeaders,
            data=img_data,
            )
            time.sleep(2)
            newDict=resp.json()
            postid = str(json.loads(responce.content)['id'])
            upd={
                'name':category,'slug':category
            }
            update_cat = requests.post(category_url , headers=header, json=upd)
            try:
                postiid = str(json.loads(update_cat.content)['id'])
            except KeyError:
                postiid = str(json.loads(update_cat.content)['data']['term_id'])
            print(postiid)
            updatedpost = {'title'    : title,
                'status'   : status_is,
                'content'  : a_content[6],
                'categories':postiid,
                "featured_media":int(postid)+1
                }
            time.sleep(2)
            update = requests.post(url + '/' + postid, headers=header, json=updatedpost)
            # print("postid = " ,postid)
            link1=update.json().get('id')
            print("link1id = ",link1)
            sql = "update bulk_feed_content set status_publish=1 where bfc_id=%s" % (a_content[0])
            # val = (1)
            mycursor.execute(sql)
            mydb.commit()
            success_log.exception(f"{datetime.now(),domain} - All articles has been published successfully.\n")
            ii+=1
            Total_Post(mydb,ii,dest__id)
        except Exception as e:
            mycursor.execute("update bulk_feed_content set status_publish=0 where bfc_id=%s" % (a_content[0]))
            mydb.commit()
            error_log.exception(f"{datetime.now(),domain} - Error: {str(e)}\n")
    Total_Post(mydb,dest__id,domain,ii)        

def Get_Content(mysql,dir):
    mycursor = mysql.cursor()
    all_Content=[]
    post_count=[]
    mycursor.execute("SELECT * FROM bulk_feed_content where Destination_id=(%s) and status = 1 and status_publish is NULL " % (dir[0]) )
    webs = mycursor.fetchall()
    print(f"Total Post Get {len(webs)} For destination id = {dir[0]}")
    all_Content.extend(webs[0:10])
    post_count.append(len(all_Content))
    if len(all_Content) > 0:
        return all_Content
    else:
        return False

def Get_token(dir,mydb):
    mycursor = mydb.cursor()
    dest__id = dir[0]
    domain=dir[1]
    print(domain)
    auth_url = domain+"wp-json/jwt-auth/v1/token"

    wp_user = dir[2]
    wp_pwd = dir[3]

    auth_data = {
            "username":wp_user,
            "password":wp_pwd
        }
    
    try:
        auth_responce = requests.post(auth_url , json=auth_data,timeout=20)
        try:
            token = auth_responce.json().get('data').get('token')
        except AttributeError:
            token = auth_responce.json().get('token')
    except:
        mycursor = mydb.cursor()
        sql = f"UPDATE destination_website SET Jwt_token = 'NULL' WHERE des_id = '{dest__id}'"
        mycursor.execute(sql)
        mydb.commit()
        mycursor.close()
        return False
    
    mycursor = mydb.cursor()
    sql = f"UPDATE destination_website SET Jwt_token = '{token}' WHERE des_id = '{dest__id}'"
    mycursor.execute(sql)
    mydb.commit()
    mycursor.close()
    return token    

def main():
    while True:
        try:
            mydb = mysql.connector.connect(
                    host="18.189.108.83",
                    user="wp_raj1",
                    password="rajPassword95$",
                    database="Article_Post"
                )
            mycursor = mydb.cursor()
            mycursor.execute("SELECT * FROM destination_website where status = 1 ")
            myresult = mycursor.fetchall()
            for dir in myresult:
                Site_Token = Get_token(dir,mydb)
                if Site_Token:
                    print(Site_Token)
                    time.sleep(5)
                    All_Contents = Get_Content(mydb,dir)
                    if Get_Content:
                        Content_Public(mydb,dir,All_Contents,Site_Token)
                time.sleep(10)
            mydb.close()
        except Exception as ee:
            print(ee)
            error_log.exception(f"{datetime.now()} - Error: {str(ee)}\n")   

#################  Start Script From Here ######################
if "__main__" == __name__:
    print("Start Script.............")
    main()
import configparser
#Librerias de Twitter
import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
#from HTMLParser import HTMLParser
#librerias BD
import sqlite3
#librerias Sistema operativo
import platform
import os
import sys


config = configparser.ConfigParser()
config.read('params.cfg')
consumer_key = config.get('TwitterAuth2','consumer_key')
consumer_secret = config.get('TwitterAuth2','consumer_secret')
access_token_key = config.get('TwitterAuth2','access_token_key')
access_token_secret = config.get('TwitterAuth2','access_token_secret')

#-------------------------------------------------------------------------------
#Autenticación Twitter
#-------------------------------------------------------------------------------
auth1 = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth1.set_access_token(access_token_key, access_token_secret)

#-------------------------------------------------------------------------------
#Ubicación BD
#-------------------------------------------------------------------------------
if platform.system() == 'Windows':
     db_location = os.path.normpath('C:/sqlite3/APITwitter.db')
else:
    db_location = r'/Your_Linux_Directory_Path/APITwitter.db'

objectsCreate = {'APISearch':
                 'CREATE TABLE IF NOT EXISTS APISearch ('
                 'Busqueda text,'
                 'user_id int, '
                 'user_name text, '
                 'screen_name  text, '
                 'user_description text, '
                 'user_location text, '
                 'user_url text, '
                 'user_created_datetime text,'
                 'user_language text ,'
                 'user_timezone text, '
                 'user_utc_offset real,'
                 'user_friends_count real,'
                 'user_followers_count real,'
                 'user_statuses_count real,'
                 'tweet_id int,'
                 'tweet_id_str text,'
                 'tweet_text text,'
                 'tweet_created_timestamp text'
                 ')',

                 'APIStreaming':
                 'CREATE TABLE IF NOT EXISTS APIStreaming ('
                 'Busqueda text,'
                 'user_id int, '
                 'user_name text, '
                 'screen_name  text, '
                 'user_description text, '
                 'user_location text, '
                 'user_url text, '
                 'user_created_datetime text,'
                 'user_language text ,'
                 'user_timezone text, '
                 'user_utc_offset real,'
                 'user_friends_count real,'
                 'user_followers_count real,'
                 'user_statuses_count real,'
                 'tweet_id int,'
                 'tweet_id_str text,'
                 'tweet_text text,'
                 'tweet_created_timestamp text'
                 ')'
                }
#-------------------------------------------------------------------------------
#crea la BD
#-------------------------------------------------------------------------------
db_is_new = not os.path.exists(db_location)
with sqlite3.connect(db_location) as conn:
    if db_is_new:
        print("Creating database schema on " + db_location + " database...\n")
        for t in objectsCreate.items():
            try:
                conn.executescript(t[1])
            except sqlite3.OperationalError as e:
                print (e)
                conn.rollback()
                sys.exit(1)
            else:
                conn.commit()
    else:
        print('Database already exists, bailing out...')


#-------------------------------------------------------------------------------
#Twitter StreamListener
#-------------------------------------------------------------------------------
class StreamListener(tweepy.StreamListener):

       def on_status(self, tweet):
           print ('Ran on_status')

       def on_error(self, status_code):
           print ('Error: ' + repr(status_code))
           return False

       def on_data(self, data):
           print ('Ok, this is actually running')
           print (data)
           try:
            Datos = json.loads(data)
            print(setTerms)
            print("user_id: " + str(Datos["user"]["id"]) )
            print("user_name: " + str(Datos["user"]["name"]))
            print("user_screen_name: " + str(Datos["user"]["screen_name"]))
            print("user_description: " + str(Datos["user"]["description"]))
            print("user_location: " + str(Datos["user"]["location"]))
            print("user_url: " + str(Datos["user"]["url"]))
            print("user_created_at: " + str(Datos["user"]["created_at"]))
            print("user_lang: " + str(Datos["user"]["lang"]))
            print("user_time_zone: " + str(Datos["user"]["time_zone"]))
            print("user_utc_offset: " + str(Datos["user"]["utc_offset"]))
            print("user_friends_count: " + str(Datos["user"]["friends_count"]))
            print("user_followers_count: " + str(Datos["user"]["followers_count"]))
            print("user_statuses_count: " + str(Datos["user"]["statuses_count"]))
            print("tweet_id: " + str(Datos["id"]))
            print("tweet_id_str: " + str(Datos["id_str"]))
            print("tweet_created_at: " + str(Datos["created_at"]))
            print("tweet_id_str: " + str(Datos["text"].replace("\n", "")) )

            conn.execute("INSERT OR IGNORE INTO APIStreaming ("
                             "user_id, "
                             "user_name, "
                             "screen_name, "
                             "user_description, "
                             "user_location, "
                             "user_url, "
                             "user_created_datetime, "
                             "user_language, "
                             "user_timezone, "
                             "user_utc_offset, "
                             "user_friends_count, "
                             "user_followers_count, "
                             "user_statuses_count, "
                             "tweet_id, "
                             "tweet_id_str, "
                             "tweet_text, "
                             "tweet_created_timestamp )"                             
                             "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                                 str(Datos["user"]["id"]),
                                 str(Datos["user"]["name"]),
                                 str(Datos["user"]["screen_name"]),
                                 str(Datos["user"]["description"]),
                                 str(Datos["user"]["location"]),
                                 str(Datos["user"]["url"]),
                                 str(Datos["user"]["created_at"]),
                                 str(Datos["user"]["lang"]),
                                 str(Datos["user"]["time_zone"]),
                                 str(Datos["user"]["utc_offset"]),
                                 str(Datos["user"]["friends_count"]),
                                 str(Datos["user"]["followers_count"]),
                                 str(Datos["user"]["statuses_count"]),
                                 str(Datos["id"]),
                                 str(Datos["id_str"]),
                                 str(Datos["text"].replace("\n", "")),
                                 str(Datos["created_at"])
                                )
                             )
            conn.commit()

           except Exception as e:
            print("Error: ", e )



stream_listener = StreamListener()
streamer = tweepy.Stream(auth=auth1, listener=stream_listener)
setTerms = ['casa', 'blanca','Casa Banca', 'White House']
streamer.filter(track = setTerms)
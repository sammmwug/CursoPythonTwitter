import tweepy
import configparser
import sqlite3
import os
import platform
import sys


config = configparser.ConfigParser()
config.read('params.cfg')
AppKey = config.get('TwitterAuth','AppKey')
AppSecret = config.get('TwitterAuth','AppSecret')
AccesesKey = config.get('TwitterAuth','AccesesKey')
AccesesSecret = config.get('TwitterAuth','AccesesSecret')
SentimentURL = 'http://text-processing.com/api/sentiment/'
WordCloudTweetNo = 40
StatusesCount = 200
SkipStatus = 1
busqueda = ''
FechaInicial = ''
FechaFinal   = ''

ScreenName = '@Pedro'
max_str_id = 3200



if platform.system() == 'Windows':
     db_location = os.path.normpath('C:/sqlite3/APITwitter.db')
else:
    db_location = r'/Your_Linux_Directory_Path/APITwitter.db'

objectsCreate = {'APISearch':
                 'CREATE TABLE IF NOT EXISTS APISearch ('
                 'Busqueda text, '
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
                 'tweet_created_timestamp text '
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


def getTweets(busqueda , FechaInicial , FechaFinal):


    try:

        auth = tweepy.OAuthHandler(AppKey, AppSecret)
        api = tweepy.API(auth)


        print('Texto a buscar', busqueda)
        print('Fecha Inicial', FechaInicial)
        print('Fecha Final', FechaFinal)
        search = tweepy.Cursor( api.search, q = busqueda + '-filter:retweets', include_entities=True , since = FechaInicial, until = FechaFinal ,  result_type="recent").items(3200)
        print(search)

        try:
            for item in search:

                print('id:', item.id)
                print('name:', item.user.name)
                print('screen_name:', item.user.screen_name)
                print('user.description:', item.user.description)
                print('user.location:', item.user.location)
                print('user.url:', item.user.url)
                print('created_at:', item.created_at)
                print('user.utc_offset:', item.user.utc_offset)
                print('user.friends_count:', item.user.friends_count)
                print('user.followers_count:', item.user.followers_count)
                print('user.statuses_count:', item.user.statuses_count)
                print('text:', item.text)
                print('item_user.id:', item.user.id)
                print('item.user.created_at', item.user.created_at)

                conn.execute("INSERT OR IGNORE INTO APISearch ("
                             "busqueda, "
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
                             "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                                 busqueda,
                                 item.user.id,
                                 item.user.name,
                                 item.user.screen_name,
                                 item.user.description,
                                 item.user.location,
                                 item.user.url,
                                 str(item.user.created_at),
                                 item.user.lang,
                                 item.user.time_zone,
                                 item.user.utc_offset,
                                 item.user.friends_count,
                                 item.user.followers_count,
                                 item.user.statuses_count,
                                 item.id,
                                 item.id_str,
                                 str(item.text.replace("\n", "")),
                                 str(item.created_at)
                                )
                             )

                conn.commit()

        except sqlite3.Error as error:
            print("Failed to insert data into sqlite table", error)
        finally:
            if (conn):
                print("The SQLite connection is closed")


    except Exception as e:
       print(e)
       sys.exit(1)

def main():
    getTweets('@congresoGuate' , '2019-01-01' , '2020-10-06')
    conn.close()

if __name__ == "__main__":
    main()
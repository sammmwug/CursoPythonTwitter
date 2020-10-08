import tweepy
import twython
import configparser
import sqlite3
import os
import platform
import re
import requests
import sys
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
import time
from collections import Counter
from tqdm import tqdm
from geopy.geocoders import GoogleV3
from geopy.exc import GeocoderQuotaExceeded

config = configparser.ConfigParser()
config.read('params.cfg')
AppKey = config.get('TwitterAuth', 'AppKey')
AppSecret = config.get('TwitterAuth', 'AppSecret')
AccesesKey = config.get('TwitterAuth', 'AccesesKey')
AccesesSecret = config.get('TwitterAuth', 'AccesesSecret')
SentimentURL = 'http://text-processing.com/api/sentiment/'
WordCloudTweetNo = 40
StatusesCount = 200
SkipStatus = 1
busqueda = ''
FechaInicial = ''
FechaFinal = ''
ScreenName = '@Pedro'
max_str_id = 3200
contador: int

if platform.system() == 'Windows':
    db_location = os.path.normpath('C:/sqlite3/Twitter.db')  # e.g. 'C:/Twitter_Scraping_Project/twitterDB.db'
else:
    db_location = r'/Your_Linux_Directory_Path/Twitter.db'  # e.g. '/home/username/Twitter_Scraping_Project/twitterDB.db'

objectsCreate = {'UserTimeline':
                     'CREATE TABLE IF NOT EXISTS UserTimeline ('
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
                     'tweet_created_timestamp text,'
                     'tweet_probability_sentiment_positive real,'
                     'tweet_probability_sentiment_neutral real,'
                     'tweet_probability_sentiment_negative real,'
                     'tweet_sentiment text, '
                     'PRIMARY KEY(tweet_id, user_id))',

                 'FollowersGeoData':
                     'CREATE TABLE IF NOT EXISTS FollowersGeoData ('
                     'follower_id int,'
                     'follower_name text,'
                     'follower_location text,'
                     'location_latitude real,'
                     'location_longitude real,'
                     'PRIMARY KEY (follower_id))',

                 'WordsCount':
                     'CREATE TABLE IF NOT EXISTS WordsCount ('
                     'word text,'
                     'frequency int)'

                     ')'}

# create database file and schema using the scripts above
db_is_new = not os.path.exists(db_location)
with sqlite3.connect(db_location) as conn:
    if db_is_new:
        print("Creating database schema on " + db_location + " database...\n")
        for t in objectsCreate.items():
            try:
                conn.executescript(t[1])
            except sqlite3.OperationalError as e:
                print(e)
                conn.rollback()
                sys.exit(1)
            else:
                conn.commit()
    else:
        print('Database already exists, bailing out...')

UserFollowerIDs = []
cur = 'SELECT DISTINCT follower_id from FollowersGeoData'
data = conn.execute(cur).fetchall()
for f in data:
    UserFollowerIDs.extend(f)


def getTweets(busqueda, FechaInicial, FechaFinal):
    tweets = []
    try:
        auth = tweepy.OAuthHandler(AppKey, AppSecret)
        api = tweepy.API(auth)

        UserFollowerIDs = []
        cur = 'SELECT DISTINCT follower_id from FollowersGeoData'
        data = conn.execute(cur).fetchall()

        # try:
        print('Texto a buscar', busqueda)
        print('Fecha Inicial', FechaInicial)
        print('Fecha Final', FechaFinal)
        try:

            #for item in tweepy.Cursor(api.user_timeline, screen_name=busqueda, exclude_replies=True).items(100):
            for item in tweepy.Cursor(api.search, q = busqueda + '-filter:retweets', include_entities=True , result_type="recent").items(100):
                # print(cont)
                # print('id:', item.id)
                # print('name:', item.user.name)
                # print('screen_name:', item.user.screen_name)
                # print('user.description:', item.user.description)
                # print('user.location:', item.user.location)
                # print('user.url:', item.user.url)
                # print('created_at:', item.created_at)
                # print('user.utc_offset:', item.user.utc_offset)
                # print('user.friends_count:', item.user.friends_count)
                # print('user.followers_count:', item.user.followers_count)
                # print('user.statuses_count:', item.user.statuses_count)
                # print('text:', item.text)
                # print('item_user.id:', item.user.id)
                # print('item.user.created_at', item.user.created_at)
                # user_created_at = str(item.user.created_at)
                # created_at = str(item.created_at)
                # print('paso')
                conn.execute("INSERT OR IGNORE INTO UserTimeline ("
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
                             "tweet_created_timestamp) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
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
                print('paso gettweets')
                print("The SQLite connection is closed")


    except Exception as e:
        print(e)
        sys.exit(1)


# assign sentiment to individual tweets
def getSentiment(SentimentURL):
    print('entro')
    cur = ''' SELECT tweet_id, tweet_text, count(*) as NoSentimentRecCount
                  FROM UserTimeline
                  WHERE tweet_sentiment IS NULL
                  GROUP BY tweet_id, tweet_text '''

    data = conn.execute(cur).fetchone()

    if data is None:
        print('Sentiment already assigned to relevant records or table is empty, bailing out...')
    else:
        print('Assigning sentiment to selected tweets...')
        data = conn.execute(cur)
        payload = {'text': 'tweet'}
        for t in tqdm(data.fetchall(), leave=1):

            try:
                id = t[0]
                #print(payload)
                payload['text'] = t[1]
                #print('paso1'+str(payload))
                # concatnate if tweet is on multiple lines
                payload['text'] = str(payload['text'].replace("\n", ""))
                #print('paso2'+str(payload))
                # remove http:// URL shortening links
                payload['text'] = re.sub(r'http://[\w.]+/+[\w.]+', "", payload['text'], re.IGNORECASE)
                #print('paso3' + str(payload))
                # remove https:// URL shortening links
                payload['text'] = re.sub(r'https://[\w.]+/+[\w.]+', "", payload['text'], re.IGNORECASE)
                #print('paso4' + str(payload))
                # remove certain characters
                payload['text'] = re.sub('[@#\[\]\'"$.;{}~`<>:%&^*()-?_!,+=]', "", payload['text'])
                #print('paso5' + str(payload))
                # try:
                #print('paso6: ' + str(len(payload)))
                if len(payload['text']) > 1:
                    #print('paso7: ')
                    post = requests.post(SentimentURL, data=payload)
                    response = post.json()
                    conn.execute("UPDATE UserTimeline "
                                 "SET tweet_probability_sentiment_positive = ?, "
                                 "tweet_probability_sentiment_neutral = ?, "
                                 "tweet_probability_sentiment_negative = ?, "
                                 "tweet_sentiment = ? WHERE tweet_id = ?",
                                 (response['probability']['neg'],
                                  response['probability']['neutral'],
                                  response['probability']['pos'],
                                  response['label'], id))
                    conn.commit()
            except Exception as e:
                print(e)
                print('paso getSentiment')
                sys.exit(1)


def getWordCounts(WordCloudTweetNo):
    print('Fetching the most commonly used {0} words in the "{1}" feed...'.format(WordCloudTweetNo, ScreenName))
    cur = "DELETE FROM WordsCount;"
    conn.execute(cur)
    conn.commit()
    cur = 'SELECT tweet_text FROM UserTimeline'
    data = conn.execute(cur)
    # StopList = stopwords.words('english')
    StopList = stopwords.words('spanish')
    Lem = WordNetLemmatizer()
    AllWords = ''
    for w in tqdm(data.fetchall(), leave=1):
        try:
            # remove certain characters and strings
            CleanWordList = re.sub(r'http://[\w.]+/+[\w.]+', "", w[0], re.IGNORECASE)
            CleanWordList = re.sub(r'https://[\w.]+/+[\w.]+', "", CleanWordList, re.IGNORECASE)
            CleanWordList = re.sub(r'[@#\[\]\'"$.;{}~`<>:%&^*()-?_!,+=]', "", CleanWordList)
            # tokenize and convert to lower case
            CleanWordList = [words.lower() for words in word_tokenize(CleanWordList) if words not in StopList]
            # lemmatize words
            CleanWordList = [Lem.lemmatize(word) for word in CleanWordList]
            # join words
            CleanWordList = ' '.join(CleanWordList)
            AllWords += CleanWordList
        except Exception as e:
            print(e)
            sys.exit(e)
    if AllWords is not None:
        words = [word for word in AllWords.split()]
        c = Counter(words)
        for word, count in c.most_common(WordCloudTweetNo):
            conn.execute("INSERT INTO WordsCount (word, frequency) VALUES (?,?)", (word, count))
            conn.commit()


# geocode followers where geolocation data stored as part of followers' profiles
def GetFollowersGeoData(StatusesCount, ScreenName, SkipStatus, AppKey, AppSecret):
    print('Acquiring followers for Twitter handle "{0}"...'.format(ScreenName))
    twitter = twython.Twython(AppKey, AppSecret, oauth_version=2)
    ACCESS_TOKEN = twitter.obtain_access_token()
    twitter = twython.Twython(AppKey, access_token=ACCESS_TOKEN)
    params = {'count': StatusesCount, 'screen_name': ScreenName, 'skip_status': 1}
    checkRateLimit(limittypecheck='usertimeline')
    TotalFollowersCount = twitter.get_user_timeline(**params)
    TotalFollowersCount = [tweet['user']['followers_count'] for tweet in TotalFollowersCount][0]

    progressbar = tqdm(total=TotalFollowersCount, leave=1)
    Cursor = -1
    while Cursor != 0:
        checkRateLimit(limittypecheck='followers')
        NewGeoEnabledUsers = twitter.get_followers_list(**params, cursor=Cursor)
        Cursor = NewGeoEnabledUsers['next_cursor']
        progressbar.update(len(NewGeoEnabledUsers['users']))
        NewGeoEnabledUsers = [[user['id'], user['screen_name'], user['location']] for user in
                              NewGeoEnabledUsers['users'] if user['location'] != '']
        # AllGeoEnabledUsers.extend(NewGeoEnabledUsers)
        for user in NewGeoEnabledUsers:
            if user[0] not in UserFollowerIDs:
                conn.execute("INSERT OR IGNORE INTO FollowersGeoData ("
                             "follower_id,"
                             "follower_name,"
                             "follower_location)"
                             "VALUES (?,?,?)",
                             (user[0], user[1], user[2]))
                conn.commit()
    progressbar.close()
    print('')
    print('Geo-coding followers location where location variable provided in the user profile...')
    geo = GoogleV3(timeout=5)
    cur = 'SELECT follower_id, follower_location FROM FollowersGeoData WHERE location_latitude IS NULL OR location_longitude IS NULL'
    data = conn.execute(cur)
    for location in tqdm(data.fetchall(), leave=1):
        try:
            try:
                followerid = location[0]
                # print(location[1])
                geoparams = geo.geocode(location[1])
            except GeocoderQuotaExceeded as e:
                print(e)
                return
                # print(geoparams)
            else:
                if geoparams is None:
                    pass
                else:
                    latitude = geoparams.latitude
                    longitude = geoparams.longitude
                    conn.execute("UPDATE FollowersGeoData "
                                 "SET location_latitude = ?,"
                                 "location_longitude = ?"
                                 "WHERE follower_id = ?",
                                 (latitude, longitude, followerid))
                    conn.commit()
        except Exception as e:
            print("Error: geocode failed on input %s with message %s" % (location[2], e))
            continue


def checkRateLimit(limittypecheck):
    appstatus = {'remaining': 1}
    while True:
        if appstatus['remaining'] > 0:
            twitter = twython.Twython(AppKey, AppSecret, oauth_version=2)
            ACCESS_TOKEN = twitter.obtain_access_token()
            twitter = twython.Twython(AppKey, access_token=ACCESS_TOKEN)
            status = twitter.get_application_rate_limit_status(resources=['statuses', 'application', 'followers'])
            appstatus = status['resources']['application']['/application/rate_limit_status']
            if limittypecheck == 'usertimeline':
                usertimelinestatus = status['resources']['statuses']['/statuses/user_timeline']
                if usertimelinestatus['remaining'] == 0:
                    wait = max(usertimelinestatus['reset'] - time.time(), 0) + 1  # addding 1 second pad
                    time.sleep(wait)
                else:
                    return
            if limittypecheck == 'followers':
                userfollowersstatus = status['resources']['followers']['/followers/list']
                if userfollowersstatus['remaining'] == 0:
                    wait = max(userfollowersstatus['reset'] - time.time(), 0) + 1  # addding 1 second pad
                    time.sleep(wait)
                else:
                    return
        else:
            wait = max(appstatus['reset'] - time.time(), 0) + 1
            time.sleep(wait)


def main():
    getTweets('Casa Blanca', '2019-01-01', '2020-10-01')
    getSentiment(SentimentURL)
    # getWordCounts(WordCloudTweetNo)
    #GetFollowersGeoData(StatusesCount, ScreenName, SkipStatus, AppKey, AppSecret)
    conn.close()


if __name__ == "__main__":
    main()
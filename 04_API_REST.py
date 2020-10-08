import tweepy
import time
import sys
import configparser


#Archivo de tokens a twiiter
config = configparser.ConfigParser()
config.read('params.cfg')
AppKey = config.get('TwitterAuth','AppKey')
AppSecret = config.get('TwitterAuth','AppSecret')
AccesesKey = config.get('TwitterAuth','AccesesKey')
AccesesSecret = config.get('TwitterAuth','AccesesSecret')

#inicialización de variables
WordCloudTweetNo = 40
StatusesCount = 200
SkipStatus = 1
busqueda = ''
FechaInicial = ''
FechaFinal   = ''


def getTweets(busqueda):
    tweets = []
    # Autenticación Twitter
    try:
        auth = tweepy.OAuthHandler(AppKey, AppSecret)
        api = tweepy.API(auth)

        UserFollowerIDs = []

        print('Texto a buscar', busqueda)

        try:
            for tweet in tweepy.Cursor(api.user_timeline, screen_name=busqueda, exclude_replies=True).items():
                print(tweet)
                #for item in tweet:
                print('---------------------------------------------------------')
                print('Datos usuario')
                print('---------------------------------------------------------')
                print('name:', tweet.user.name)
                print('screen_name:', tweet.user.screen_name)
                print('user.description:', tweet.user.description)
                print('user.location:', tweet.user.location)
                print('user.url:', tweet.user.url)
                print('item_user.id:', tweet.user.id)
                print('item.user.created_at', tweet.user.created_at)
                print('---------------------------------------------------------')
                print('user.utc_offset:', tweet.user.utc_offset)
                print('user.friends_count:', tweet.user.friends_count)
                print('user.followers_count:', tweet.user.followers_count)
                print('user.statuses_count:', tweet.user.statuses_count)
                print('---------------------------------------------------------')
                print('Datos tweet')
                print('---------------------------------------------------------')
                print('id:', tweet.id)
                print('created_at:', tweet.created_at)
                print('text:', tweet.text)
                print('---------------------------------------------------------')

        except tweepy.TweepError:
            time.sleep(60)

    except Exception as e:
        print("Error autenticación twiiter" +e)
        sys.exit(e)

def main():
    getTweets('@congresoGuate' )

if __name__ == "__main__":
    main()
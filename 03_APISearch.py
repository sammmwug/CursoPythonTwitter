import tweepy
import twython
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


def getTweets(busqueda , FechaInicial , FechaFinal):
    tweets = []
    # Autenticación Twitter
    try:
        auth = tweepy.OAuthHandler(AppKey, AppSecret)
        api = tweepy.API(auth)

        UserFollowerIDs = []

        print('Texto a buscar', busqueda)
        print('Fecha Inicial', FechaInicial)
        print('Fecha Final', FechaFinal)

        search = tweepy.Cursor( api.search, q = busqueda + '-filter:retweets', include_entities=True , since = FechaInicial, until = FechaFinal ,  result_type="recent").items(320)
        print(search)
        try:
            for item in search:
                print('---------------------------------------------------------')
                print('Datos usuario')
                print('---------------------------------------------------------')
                print('name:', item.user.name)
                print('screen_name:', item.user.screen_name)
                print('user.description:', item.user.description)
                print('user.location:', item.user.location)
                print('user.url:', item.user.url)
                print('item_user.id:', item.user.id)
                print('item.user.created_at', item.user.created_at)
                print('---------------------------------------------------------')
                print('user.utc_offset:', item.user.utc_offset)
                print('user.friends_count:', item.user.friends_count)
                print('user.followers_count:', item.user.followers_count)
                print('user.statuses_count:', item.user.statuses_count)
                print('---------------------------------------------------------')
                print('Datos tweet')
                print('---------------------------------------------------------')
                print('id:', item.id)
                print('created_at:', item.created_at)
                print('text:', item.text)
                print('---------------------------------------------------------')
        except Exception as e:
            print("Error obtener twiiter"+e)
            sys.exit(e)
    except Exception as e:
        print("Error autenticación twiiter" +e)
        sys.exit(e)

def main():
    getTweets('@congresoGuate' , '2020-09-30' , '2020-10-06')

if __name__ == "__main__":
    main()
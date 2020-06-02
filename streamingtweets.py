from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import sqlite3
from textblob import TextBlob
from unidecode import unidecode
import time
import re
from nltk.tokenize import word_tokenize, RegexpTokenizer
from nltk.corpus import stopwords
import string
from geotext import GeoText
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# consumer key, consumer secret, access token, access secret.
consumerKey = ""
consumerSecret = ""
accessToken = ""
accessTokenSecret = ""

conn = sqlite3.connect('db.twitterdata')
c = conn.cursor()


class Listener(StreamListener):
    def processTweet(self, tweet):
        tweet = re.sub(r'\&\w*;', '', tweet)
        tweet = re.sub('@[^\s]+', '', tweet)
        tweet = re.sub(r'\$\w*', '', tweet)
        tweet = tweet.lower()
        tweet = re.sub(r'https?:\/\/.*\/\w*', '', tweet)
        tweet = re.sub(r'#\w*', '', tweet)
        # tweet = re.sub(r'[' + punctuation.replace('@', '') + ']+', ' ', tweet)
        tweet = re.sub(r't\b\w\b', '', tweet)
        tweet = re.sub(r'\s\s+', ' ', tweet)
        tweet = re.sub(r"[-()\"#/@;:<>{}[`+=~*|.!?,.....']", "", tweet)
        tweet = re.sub(r"rt", '', tweet)
        tweet = re.sub(r"1234567890", "", tweet)
        tweet = tweet.lstrip(' ')
        tweet = ''.join(c for c in tweet if c <= '\uFFFF')
        tweet = ''.join(c for c in tweet if not c.isdigit())
        return tweet

    def wordList(self, tweet):
        stop_words = set(stopwords.words('english') + list(string.punctuation))
        word = word_tokenize(tweet.replace('\n', ' '))
        # freq_words = FreqDist(word)
        filtered_word = []
        for w in word:
            if len(w) > 2:
                if w not in stop_words:
                    filtered_word.append(w)
        return filtered_word

    def location(self, userlocation):
        country = None
        city = None
        state = None
        cur = conn.cursor()
        cur.execute('SELECT * FROM worldcities') # You need to have table worldcities information.
        world = cur.fetchall()
        places = GeoText(str(userlocation))
        countries = places.countries
        cities = places.cities
        if len(cities) == 1 and len(countries) == 1:
            city = cities[0]
            country = countries[0]
            for w in world:
                if len(w[0]) != 0 and len(w[2]) != 0 and str(city).find(w[0]) != -1 and str(country).find(w[2]) != -1:
                    state = w[5]
        else:
            if len(countries) == 1:
                country = countries[0]
            if len(cities) == 1:
                city = cities[0]
                population = []
                country_array = []
                state_array = []
                for w in world:
                    if len(w[0]) != 0 and str(cities).find(w[0]) != -1:
                        if w[6] is not None:
                            population.append(int(w[6]))
                        else:
                            population.append(0)
                        country_array.append(w[2])
                        state_array.append(w[5])
                    elif w[5] is not None and str(cities).find(w[5]) != -1:
                        if w[6] is not None:
                            population.append(int(w[6]))
                        else:
                            population.append(0)
                        country_array.append(w[2])
                        state_array.append(w[5])
                if len(population) != 0:
                    p_index = population.index(max(population))
                    country = country_array[p_index]
                    state = state_array[p_index]
                else:
                    if len(country_array) != 0:
                        country = country_array[0]
                    if len(state_array) != 0:
                        state = state_array[0]
            else:
                splited = str(userlocation).lower().strip(',').strip('.').split()
                splited_left = str(userlocation).split(',')[0]
                if 'usa' in splited:
                    country = 'United States'
                if 'uk' in splited:
                    country = 'United Kingdom'
                if 'england' in splited:
                    country = 'United Kingdom'
                    state = 'England'
                if userlocation != splited_left:
                    splited_right = str(userlocation).split(',')[1].strip()
                    cur.execute('SELECT state FROM usa_states WHERE code LIKE ?', (splited_left,))
                    code_right = cur.fetchone()
                    if code_right is not None:
                        state = code_right[0]
                        country = 'United States'
                    cur.execute('SELECT state FROM usa_states WHERE code LIKE ?', (splited_right,))
                    code_left = cur.fetchone()
                    if code_left is not None:
                        state = code_left[0]
                        country = 'United States'
                    cur.execute('SELECT state FROM usa_states WHERE state LIKE ?', (splited_left,))
                    state_right = cur.fetchone()
                    if state_right is not None:
                        state = state_right[0]
                        country = 'United States'
                    cur.execute('SELECT state FROM usa_states WHERE state LIKE ?', (splited_right,))
                    state_left = cur.fetchone()
                    if state_left is not None:
                        state = state_left[0]
                        country = 'United States'

        returnedlist = [country, state, city]
        print(userlocation)
        print(countries)
        print(cities)
        print(returnedlist)
        print("------------------------------")
        return returnedlist

    def on_data(self, data):
        try:
            tweet_frame = json.loads(data)
            user_id = tweet_frame['user']['id']
            tweet = unidecode(tweet_frame['text'])
            user_name = tweet_frame["user"]["name"]
            user_screen_name = tweet_frame["user"]["screen_name"]
            user_followers_count = tweet_frame['user']['followers_count']
            user_verified = tweet_frame['user']['verified']
            user_location = tweet_frame['user']['location']
            created_at = tweet_frame['created_at']
            id_str = tweet_frame['id_str']
            verified_int = 0
            if user_verified:
                verified_int = 1
            else:
                verified_int = 0

            clean_tweet = self.processTweet(tweet)

            if user_name is not None:
                if user_followers_count is not None:
                    if user_location is not None:
                        if user_followers_count > 20:
                            vader_analyzer = SentimentIntensityAnalyzer()
                            vader_polarity = vader_analyzer.polarity_scores(clean_tweet)
                            vader_compound = vader_polarity['compound']

                            textblob_analyzer = TextBlob(clean_tweet)
                            textblob_polarity = textblob_analyzer.sentiment.polarity
                            textblob_subjective = textblob_analyzer.subjectivity

                            insertedlist = self.location(user_location)

                            country = insertedlist[0]
                            state = insertedlist[1]
                            city = insertedlist[2]
                            word_list = ','.join(self.wordList(clean_tweet))

                            c.execute(
                                "INSERT INTO homepage_tweets (user_id, user_name, user_screen_name, user_follower_count, created, verified, location, country, state, city, tweet, clean_tweet, word_list, polarity, subjectivity, vader_compound,id_str) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)",
                                (user_id, user_name, user_screen_name, user_followers_count, created_at, verified_int,
                                 user_location, country, state, city, tweet, clean_tweet, word_list, textblob_polarity,
                                 textblob_subjective, vader_compound, id_str
                                 ))

                            conn.commit()
                        print(user_followers_count)
        except KeyError as e:
            print(str(e))
        return (True)

    def on_error(self, status):
        print(status)

def stream():
    while True:
        try:
            auth = OAuthHandler(consumerKey, consumerSecret)
            auth.set_access_token(accessToken, accessTokenSecret)
            twitterStream = Stream(auth, Listener())
            twitterStream.filter(track=["a", "e", "i", "o", "u"], languages=['en'])
        except Exception as e:
            print(str(e))
            time.sleep(1)
if __name__ == "__main__":
    stream()

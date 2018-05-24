import sqlite3
import json
import html

conn = sqlite3.connect('db.sqlite3');
tweets_file = 'three_minutes_tweets.json.txt'
afinn_dict_file = 'AFINN-111.txt'
columns = ['name', 'tweet_text', 'country_code', 'display_url', 'lang', 'created_at', 'location']


def drop_table():
    """
    drop table
    """
    print("dropping table ...")
    cursor = conn.cursor()
    cursor.execute("""DROP TABLE IF EXISTS Tweet;""")
    conn.commit()
    print("table dropped\n")

def create_table():
    """
    creating table
    """
    print("creating table ...")
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS Tweet(
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        tweet_text TEXT,
                        country_code TEXT,
                        display_url TEXT,
                        lang TEXT,
                        created_at TIMESTAMP,
                        location TEXT);""")
    conn.commit()
    print("table created\n")

def insert_one_row(row):
    """
    insert data
    :param row: список названий столбцов таблицы
    :return:
    """
    name, tweet_text, country_code, display_url, lang, created_at, location = row
    try:
        cursor = conn.cursor()
        query = """INSERT INTO Tweet(
                name, tweet_text, country_code, display_url, lang, created_at, location)
                VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}');"""\
                .format(name, tweet_text, country_code, display_url, lang, created_at, location)
        cursor.execute(query)
    except sqlite3.IntegrityError as error:
        print("Error", error)

def delete_all_data():
    """
        удаление всех данных
    """
    print("deleting all rows ...")
    try:
        cursor = conn.cursor()
        query = """DELETE FROM Tweet"""
        cursor.execute(query)
    except sqlite3.IntegrityError as error:
        print("Error", error)
    conn.commit()
    print("all rows deleted\n")

def clean_data(data):
    return html.escape(data.strip())

def clean_word(word):
    return word.strip().lower()

def load_tweet(file_name):
    """
        read tweets from file and insert those into DB
        :param file_name: название файла с твитами
    """
    print("loading tweets ...")
    with open(file_name, 'r') as file:
        counter = 0
        line = file.readline()
        while file.readline():
            line = file.readline()
            tweet = json.loads(line)

            created_at = tweet.get('created_at')
            if created_at:

                user = tweet.get("user")
                name = user.get("name")
                tweet_text = tweet.get('text')
                place = tweet.get("place")
                country_code = place.get('country_code') if tweet.get("place") else ''
                media = tweet.get('extended_entities').get('media') if tweet.get('extended_entities') else ''
                display_url = media[0].get('display_url','') if media else ''
                lang = user.get('lang')
                location = user.get('location')
                row = (clean_data(name), clean_data(tweet_text), clean_data(country_code), clean_data(display_url), clean_data(lang), created_at, clean_data(location))
                # print(row)
                insert_one_row(row)
                counter+=1
    conn.commit()
    print("file with tweets was read with {} rows\n".format(counter))


def add_column():
    """
        добавление колонки 'tweet_sentiment' в таблицу 'Tweet'
    """
    print("adding tweet_sentiment columns ...")
    cursor = conn.cursor()
    cursor.execute("""ALTER TABLE tweet ADD tweet_sentiment INTEGER DEFAULT 0;""")
    print("columns tweet_sentiment added\n")


def select_data(query = None):
    """
        запрос всех записей таблицы с твитами
    """
    cursor = conn.cursor()
    if query is None:
        for row in cursor.execute("""select * from tweet;"""):
            print(row)
    else:
        for i, row in enumerate(cursor.execute("""{};""".format(query))):
            print(i, row)


def get_afinn_dict(file_name):
    """
        загрузка AFINN словаря
        :param file_name: название словаря
    """
    print("loading {} file ...".format(afinn_dict_file))
    dic = {}
    with open(file_name, 'r') as file:
        for data in file.readlines():
            word, value = data.split('\t')
            dic[word] = int(value)

    print("AFINN file was read with {} rows\n".format(len(dic)))
    return dic

def calculate_tweet_sentiment():
    """
        расчет значений 'sentiment'
    """
    print("calculating tweet_sentiment ...")
    afinn_data = get_afinn_dict(file_name=afinn_dict_file)

    cursor = conn.cursor()
    sentiment_dict = {}

    for rid, tweet_sentiment in cursor.execute("""select id, tweet_text from tweet;"""):
        for word in tweet_sentiment.split(' '):
            word = clean_word(word)
            if word in afinn_data:
                sentiment_dict[rid]=sentiment_dict.get(rid,[])+[afinn_data.get(word)]

    for rid, values in sentiment_dict.items():
        sentiment_dict[rid]=round(sum(values)/len(values))

    print("tweet_sentiment calculated")
    return sentiment_dict

def update_tweet_sentiment():
    """
        обновление значений 'tweet_sentiment'
    """
    print("updating tweet_sentiment ...")
    cursor = conn.cursor()

    sentiment_for_update = calculate_tweet_sentiment()

    for rid, tweet_sentiment in sentiment_for_update.items():
         cursor.execute("""UPDATE Tweet
                              SET tweet_sentiment='{}'
                            WHERE id = '{}';""".format(rid, tweet_sentiment))
    conn.commit()
    print("tweet_sentiment updated on {} rows".format(len(sentiment_for_update)))


def main():
    # удаление таблицы, для проверки
    drop_table()

    # удаление данных, для проверки
    # delete_all_data()

    # создание таблицы
    create_table()

    # загрузка твитов
    load_tweet(file_name=tweets_file)

    # добавление колонки tweet_sentiment
    add_column()


    # расчет значений tweet_sentiment
    update_tweet_sentiment()

    # проверка
#    select_data("select * from tweet where tweet_sentiment != 0")


if __name__ == "__main__":
    main()
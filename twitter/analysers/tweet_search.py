from pyspark.sql.functions import col


def search_by_key_word(df, key_word):
    return df.filter(col('text').contains(key_word))


def search_by_key_words(df, key_words):
    words = [word.strip() for word in key_words.split(',')]

    condition = col('text').contains(words[0])
    for word in words[1:]:
        condition = condition & col('text').contains(word)

    return df.filter(condition)
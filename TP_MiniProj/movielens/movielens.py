from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, array_contains, explode
import tkinter as tk
import customtkinter as ctk

spark = SparkSession.builder.appName("MovieLens").getOrCreate()

movies_df = spark.read.csv(
    './datasets/movies.dat',
    sep='::',
    header=False,
    inferSchema=True
).toDF('MovieID', 'Title', 'Genres')

movies_df = movies_df.withColumn('Genres', split(col('Genres'), '\|'))

users_df = spark.read.csv(
    './datasets/users.dat',
    sep='::',
    header=False,
    inferSchema=True
).toDF('UserID', 'Gender', 'Age', 'Occupation', 'zip-code')

ratings_df = spark.read.csv(
    './datasets/ratings.dat',
    sep='::',
    header=False,
    inferSchema=True
).toDF('UserID', 'MovieID', 'Rating', 'Timestamp')

ratings_df = ratings_df.withColumn('Timestamp', ratings_df['Timestamp'].cast('timestamp'))

movie_ratings_df = movies_df.join(ratings_df, on='MovieID')
master_data = movie_ratings_df.join(users_df, on='UserID')
master_data = master_data.select('MovieID', 'Title', 'Genres', 'UserID', 'Age', 'Gender', 'Occupation', 'Rating')

def find_most_appreciated_movies():
    most_appreciated_movies = master_data.groupBy('MovieID', 'Title') \
        .agg({'Rating': 'avg', 'MovieID': 'count'}) \
        .withColumnRenamed('avg(Rating)', 'avg_rating') \
        .withColumnRenamed('count(MovieID)', 'num_reviews')
    prior_belief = 3.0
    most_appreciated_movies = most_appreciated_movies.withColumn(
        'bayesian_avg',
        (most_appreciated_movies['num_reviews'] * most_appreciated_movies['avg_rating'] + 10 * prior_belief) /
        (most_appreciated_movies['num_reviews'] + 10)
    )
    most_appreciated_movies = most_appreciated_movies.orderBy('bayesian_avg', ascending=False)
    return most_appreciated_movies

def find_least_appreciated_movies():
    least_appreciated_movies = master_data.groupBy('MovieID', 'Title') \
        .agg({'Rating': 'avg', 'MovieID': 'count'}) \
        .withColumnRenamed('avg(Rating)', 'avg_rating') \
        .withColumnRenamed('count(MovieID)', 'num_reviews')
    prior_belief = 3.0
    least_appreciated_movies = least_appreciated_movies.withColumn(
        'bayesian_avg',
        (least_appreciated_movies['num_reviews'] * least_appreciated_movies['avg_rating'] + 10 * prior_belief) /
        (least_appreciated_movies['num_reviews'] + 10)
    )
    least_appreciated_movies = least_appreciated_movies.orderBy('bayesian_avg')
    return least_appreciated_movies

def find_most_appreciated_movies_by_genre(genre):
    most_appreciated_movies_by_genre = master_data.filter(array_contains(col('Genres'), genre)) \
        .groupBy('MovieID', 'Title') \
        .agg({'Rating': 'avg', 'MovieID': 'count'}) \
        .withColumnRenamed('avg(Rating)', 'avg_rating') \
        .withColumnRenamed('count(MovieID)', 'num_reviews')
    prior_belief = 3.0
    most_appreciated_movies_by_genre = most_appreciated_movies_by_genre.withColumn(
        'bayesian_avg',
        (most_appreciated_movies_by_genre['num_reviews'] * most_appreciated_movies_by_genre['avg_rating'] + 10 * prior_belief) /
        (most_appreciated_movies_by_genre['num_reviews'] + 10)
    )
    most_appreciated_movies_by_genre = most_appreciated_movies_by_genre.orderBy('bayesian_avg', ascending=False)
    return most_appreciated_movies_by_genre

def find_least_appreciated_movies_by_genre(genre):
    least_appreciated_movies_by_genre = master_data.filter(array_contains(col('Genres'), genre)) \
        .groupBy('MovieID', 'Title') \
        .agg({'Rating': 'avg', 'MovieID': 'count'}) \
        .withColumnRenamed('avg(Rating)', 'avg_rating') \
        .withColumnRenamed('count(MovieID)', 'num_reviews')
    prior_belief = 3.0
    least_appreciated_movies_by_genre = least_appreciated_movies_by_genre.withColumn(
        'bayesian_avg',
        (least_appreciated_movies_by_genre['num_reviews'] * least_appreciated_movies_by_genre['avg_rating'] + 10 * prior_belief) /
        (least_appreciated_movies_by_genre['num_reviews'] + 10)
    )
    least_appreciated_movies_by_genre = least_appreciated_movies_by_genre.orderBy('bayesian_avg')
    return least_appreciated_movies_by_genre

def find_genres_of_most_appreciated_movies():
    exploded_genres = master_data.withColumn("Genre", explode(col("Genres")))
    genres_of_most_appreciated_movies = exploded_genres.groupBy('Genre').agg({'Rating': 'avg'}).orderBy('avg(Rating)', ascending=False)
    return genres_of_most_appreciated_movies

def recommend_movies_for_user(user_id):
    user_ratings = master_data.filter(col('UserID') == user_id)
    user_avg_rating = user_ratings.agg({'Rating': 'avg'}).collect()[0][0]
    recommended_movies = master_data.filter(col('Rating') >= user_avg_rating).select('MovieID', 'Title')
    return recommended_movies

def get_username_by_id(user_id):
    return users_df.filter(col('UserID') == user_id).select

def does_user_exist(user_id):
    return users_df.filter(col('UserID') == user_id).count() > 0

def does_genre_exist(genre):
    return movies_df.filter(array_contains(col('Genres'), genre)).count() > 0

def on_most_appreciated_by_genre_click(genre_textbox, text_box):
    genre = genre_textbox.get("1.0", tk.END).strip()
    if does_genre_exist(genre):
        text_box.delete('1.0', tk.END)
        result_data = find_most_appreciated_movies_by_genre(genre).limit(10).collect()
        text_box.insert(tk.END, '\n'.join([str(row) for row in result_data]))
    else:
        available_genres = [row.Genres for row in movies_df.select('Genres').distinct().collect()]
        text_box.delete('1.0', tk.END)
        text_box.insert(tk.END, f"Genre '{genre}' does not exist. Available genres: {', '.join(available_genres)}")

def on_recommend_click(user_textbox, text_box):
    user_id = user_textbox.get("1.0", tk.END).strip()
    if does_user_exist(user_id):
        text_box.delete('1.0', tk.END)
        result_data = recommend_movies_for_user(user_id).limit(10).collect()
        text_box.insert(tk.END, '\n'.join([str(row) for row in result_data]))
    else:
        text_box.delete('1.0', tk.END)
        text_box.insert(tk.END, f"User ID '{user_id}' does not exist.")

def on_least_appreciated_by_genre_click(genre_textbox, text_box):
    genre = genre_textbox.get("1.0", tk.END).strip()
    if does_genre_exist(genre):
        text_box.delete('1.0', tk.END)
        result_data = find_least_appreciated_movies_by_genre(genre).limit(10).collect()
        text_box.insert(tk.END, '\n'.join([str(row) for row in result_data]))
    else:
        available_genres = [row.Genres for row in movies_df.select('Genres').distinct().collect()]
        text_box.delete('1.0', tk.END)
        text_box.insert(tk.END, f"Genre '{genre}' does not exist. Available genres: {', '.join(available_genres)}")

def main():
    root = ctk.CTk()
    text_box = ctk.CTkTextbox(root, width=600, height=250)
    text_box.pack()

    pady_value = 5

    button_most_appreciated_movies = ctk.CTkButton(root, text="Most appreciated movies", command=lambda: [text_box.delete('1.0', tk.END), text_box.insert(tk.END, '\n'.join([str(row) for row in find_most_appreciated_movies().limit(10).collect()]))])
    button_most_appreciated_movies.pack(pady=pady_value)

    button_least_appreciated_movies = ctk.CTkButton(root, text="Least appreciated movies", command=lambda: [text_box.delete('1.0', tk.END), text_box.insert(tk.END, '\n'.join([str(row) for row in find_least_appreciated_movies().limit(10).collect()]))])
    button_least_appreciated_movies.pack(pady=pady_value)

    genre_textbox = ctk.CTkTextbox(root, width=100, height=10)
    genre_textbox.pack()
    genre_textbox.insert(tk.END, 'Comedy')

    button_most_appreciated_by_genre = ctk.CTkButton(root, text="Most appreciated movies by genre", command=lambda: on_most_appreciated_by_genre_click(genre_textbox, text_box))
    button_most_appreciated_by_genre.pack(pady=pady_value)

    button_least_appreciated_by_genre = ctk.CTkButton(root, text="Most appreciated movies by genre", command=lambda: on_least_appreciated_by_genre_click(genre_textbox, text_box))
    button_least_appreciated_by_genre.pack(pady=pady_value)

    button_most_appreciated_genres = ctk.CTkButton(root, text="Genres of most appreciated movies", command=lambda: [text_box.delete('1.0', tk.END), text_box.insert(tk.END, '\n'.join([str(row) for row in find_genres_of_most_appreciated_movies().limit(10).collect()]))])
    button_most_appreciated_genres.pack(pady=pady_value)

    user_textbox = ctk.CTkTextbox(root, width=50, height=10)
    user_textbox.pack()
    user_textbox.insert(tk.END, '1')

    button_recommend = ctk.CTkButton(root, text="Recommend movies for user", command=lambda: on_recommend_click(user_textbox, text_box))
    button_recommend.pack(pady=pady_value)

    root.mainloop()

if __name__ == "__main__":
    main()
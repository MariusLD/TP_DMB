import time
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

def get_player_names_and_kills(df):
    return df.select("player_name", "player_kills").distinct().collect()

def get_mean_kills_per_player(df):
    return df.rdd.map(lambda x: (x[11], (x[10], 1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0] / x[1][1], x[1][1])).collect()

def get_best_10_players(df):
    return df.rdd.map(lambda x: (x[11], (x[10], 1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0] / x[1][1])).sortBy(lambda x: x[1], ascending=False).take(10)

def players_that_played_at_least_4_games(df):
    return df.rdd.map(lambda x: (x[11], 1)).reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] >= 4).collect()  

def remove_outliers(df):
    return df.filter((df.player_name != "") | (df.player_dist_walk > 0))

def score_by_rank(rank):
    return 1000 - ((rank-1) * 10)

def get_player_score(df):
    df = df.withColumn("player_assists", df.player_assists.cast(IntegerType()))
    df = df.withColumn("player_dmg", df.player_dmg.cast(IntegerType()))
    df = df.withColumn("team_placement", df.team_placement.cast(IntegerType()))
    #return df.rdd.map(lambda x: (x[11], score_by_rank(x[14])+x[5]*50+x[10]*100+x[9])).collect()
    return df.rdd.map(lambda x: (x[11], score_by_rank(x[14])+x[5]*50+x[10]*100+x[9])).reduceByKey(lambda x, y: x + y).collect()

def loop_pubg(df):
    start = time.clock() 
    for _ in range(3):
        get_player_names_and_kills(df)
        get_best_10_players(df)
    return time.clock() - start

def main():
    spark = SparkSession.builder.appName("PUBG").getOrCreate()
    df = spark.read.csv("agg_match_stats_0.csv", header=True)
    df = df.withColumn("player_kills", df.player_kills.cast(IntegerType()))
    
    #print(get_player_names_and_kills(df))

    #print(get_mean_kills_per_player(df))

    #print(get_best_10_players(df))

    #print(players_that_played_at_least_4_games(df))

    #print(remove_outliers(df))
    
    #print(get_player_score(df))

    print("Temps d\'exécution non persistant : ",   loop_pubg(df))

    df.persist(StorageLevel.MEMORY_ONLY)

    print("Temps d\'exécution persistant : ",   loop_pubg(df))

if __name__ == "__main__":
    main()
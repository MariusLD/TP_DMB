from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
import networkx as nx
import matplotlib.pyplot as plt
from graphframes import *
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.sql.functions import lit

class Station:
    def __init__(self, station_id, name, latitude, longitude):
        self.id = station_id
        self.name = name
        self.latitude = latitude
        self.longitude = longitude

class Trip:
    def __init__(self, ride_id, rideable_type, started_at, ended_at, start_station, end_station, member_casual):
        self.ride_id = ride_id
        self.rideable_type = rideable_type
        self.started_at = started_at
        self.ended_at = ended_at
        self.start_station = start_station
        self.end_station = end_station
        self.member_casual = member_casual

def date_to_milliseconds(date_str):
    if " " in date_str:
        format_str = "%Y-%m-%d %H:%M:%S"
    else:
        format_str = "%d-%m-%Y"
    dt = datetime.strptime(date_str, format_str)
    epoch = datetime.utcfromtimestamp(0)
    milliseconds = (dt - epoch).total_seconds() * 1000.0
    return int(milliseconds)

def nearest_station_time(graph, station_id):
    edges_rdd = graph.edges.rdd
    edges_with_time_rdd = edges_rdd.map(lambda row: Row(
        src=row['src'],
        dst=row['dst'],
        id=row['id'],
        time=date_to_milliseconds(row['ended_at']) - date_to_milliseconds(row['started_at'])
    ))
    edges_with_time = edges_with_time_rdd.toDF()
    edges_from_station = edges_with_time.filter((col("src") == station_id) | (col("dst") == station_id))
    min_times = edges_from_station.groupBy("dst").agg({"time": "min"})
    min_times = min_times.filter(col("dst") != station_id)
    nearest_edge = min_times.orderBy("min(time)").first()
    print(f"The nearest station from {station_id} in terms of time is {nearest_edge['dst']}.")

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371
    return c * r

haversine_udf = udf(haversine, FloatType())

def nearest_station_distance(graph, station_id):
    station = graph.vertices.filter(col("id") == station_id).first()
    station_lon = float(station['longitude'])
    station_lat = float(station['latitude'])
    vertices_with_distance = graph.vertices.withColumn("distance", haversine_udf(lit(station_lon), lit(station_lat), col("longitude").cast("float"), col("latitude").cast("float")))
    vertices_with_distance = vertices_with_distance.filter(col("id") != station_id)
    nearest_station = vertices_with_distance.orderBy("distance").first()
    print(f"The nearest station from {station_id} in terms of distance is {nearest_station['id']}.")

def get_subgraph(start_date, end_date, graph):
    start_ms = date_to_milliseconds(start_date)
    end_ms = date_to_milliseconds(end_date)
    edges = graph.edges.rdd.map(lambda row: Row(
        src=row['src'],
        dst=row['dst'],
        id=row['id'],
        started_at=date_to_milliseconds(row['started_at']),
        ended_at=date_to_milliseconds(row['ended_at'])
    )).toDF()
    sub_edges = edges.filter((col("started_at") >= start_ms) & (col("ended_at") <= end_ms))
    subgraph = GraphFrame(graph.vertices, sub_edges)
    return subgraph

def compute_top_stations(graph):
    incoming_trips = graph.edges.groupBy("dst").count().orderBy('count', ascending=False)
    outgoing_trips = graph.edges.groupBy("src").count().orderBy('count', ascending=False)
    print("Top 10 stations for incoming trips:")
    incoming_trips.show(10)
    print("Top 10 stations for outgoing trips:")
    outgoing_trips.show(10)

spark = SparkSession.builder.appName("Spark GraphFrames").enableHiveSupport().getOrCreate()

# Columns : ride_id (0), rideable_type (1), started_at (2), ended_at (3), start_station_name (4), start_station_id (5), end_station_name (6), end_station_id (7), start_lat (8), start_lng (9), end_lat (10), end_lng (11), member_casual (12)
df = spark.read.csv("JC-202112-citibike-tripdata.csv", header=True)

start_stations = df.select(
    col("start_station_id").alias("id"),
    col("start_station_name").alias("name"),
    col("start_lat").alias("latitude"),
    col("start_lng").alias("longitude")
)

start_stations = start_stations.dropna(subset=["id", "name"])

end_stations = df.select(
    col("end_station_id").alias("id"),
    col("end_station_name").alias("name"),
    col("end_lat").alias("latitude"),
    col("end_lng").alias("longitude")
)

end_stations = end_stations.dropna(subset=["id", "name"])

station_vertices = start_stations.union(end_stations).dropDuplicates(["id"])

trip_edges = df.select(
    col("ride_id").alias("id"),
    col("rideable_type"),
    col("started_at"),
    col("ended_at"),
    col("start_station_id").alias("src"),
    col("end_station_id").alias("dst")
)

trip_edges = trip_edges.dropna(subset=["src", "dst"])

graph = GraphFrame(station_vertices, trip_edges)

def PlotGraph(edge_list, vertices):
    Gplot = nx.Graph()
    id_to_name = {row['id']: row['name'] for row in vertices.select('id', 'name').collect()}
    for row in edge_list.select('src', 'dst', 'id').collect():
        Gplot.add_edge(id_to_name.get(row['src'], 'unknown'), id_to_name.get(row['dst'], 'unknown'), label=row['id'])
    pos = nx.spring_layout(Gplot)
    nx.draw(Gplot, pos, with_labels=True, font_weight='bold', node_size=700, node_color='skyblue', font_size=10, font_color='black')
    labels = nx.get_edge_attributes(Gplot, 'label')
    nx.draw_networkx_edge_labels(Gplot, pos, edge_labels=labels)
    plt.show()

# Question 2.1
#graph = get_subgraph("05-12-2021", "25-12-2021", graph)
#PlotGraph(graph.edges, graph.vertices)
    
# Question 1.2
#PlotGraph(graph.edges, graph.vertices)
    
# Question 2.2
#compute_top_stations(graph)

# Question 3.1
#nearest_station_distance(graph, "JC013")

# Question 3.2
#nearest_station_time(graph, "JC013")
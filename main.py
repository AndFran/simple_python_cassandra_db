from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()

# create the keyspace
session.execute("""
            CREATE KEYSPACE IF NOT EXISTS book_shelf
            WITH REPLICATION =
            { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """)

session.set_keyspace('book_shelf')

query = """
CREATE TABLE IF NOT EXISTS books(
year INT,
author_name TEXT,
title TEXT,
PRIMARY KEY(year, author_name)
);
"""

# createt the table
session.execute(query)

query = """
INSERT INTO books(year, author_name, title) VALUES (%s, %s, %s)
"""

# insert the data 
session.execute(query, (2023, "Marian Montagnino", "Building Modern CLI Applications in Go"))
session.execute(query, (2023, "Fabio Nelli", "Parallel and High Performance Programming with Python"))

# query the data
results = session.execute("SELECT * FROM books WHERE year=2023")
for result in results:
  print(result.year, result.author_name, result.title)

  
session.shutdown()
cluster.shutdown()    



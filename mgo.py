#!/usr/bin/env python

# https://c-catalog.mgo-images.com/catalog/catalog/content/MMVC6056BC7A925AB99634588D167B8D7701/crew
# https://c-catalog.mgo-images.com/catalog/catalog/content/MMVC6056BC7A925AB99634588D167B8D7701/cast

from time import sleep
from random import random
import requests
import sqlite3
import json

MAX_RETRIES = 3

GENRES = ["ACTION & ADVENTURE", "ANIMATED", "COMEDY", "DOCUMENTARY", "DRAMA", "FAMILY", "FOREIGN", "HORROR", "INDIE", "MUSICAL", "MYSTERY & SUSPENSE", "ROMANCE", "SCI-FI & FANTASY", "WESTERN"]

db_connection = sqlite3.connect("mgo.db")

genre_cache = {}
db_cursor = db_connection.cursor()
db_cursor.execute("SELECT id, name FROM genres")
for row in db_cursor.fetchall():
  genre_cache[row[1]] = row[0]

for genre in GENRES:
  payload = {
    "providers": [{
      "types": [{
        "type": "MOVIE"
      }],
      "name": "DIGITALSMITHS",
      "id": None
    }],
    "searchQuery": {
      "terms": [{
        "field": "GENRES",
        "value": genre,
        "operator": "AND"
      }, {
        "value": "*:*",
        "field": "TERMS",
        "operator": "AND"
      }, {
        "value": "true",
        "field": "MEDIA_NAVI_OFFERS",
        "operator": "AND"
      }]
    },
    "startIndex": 0,
    "count": 10000
  }
  params = {
    "include": "hitCount,hits,id,title"  # description
  }
  response = requests.put("https://www.mgo.com/core/searchreco/search", params=params, data=json.dumps(payload))
  assert response.status_code == 200
  result = response.json()
  assert len(result["hits"]) == result["hitCount"]  # Make sure we got all results as one page
  
  print "===== %s =====" % genre
  for result in result["hits"]:
    db_cursor = db_connection.cursor()
    db_cursor.execute("SELECT 1 FROM movies WHERE mgo_id = ?", (result["id"], ))
    if db_cursor.fetchone() is not None:
      continue
    print "[%s] %s" % (result["id"], result["title"])
    
    response = requests.get("https://c-catalog.mgo-images.com/catalog/catalog/content/%s/detail" % result["id"])
    assert response.status_code == 200
    info = response.json()
    assert info["id"] == result["id"]
    assert info["mgoId"] == info["id"]
    
    with db_connection:
      db_cursor = db_connection.cursor()
      db_cursor.execute("INSERT OR REPLACE INTO movies ('mgo_id', 'title', 'description', 'release_year', 'rotten_tomatoes_id') VALUES (?, ?, ?, ?, ?)", (info["id"], info["title"], info["description"], int(info["origReleaseDate"][:4]) if info["origReleaseDate"] else None, int(info["rotten_tomatoes_id"]) if info.has_key("rotten_tomatoes_id") else None))
      movie_id = db_cursor.lastrowid
      assert movie_id is not None
      
      for item in info["genres"]:
        genre_name = item["displayName"]
        genre_id = genre_cache.get(genre_name)
        if genre_id is None:
          db_cursor = db_connection.cursor()
          db_cursor.execute("INSERT INTO genres ('name') VALUES (?)", (genre_name, ))
          genre_id = db_cursor.lastrowid
          assert genre_id is not None
          genre_cache[genre_name] = genre_id
        db_cursor = db_connection.cursor()
        db_cursor.execute("INSERT OR IGNORE INTO movies_genres ('movie_id', 'genre_id') VALUES (?, ?)", (movie_id, genre_id))
      
      for i in xrange(1, MAX_RETRIES):
        response = requests.get("https://www.mgo.com/security/explore/content/%s/offers" % result["id"])
        if response.status_code != 400:
          break
        print "  (Retrying fetching offers...)"
        sleep(i * random())
      if response.status_code == 400:
        print "  <SKIP>"
        db_connection.rollback()
      else:
        assert response.status_code == 200
        offers = response.json()
        assert len(offers["offers"])
        for offer in offers["offers"]:
          db_cursor = db_connection.cursor()
          db_cursor.execute("INSERT OR REPLACE INTO offers ('movie_id', 'sku', 'type', 'definition', 'price') VALUES (?, ?, ?, ?, ?)", (movie_id, offer["sku"], offer["acquisitionType"], offer["definition"], offer["purchasePrice"]["price"]))
        
        db_connection.commit()

db_connection.close()

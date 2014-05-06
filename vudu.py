#!/usr/bin/env python

from time import sleep
import requests
import sqlite3
import json

JSON_SECURE_PREFIX = "/*-secure-"
JSON_SECURE_SUFFIX = "*/"
COUNT = 100  # Seems to be the maximum acceptable

db_connection = sqlite3.connect("vudu.db")

genre_cache = {}
db_cursor = db_connection.cursor()
db_cursor.execute("SELECT id, name FROM genres")
for row in db_cursor.fetchall():
  genre_cache[row[1]] = row[0]

offset = 0
while True:
  print "===== %i =====" % offset
  
  url = "http://apicache.vudu.com/api2/claimedAppId/myvudu/format/application*2Fjson/_type/contentSearch/count/%i/dimensionality/any/offset/%i/sortBy/title/superType/movies/type/program/type/bundle" % (COUNT, offset)  # /followup/ratingsSummaries/followup/promoTags
  response = requests.get(url)
  assert response.status_code == 200
  data = response.text
  assert data.startswith(JSON_SECURE_PREFIX) and data.endswith(JSON_SECURE_SUFFIX)
  results = json.loads(data[len(JSON_SECURE_PREFIX):-len(JSON_SECURE_SUFFIX)])
  
  for result in results["content"]:
    db_cursor = db_connection.cursor()
    db_cursor.execute("SELECT 1 FROM movies WHERE content_id = ?", (result["contentId"][0], ))
    if db_cursor.fetchone() is not None:
      continue
    print "[%s] %s" % (int(result["contentId"][0]), result["title"][0])
    
    url = "http://apicache.vudu.com/api2/claimedAppId/myvudu/format/application*2Fjson/_type/contentSearch/contentId/%s/dimensionality/any/followup/usefulStreamableOffers/followup/genres" % result["contentId"][0]  # /followup/credits/followup/ultraVioletability/followup/promoTags/followup/subtitleTrack
    response = requests.get(url)
    data = response.text
    assert response.status_code == 200
    assert data.startswith(JSON_SECURE_PREFIX) and data.endswith(JSON_SECURE_SUFFIX)
    info = json.loads(data[len(JSON_SECURE_PREFIX):-len(JSON_SECURE_SUFFIX)])
    
    with db_connection:
      db_cursor = db_connection.cursor()
      db_cursor.execute("INSERT OR REPLACE INTO movies ('content_id', 'title', 'description', 'release_year', 'country', 'language') VALUES (?, ?, ?, ?, ?, ?)", (int(result["contentId"][0]), result["title"][0], result["description"][0], int(result["releaseTime"][0][:4]) if result.has_key("releaseTime") else None, result["country"][0] if result.has_key("country") else None, result["language"][0] if result.has_key("language") else None))
      movie_id = db_cursor.lastrowid
      assert movie_id is not None
      
      for genre in info["content"][0]["genres"][0]["genre"]:
        genre_name = genre["name"][0]
        genre_id = genre_cache.get(genre_name)
        if genre_id is None:
          db_cursor = db_connection.cursor()
          db_cursor.execute("INSERT INTO genres ('name') VALUES (?)", (genre_name, ))
          genre_id = db_cursor.lastrowid
          assert genre_id is not None
          genre_cache[genre_name] = genre_id
        db_cursor = db_connection.cursor()
        db_cursor.execute("INSERT OR IGNORE INTO movies_genres ('movie_id', 'genre_id') VALUES (?, ?)", (movie_id, genre_id))
      
      for variant in info["content"][0]["contentVariants"][0]["contentVariant"]:
        if variant["offers"][0].has_key("offer"):
          for offer in variant["offers"][0]["offer"]:
            db_cursor = db_connection.cursor()
            db_cursor.execute("INSERT OR REPLACE INTO offers ('movie_id', 'sku', 'type', 'definition', 'price') VALUES (?, ?, ?, ?, ?)", (movie_id, offer["offerId"][0], offer["offerType"][0], variant["videoQuality"][0], float(offer["price"][0])))
      
      db_connection.commit()
  
  if results["moreBelow"][0] != "true":
    break
  offset += COUNT

db_connection.close()

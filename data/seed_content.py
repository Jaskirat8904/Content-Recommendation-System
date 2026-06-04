import pandas as pd
import random

GENRES = ["Action", "Comedy", "Drama", "Thriller", "Romance",
          "SciFi", "Horror", "Documentary", "Animation", "Crime"]

LANGUAGES = ["Hindi", "English", "Tamil", "Telugu", "Kannada"]

CONTENT_TYPES = ["Movie", "Series", "Short"]

rows = []
for i in range(1, 501):
    rows.append({
        "content_id": i,
        "content_name": f"Title_{i}",
        "genre": random.choice(GENRES),
        "language": random.choice(LANGUAGES),
        "content_type": random.choice(CONTENT_TYPES),
        "release_year": random.randint(2015, 2026),
        "avg_rating": round(random.uniform(3.0, 9.5), 1),
        "duration_mins": random.randint(20, 180),
        "cast": f"Actor_{random.randint(1,50)}, Actor_{random.randint(51,100)}",
        "director": f"Director_{random.randint(1,30)}",
    })

df = pd.DataFrame(rows)
df.to_csv("data/content_catalog.csv", index=False)
print(f"Generated {len(df)} content items -> data/content_catalog.csv")
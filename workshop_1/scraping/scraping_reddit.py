import requests
import json
import time

# Configuración
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

SUBREDDIT = "musicsuggestions"  # se puede poner lo que sea
POST_LIMIT = 30
COMMENTS_LIMIT = 5

KEYWORDS = ["album", "song", "music", "artist", "spotify"]

# URL base
url = f"https://api.reddit.com/r/{SUBREDDIT}"

response = requests.get(url, headers=HEADERS)

if response.status_code != 200:
    print("Error:", response.status_code)
    print(response.text)
    exit()

data = response.json()

results = []

for post in data["data"]["children"]:
    title = post["data"]["title"]
    title_lower = title.lower()

    # 🔍 FILTRO por palabras clave
    #if not any(k in title_lower for k in KEYWORDS):
    #    continue

    post_id = post["data"]["id"]

    # URL de comentarios
    comments_url = f"https://www.reddit.com/comments/{post_id}.json"

    comments_response = requests.get(comments_url, headers=HEADERS)

    if comments_response.status_code != 200:
        continue

    comments_json = comments_response.json()

    comments_list = []

    try:
        for c in comments_json[1]["data"]["children"]:
            if "body" in c["data"]:
                comment = c["data"]["body"]

                # evitar comentarios eliminados
                if comment not in ["[deleted]", "[removed]"]:
                    comments_list.append(comment)

            if len(comments_list) >= COMMENTS_LIMIT:
                break
    except:
        continue

    if len(comments_list) == 0:
        continue

    results.append({
        "title": title,
        "score": post["data"]["score"],
        "comments": comments_list
    })

    # cantidad de cuantos registros
    if len(results) >= 30:
        break

    time.sleep(1) 

# Se guarda en la carpeta DATA
with open("workshop_1/data/reddit_music_opinions.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=4, ensure_ascii=False)

print(f"Listo {len(results)} registros guardados")

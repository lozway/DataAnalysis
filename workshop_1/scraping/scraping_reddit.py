import requests
from bs4 import BeautifulSoup
import json
import time
import os
import re
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

BASE_URL = "https://old.reddit.com"
COMMENTS_LIMIT = 5
POSTS_PER_SUBREDDIT = 35

# Run from project root: poetry run python workshop_1/scraping/scraping_reddit.py
OUTPUT_DIR = "datalake_bronze/reddit"

# Posts that signal genuine reactions/opinions about music
REACTION_KEYWORDS = [
    "[fresh",                           # [FRESH ALBUM], [FRESH TRACK], [FRESH VIDEO]
    "thoughts", "opinion", "reaction",
    "first listen", "first impression",
    "what do you think", "how do you feel",
    "rate", "rating", "rated", "ranked", "ranking",
    "aoty", "album of the year",
    "underrated", "overrated", "disappointing", "masterpiece",
    "classic", "favorite", "favourite", "best", "worst", "slept on",
    "album", "ep", "mixtape", "discography",
    "unpopular opinion", "hot take",
]

# Flair prefixes like [FRESH ALBUM], [DISCUSSION], [HYPE], etc.
FLAIR_PREFIX_RE = re.compile(r"^\[.*?\]\s*", re.IGNORECASE)

SUBREDDITS = [
    {"name": "indieheads",  "url": f"{BASE_URL}/r/indieheads/"},
    {"name": "hiphopheads", "url": f"{BASE_URL}/r/hiphopheads/"},
]


def is_reaction_post(title: str) -> bool:
    title_lower = title.lower()
    return any(kw in title_lower for kw in REACTION_KEYWORDS)


def clean_title(title: str) -> str:
    """Removes flair prefixes like [FRESH ALBUM], [DISCUSSION], etc."""
    return FLAIR_PREFIX_RE.sub("", title).strip()


def parse_posts(soup) -> list:
    posts = []
    for thing in soup.select("div.thing.link"):
        title_tag = thing.select_one("a.title")
        if not title_tag:
            continue
        raw_title = title_tag.get_text(strip=True)
        permalink = thing.get("data-permalink", "")
        try:
            score = int(thing.get("data-score", 0))
        except ValueError:
            score = 0
        if raw_title and permalink:
            posts.append({
                "raw_title": raw_title,
                "permalink": permalink,
                "score":     score,
            })
    return posts


def get_next_url(soup) -> str | None:
    btn = soup.select_one("span.next-button a")
    return btn["href"] if btn else None


def fetch_comments(permalink: str) -> list:
    """Scrapes top-level comments from a post page."""
    url = BASE_URL + permalink
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")

        comment_area = soup.select_one("div.commentarea > div.sitetable")
        if not comment_area:
            return []

        comments = []
        for comment_div in comment_area.select(":scope > div.thing.comment"):
            body = comment_div.select_one(".usertext-body .md")
            if not body:
                continue
            text = body.get_text(separator=" ", strip=True)
            if text and text not in ("[deleted]", "[removed]"):
                comments.append(text)
            if len(comments) >= COMMENTS_LIMIT:
                break
        return comments
    except Exception as exc:
        print(f"    ⚠ Error obteniendo comentarios: {exc}")
        return []


def fetch_subreddit_posts(subreddit: dict, target: int = POSTS_PER_SUBREDDIT) -> list:
    results = []
    current_url = subreddit["url"]

    print(f"\n▶ Scraping r/{subreddit['name']}")

    while len(results) < target and current_url:
        try:
            resp = requests.get(current_url, headers=HEADERS, timeout=15)
        except Exception as exc:
            print(f"  ⚠ Error de red: {exc}")
            break

        if resp.status_code != 200:
            print(f"  ⚠ HTTP {resp.status_code} — deteniendo")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        posts = parse_posts(soup)

        for post in posts:
            if len(results) >= target:
                break
            if not is_reaction_post(post["raw_title"]):
                continue

            comments = fetch_comments(post["permalink"])
            time.sleep(1.5)

            if not comments:
                continue

            results.append({
                "title":    clean_title(post["raw_title"]),
                "score":    post["score"],
                "comments": comments,
            })
            print(f"  [{len(results):>2}/{target}] {results[-1]['title'][:70]}")

        current_url = get_next_url(soup)
        if current_url:
            time.sleep(2)

    print(f"  → {len(results)} registros obtenidos")
    return results


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_results = []

    for subreddit in SUBREDDITS:
        posts = fetch_subreddit_posts(subreddit, target=POSTS_PER_SUBREDDIT)
        all_results.extend(posts)

    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(OUTPUT_DIR, f"reddit_music_opinions_{timestamp}.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=4, ensure_ascii=False)

    print(f"\n✅ {len(all_results)} registros totales guardados en {output_path}")


if __name__ == "__main__":
    main()

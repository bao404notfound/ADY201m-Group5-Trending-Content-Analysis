import time
from pathlib import Path

import polars as pl
import requests


# ============================================================
# CONFIG
# ============================================================

NON_TRENDING_FILE = Path(
    "data/raw/youtube/non_trending/non_trending_raw.parquet"
)

TRENDING_FILE = Path(
    "data/raw/youtube/trending/trending_raw.parquet"
)

OUTPUT_FILE = Path(
    "data/raw/youtube/dislikes/dislikes_raw.parquet"
)

CHECKPOINT_EVERY = 100

REQUEST_TIMEOUT = 15

# ~100 requests/minute
SLEEP_SECONDS = 0.65

MAX_RETRIES = 3

API_URL = "https://returnyoutubedislikeapi.com/Votes"


# ============================================================
# LOAD VIDEO IDS
# ============================================================

def load_video_ids():
    print("=" * 70)
    print("LOADING VIDEO IDS")
    print("=" * 70)

    non_trending = (
        pl.read_parquet(NON_TRENDING_FILE)
        .select("video_id")
    )

    trending = (
        pl.read_parquet(TRENDING_FILE)
        .select("video_id")
    )

    video_ids = (
        pl.concat([non_trending, trending])
        .unique()
        .get_column("video_id")
        .to_list()
    )

    print(f"Non-trending rows : {non_trending.height}")
    print(f"Trending rows     : {trending.height}")
    print(f"Unique video IDs  : {len(video_ids)}")

    return video_ids


# ============================================================
# LOAD EXISTING CHECKPOINT
# ============================================================

def load_existing():
    if not OUTPUT_FILE.exists():
        return [], set()

    print("\n[CHECKPOINT FOUND]")
    print(f"File: {OUTPUT_FILE}")

    df = pl.read_parquet(OUTPUT_FILE)

    existing_ids = set(
        df.get_column("video_id").to_list()
    )

    print(f"Already crawled: {len(existing_ids)}")

    return df.to_dicts(), existing_ids


# ============================================================
# FETCH ONE VIDEO
# ============================================================

def fetch_dislike(video_id, session):

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = session.get(
                API_URL,
                params={"videoId": video_id},
                timeout=REQUEST_TIMEOUT,
            )

            # ------------------------------------------------
            # SUCCESS
            # ------------------------------------------------

            if response.status_code == 200:

                data = response.json()

                return {
                    "video_id": video_id,
                    "likes": data.get("likes"),
                    "dislikes": data.get("dislikes"),
                    "raw_likes": data.get("rawLikes"),
                    "raw_dislikes": data.get("rawDislikes"),
                    "rating": data.get("rating"),
                    "view_count": data.get("viewCount"),
                    "date_created": data.get("dateCreated"),
                    "deleted": data.get("deleted"),
                    "source": "return_youtube_dislike",
                }

            # ------------------------------------------------
            # NOT FOUND
            # ------------------------------------------------

            elif response.status_code == 404:

                print(f"[404] {video_id}")

                return {
                    "video_id": video_id,
                    "likes": None,
                    "dislikes": None,
                    "raw_likes": None,
                    "raw_dislikes": None,
                    "rating": None,
                    "view_count": None,
                    "date_created": None,
                    "deleted": True,
                    "source": "return_youtube_dislike",
                }

            # ------------------------------------------------
            # RATE LIMIT
            # ------------------------------------------------

            elif response.status_code == 429:

                wait_time = 30 * attempt

                print(
                    f"[429] Rate limited. "
                    f"Waiting {wait_time}s..."
                )

                time.sleep(wait_time)

            # ------------------------------------------------
            # OTHER HTTP ERROR
            # ------------------------------------------------

            else:

                print(
                    f"[HTTP {response.status_code}] "
                    f"{video_id}"
                )

                time.sleep(5 * attempt)

        except requests.RequestException as e:

            print(
                f"[NETWORK ERROR] {video_id} "
                f"(attempt {attempt}/{MAX_RETRIES}): {e}"
            )

            time.sleep(5 * attempt)

    return None


# ============================================================
# SAVE CHECKPOINT
# ============================================================

def save_checkpoint(rows):

    if not rows:
        return

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    df = pl.DataFrame(rows)

    temp_file = OUTPUT_FILE.with_suffix(
        ".tmp.parquet"
    )

    df.write_parquet(
        temp_file,
        compression="zstd"
    )

    temp_file.replace(OUTPUT_FILE)

    print(
        f"[CHECKPOINT SAVED] "
        f"{len(rows)} total rows"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    # --------------------------------------------------------
    # LOAD INPUT
    # --------------------------------------------------------

    video_ids = load_video_ids()

    # --------------------------------------------------------
    # LOAD CHECKPOINT
    # --------------------------------------------------------

    rows, existing_ids = load_existing()

    # --------------------------------------------------------
    # FIND REMAINING IDS
    # --------------------------------------------------------

    remaining_ids = [
        video_id
        for video_id in video_ids
        if video_id not in existing_ids
    ]

    print("\n" + "=" * 70)
    print("DISLIKE CRAWLER")
    print("=" * 70)

    print(f"Total unique videos : {len(video_ids)}")
    print(f"Already crawled     : {len(existing_ids)}")
    print(f"Remaining           : {len(remaining_ids)}")

    if not remaining_ids:

        print("\n[COMPLETE]")
        print("All video IDs have been crawled.")

        return

    # --------------------------------------------------------
    # SESSION
    # --------------------------------------------------------

    session = requests.Session()

    success_count = 0
    error_count = 0

    try:

        for index, video_id in enumerate(
            remaining_ids,
            start=1
        ):

            result = fetch_dislike(
                video_id,
                session
            )

            if result is not None:

                rows.append(result)
                existing_ids.add(video_id)

                success_count += 1

            else:

                error_count += 1

            # ------------------------------------------------
            # PROGRESS
            # ------------------------------------------------

            if (
                index % 10 == 0
                or index == 1
                or index == len(remaining_ids)
            ):

                total_done = len(existing_ids)

                print(
                    f"[PROGRESS] "
                    f"{index}/{len(remaining_ids)} | "
                    f"Total saved: {total_done} | "
                    f"Success: {success_count} | "
                    f"Errors: {error_count}"
                )

            # ------------------------------------------------
            # CHECKPOINT
            # ------------------------------------------------

            if index % CHECKPOINT_EVERY == 0:

                save_checkpoint(rows)

            # ------------------------------------------------
            # RATE LIMIT DELAY
            # ------------------------------------------------

            time.sleep(SLEEP_SECONDS)

    except KeyboardInterrupt:

        print("\n\n[INTERRUPTED]")

        print("Saving current progress...")

        save_checkpoint(rows)

        print(
            f"Saved {len(rows)} rows."
        )

        print(
            "You can safely run the script again later."
        )

        return

    finally:

        session.close()

    # --------------------------------------------------------
    # FINAL SAVE
    # --------------------------------------------------------

    save_checkpoint(rows)

    print("\n" + "=" * 70)
    print("CRAWL FINISHED")
    print("=" * 70)

    print(f"Success : {success_count}")
    print(f"Errors  : {error_count}")
    print(f"Saved   : {len(rows)}")


if __name__ == "__main__":
    main()
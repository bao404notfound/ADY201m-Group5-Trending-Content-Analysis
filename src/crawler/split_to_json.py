import json
from pathlib import Path
import polars as pl

# ============================================================
# CONFIG
# ============================================================

INPUT_FILE = Path(
    "data/raw/youtube/youtube_raw_merged.parquet"
)

OUTPUT_DIR = Path(
    "data/raw/youtube/json"
)

CHUNK_SIZE = 1_000


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    print("=" * 70)
    print("SPLIT MERGED PARQUET → JSON CHUNKS")
    print("=" * 70)

    # --------------------------------------------------------
    # 1. Check input
    # --------------------------------------------------------

    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}"
        )

    print(f"Input : {INPUT_FILE}")
    print(f"Output: {OUTPUT_DIR}")
    print(f"Chunk : {CHUNK_SIZE:,} records/file")

    # --------------------------------------------------------
    # 2. Read merged Parquet
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("READ MERGED PARQUET")
    print("=" * 70)

    df = pl.read_parquet(INPUT_FILE)

    total_records = df.height

    print(f"Total records: {total_records:,}")
    print(f"Columns      : {len(df.columns)}")

    # --------------------------------------------------------
    # 3. Validate video_id
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("VALIDATE VIDEO IDS")
    print("=" * 70)

    unique_video_ids = df.select(
        "video_id"
    ).unique().height

    duplicate_video_ids = (
        total_records - unique_video_ids
    )

    print(f"Unique video IDs     : {unique_video_ids:,}")
    print(f"Duplicate video IDs  : {duplicate_video_ids:,}")

    if duplicate_video_ids != 0:
        raise ValueError(
            "Duplicate video_id detected. "
            "Stop before exporting JSON."
        )

    print("Video ID validation: OK")

    # --------------------------------------------------------
    # 4. Prepare output directory
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("PREPARE OUTPUT DIRECTORY")
    print("=" * 70)

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Remove old generated JSON chunks.
    old_files = sorted(
        OUTPUT_DIR.glob("part_*.json")
    )

    if old_files:
        print(
            f"Removing {len(old_files)} old JSON files..."
        )

        for file in old_files:
            file.unlink()

    else:
        print("No old JSON chunks found.")

    # --------------------------------------------------------
    # 5. Calculate number of chunks
    # --------------------------------------------------------

    num_chunks = (
        total_records + CHUNK_SIZE - 1
    ) // CHUNK_SIZE

    print(f"Expected JSON files: {num_chunks}")

    # --------------------------------------------------------
    # 6. Export chunks
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("EXPORT JSON CHUNKS")
    print("=" * 70)

    exported_records = 0

    for chunk_index, start in enumerate(
        range(0, total_records, CHUNK_SIZE)
    ):

        end = min(
            start + CHUNK_SIZE,
            total_records,
        )

        chunk = df.slice(
            start,
            end - start,
        )

        records = chunk.to_dicts()

        output_file = OUTPUT_DIR / (
            f"part_{chunk_index:03d}.json"
        )

        with output_file.open(
            "w",
            encoding="utf-8",
        ) as f:

            json.dump(
                records,
                f,
                ensure_ascii=False,
                indent=2,
            )

        exported_records += len(records)

        print(
            f"{output_file.name:<15} "
            f"{len(records):>6,} records"
        )

    # --------------------------------------------------------
    # 7. Validate exported file count
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("VALIDATE OUTPUT")
    print("=" * 70)

    output_files = sorted(
        OUTPUT_DIR.glob("part_*.json")
    )

    actual_file_count = len(output_files)

    print(
        f"Expected files : {num_chunks}"
    )

    print(
        f"Actual files   : {actual_file_count}"
    )

    if actual_file_count != num_chunks:
        raise RuntimeError(
            "Output file count does not match expected count."
        )

    # --------------------------------------------------------
    # 8. Validate exported record count
    # --------------------------------------------------------

    print(
        f"Input records  : {total_records:,}"
    )

    print(
        f"Output records : {exported_records:,}"
    )

    if exported_records != total_records:
        raise RuntimeError(
            "Record count mismatch! "
            "Some records may have been lost."
        )

    # --------------------------------------------------------
    # 9. Validate JSON files
    # --------------------------------------------------------

    print()
    print("Validating JSON files...")

    validated_records = 0

    for file in output_files:

        with file.open(
            "r",
            encoding="utf-8",
        ) as f:

            records = json.load(f)

        if not isinstance(records, list):
            raise ValueError(
                f"{file.name} is not a JSON array."
            )

        if len(records) == 0:
            raise ValueError(
                f"{file.name} is empty."
            )

        validated_records += len(records)

    print(
        f"Validated records: {validated_records:,}"
    )

    if validated_records != total_records:
        raise RuntimeError(
            "JSON validation failed: "
            "record count mismatch."
        )

    # --------------------------------------------------------
    # 10. Final result
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("SPLIT COMPLETED SUCCESSFULLY")
    print("=" * 70)

    print(
        f"Total records : {total_records:,}"
    )

    print(
        f"JSON files    : {actual_file_count}"
    )

    print(
        f"Chunk size    : {CHUNK_SIZE:,}"
    )

    print(
        f"Output dir    : {OUTPUT_DIR}"
    )

    print()
    print(
        f"VALIDATION: "
        f"{validated_records:,} / "
        f"{total_records:,} records OK"
    )


if __name__ == "__main__":
    main()

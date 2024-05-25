import pandas as pd
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Spliter", description="Program to spliting data filter and split dataset."
    )

    parser.add_argument("-u", "--max-users", type=int)
    parser.add_argument("-s", "--db-size", type=float)

    args = parser.parse_args()

    origin_fn = "data/rec-amazon-ratings.edges"
    in_db_fn = "data/in_db.edges"
    stream_fn = "data/stream.edges"

    df_origin = pd.read_csv(
        origin_fn, sep=",", names=["UserId", "ProductId", "Review", "Timestamp"]
    )

    df_narrowed = df_origin[df_origin["UserId"] < args.max_users]

    in_db_df = df_narrowed.sample(frac=args.db_size, random_state=200)
    stream_df = df_narrowed.drop(in_db_df.index)

    print(f"Original rows number: {df_origin.shape[0]}")
    print(f"Narrowed rows number: {df_narrowed.shape[0]}")
    print(f"In DB rows number: {in_db_df.shape[0]}")
    print(f"Stream rows number: {stream_df.shape[0]}")

    in_db_df.to_csv(in_db_fn)
    stream_df.to_csv(stream_fn)


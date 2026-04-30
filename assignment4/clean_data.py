import pandas as pd
import numpy as np
import os

def clean_data(file_path):
    df = pd.read_csv(file_path)
    initial_count = len(df)
    
    # 1. Remove duplicates based on ID or Title
    df = df.drop_duplicates(subset=["id"])
    df = df.drop_duplicates(subset=["title"])
    after_duplicates = len(df)
    
    # 2. Handle missing values
    # Fill empty authors, keywords, countries with "Unknown"
    df["authors"] = df["authors"].fillna("Unknown Author")
    df["journal"] = df["journal"].fillna("Unknown Journal")
    df["keywords"] = df["keywords"].fillna("")
    df["countries"] = df["countries"].fillna("Unknown")
    
    # 3. Normalize names
    # Author normalization (simple: strip spaces, title case)
    df["authors"] = df["authors"].apply(lambda x: "; ".join([a.strip().title() for a in str(x).split(";")]))
    
    # Journal normalization
    df["journal"] = df["journal"].apply(lambda x: str(x).strip().title())
    
    # 4. Filter by year (ensure it's within range if not already)
    df = df[df["year"] >= 2014]
    
    # Summary of cleaning
    print(f"Initial records: {initial_count}")
    print(f"After removing duplicates: {after_duplicates}")
    print(f"Final records: {len(df)}")
    
    return df

if __name__ == "__main__":
    raw_csv = "assignment4/data/raw/openalex_processed_raw.csv"
    os.makedirs("assignment4/data/processed", exist_ok=True)
    
    cleaned_df = clean_data(raw_csv)
    cleaned_df.to_csv("assignment4/data/processed/openalex_cleaned.csv", index=False)
    print("Saved cleaned data to assignment4/data/processed/openalex_cleaned.csv")

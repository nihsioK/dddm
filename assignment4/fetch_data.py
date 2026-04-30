import requests
import json
import time
import pandas as pd
import os

def fetch_openalex_data(query, n_results=1000):
    base_url = "https://api.openalex.org/works"
    params = {
        "search": query,
        "filter": "publication_year:>2013",
        "per_page": 200,
        "page": 1
    }
    
    all_works = []
    while len(all_works) < n_results:
        print(f"Fetching page {params['page']}...")
        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            print(f"Error fetching data: {response.status_code}")
            break
        
        data = response.json()
        results = data.get("results", [])
        if not results:
            break
            
        all_works.extend(results)
        params["page"] += 1
        
        if len(results) < params["per_page"]:
            break
            
        # Respect rate limits
        time.sleep(0.1)
        
    return all_works[:n_results]

def process_works(works):
    processed_data = []
    for work in works:
        # Extract authors
        authors = [a.get("author", {}).get("display_name") for a in work.get("authorships", [])]
        authors_str = "; ".join([a for a in authors if a])
        
        # Extract journal
        journal = work.get("primary_location", {}).get("source", {}).get("display_name") if work.get("primary_location") and work.get("primary_location").get("source") else None
        
        # Extract keywords
        keywords = [k.get("display_name") for k in work.get("keywords", [])]
        keywords_str = "; ".join(keywords)
        
        # Extract countries
        countries = set()
        for authorship in work.get("authorships", []):
            for inst in authorship.get("institutions", []):
                country_code = inst.get("country_code")
                if country_code:
                    countries.add(country_code)
        countries_str = "; ".join(sorted(list(countries)))
        
        processed_data.append({
            "id": work.get("id"),
            "title": work.get("display_name"),
            "year": work.get("publication_year"),
            "authors": authors_str,
            "journal": journal,
            "citations": work.get("cited_by_count"),
            "keywords": keywords_str,
            "countries": countries_str,
            "abstract_inverted_index": work.get("abstract_inverted_index") # useful for later
        })
    return pd.DataFrame(processed_data)

if __name__ == "__main__":
    # Query related to Staj.kz business idea: Internships, Early Career, Graduate Recruitment
    query = '(internship OR "early career" OR "graduate recruitment" OR "entry level jobs")'
    
    # Ensure directory exists
    os.makedirs("assignment4/data/raw", exist_ok=True)
    
    print(f"Starting data collection for query: {query}")
    works = fetch_openalex_data(query, n_results=1200) # Fetch a bit more to account for duplicates
    
    # Save raw JSON
    with open("assignment4/data/raw/openalex_raw.json", "w") as f:
        json.dump(works, f)
    print(f"Saved {len(works)} raw records to assignment4/data/raw/openalex_raw.json")
    
    # Process and save to CSV
    df = process_works(works)
    df.to_csv("assignment4/data/raw/openalex_processed_raw.csv", index=False)
    print(f"Saved processed data to assignment4/data/raw/openalex_processed_raw.csv")

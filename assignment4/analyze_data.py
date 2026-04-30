import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import os
import json
from collections import Counter

# Set style
sns.set(style="whitegrid")

def plot_dynamics(df, output_path):
    plt.figure(figsize=(10, 6))
    yearly_counts = df["year"].value_counts().sort_index()
    sns.lineplot(x=yearly_counts.index, y=yearly_counts.values, marker='o')
    plt.title("Dynamics of Publications by Year")
    plt.xlabel("Year")
    plt.ylabel("Number of Publications")
    plt.savefig(f"{output_path}/dynamics.png")
    plt.close()

def plot_top_10(df, column, title, output_path, filename, split=False):
    plt.figure(figsize=(12, 8))
    if split:
        all_items = []
        for items in df[column].dropna():
            all_items.extend([i.strip() for i in str(items).split(";") if i.strip() and i.strip() != "Unknown" and i.strip() != "Unknown Author"])
        counts = pd.Series(all_items).value_counts().head(10)
    else:
        counts = df[column].value_counts().head(10)
    
    sns.barplot(x=counts.values, y=counts.index, palette="viridis")
    plt.title(title)
    plt.xlabel("Count")
    plt.savefig(f"{output_path}/{filename}.png")
    plt.close()

def plot_citations(df, output_path):
    plt.figure(figsize=(10, 6))
    sns.histplot(df["citations"], bins=30, kde=True, log_scale=(False, True))
    plt.title("Distribution of Citations (Log Scale)")
    plt.xlabel("Citations")
    plt.ylabel("Frequency (Log)")
    plt.savefig(f"{output_path}/citations.png")
    plt.close()

def build_keyword_network(df, output_path):
    G = nx.Graph()
    keyword_pairs = []
    
    for k_str in df["keywords"].dropna():
        keywords = [k.strip() for k in str(k_str).split(";") if k.strip()]
        for i in range(len(keywords)):
            for j in range(i + 1, len(keywords)):
                keyword_pairs.append(tuple(sorted((keywords[i], keywords[j]))))
                
    pair_counts = Counter(keyword_pairs)
    # Take top 100 edges for visibility
    top_pairs = sorted(pair_counts.items(), key=lambda x: x[1], reverse=True)[:100]
    
    for (u, v), w in top_pairs:
        G.add_edge(u, v, weight=w)
        
    plt.figure(figsize=(15, 12))
    pos = nx.spring_layout(G, k=0.5)
    
    # Node size based on degree
    degrees = dict(G.degree())
    node_sizes = [v * 100 for v in degrees.values()]
    
    nx.draw_networkx_nodes(G, pos, node_size=node_sizes, node_color='skyblue', alpha=0.8)
    nx.draw_networkx_edges(G, pos, width=1, alpha=0.5, edge_color='grey')
    nx.draw_networkx_labels(G, pos, font_size=10, font_family='sans-serif')
    
    plt.title("Keyword Co-occurrence Network (Top 100 Edges)")
    plt.axis('off')
    plt.savefig(f"{output_path}/network.png")
    plt.close()
    
    # Centrality
    centrality = nx.degree_centrality(G)
    top_central = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:10]
    return top_central

def perform_topic_modeling(df, n_topics=5):
    # Combine title and keywords for modeling
    text_data = df["title"] + " " + df["keywords"].fillna("")
    
    vectorizer = TfidfVectorizer(stop_words='english', max_features=1000)
    dtm = vectorizer.fit_transform(text_data)
    
    lda = LatentDirichletAllocation(n_components=n_topics, random_state=42)
    lda.fit(dtm)
    
    words = vectorizer.get_feature_names_out()
    topics = {}
    for i, topic in enumerate(lda.components_):
        top_words = [words[j] for j in topic.argsort()[-10:]]
        topics[f"Topic {i+1}"] = top_words
        
    return topics

if __name__ == "__main__":
    df = pd.read_csv("assignment4/data/processed/openalex_cleaned.csv")
    fig_path = "assignment4/output/figures"
    os.makedirs(fig_path, exist_ok=True)
    
    print("Generating visualizations...")
    plot_dynamics(df, fig_path)
    plot_top_10(df, "journal", "Top 10 Journals", fig_path, "top_journals")
    plot_top_10(df, "authors", "Top 10 Authors", fig_path, "top_authors", split=True)
    plot_top_10(df, "countries", "Top 10 Countries", fig_path, "top_countries", split=True)
    plot_citations(df, fig_path)
    
    print("Building network...")
    centrality = build_keyword_network(df, fig_path)
    print("Top Central Keywords:", centrality)
    
    print("Performing topic modeling...")
    topics = perform_topic_modeling(df)
    with open("assignment4/output/topics.json", "w") as f:
        json.dump(topics, f, indent=4)
    print("Topics identified and saved to assignment4/output/topics.json")

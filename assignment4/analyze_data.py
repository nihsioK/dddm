import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import seaborn as sns
import networkx as nx
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import os
import json
from collections import Counter
import numpy as np

sns.set(style="whitegrid")


def plot_dynamics(df, output_path):
    plt.figure(figsize=(10, 6))
    yearly_counts = df["year"].value_counts().sort_index()
    sns.lineplot(x=yearly_counts.index, y=yearly_counts.values, marker="o")
    plt.title("Publication Dynamics by Year")
    plt.xlabel("Year")
    plt.ylabel("Number of Publications")
    plt.tight_layout()
    plt.savefig(f"{output_path}/dynamics.png", dpi=150)
    plt.close()


def plot_top_10(df, column, title, output_path, filename, split=False):
    plt.figure(figsize=(12, 8))
    if split:
        all_items = []
        for items in df[column].dropna():
            all_items.extend(
                [
                    i.strip()
                    for i in str(items).split(";")
                    if i.strip()
                    and i.strip() not in ("Unknown", "Unknown Author")
                ]
            )
        counts = pd.Series(all_items).value_counts().head(10)
    else:
        filtered = df[column][~df[column].isin(["Unknown Journal", "Unknown"])]
        counts = filtered.value_counts().head(10)

    sns.barplot(x=counts.values, y=counts.index, palette="viridis")
    plt.title(title)
    plt.xlabel("Count")
    plt.tight_layout()
    plt.savefig(f"{output_path}/{filename}.png", dpi=150)
    plt.close()


def plot_citations(df, output_path):
    plt.figure(figsize=(10, 6))
    data = df["citations"].dropna()
    data = data[data > 0]
    sns.histplot(data, bins=40, kde=False, log_scale=(False, True))
    plt.title("Citation Distribution (Log Y-axis)")
    plt.xlabel("Citations")
    plt.ylabel("Frequency (log scale)")
    plt.tight_layout()
    plt.savefig(f"{output_path}/citations.png", dpi=150)
    plt.close()


def build_keyword_network(df, output_path):
    keyword_pairs = []
    for k_str in df["keywords"].dropna():
        keywords = [k.strip() for k in str(k_str).split(";") if k.strip()]
        for i in range(len(keywords)):
            for j in range(i + 1, len(keywords)):
                keyword_pairs.append(tuple(sorted((keywords[i], keywords[j]))))

    pair_counts = Counter(keyword_pairs)
    top_pairs = sorted(pair_counts.items(), key=lambda x: x[1], reverse=True)[:100]

    G = nx.Graph()
    for (u, v), w in top_pairs:
        G.add_edge(u, v, weight=w)

    # Community detection
    communities = list(nx.community.greedy_modularity_communities(G))
    node_community = {}
    for idx, comm in enumerate(communities):
        for node in comm:
            node_community[node] = idx

    color_map = cm.get_cmap("tab10", len(communities))
    node_colors = [color_map(node_community.get(n, 0)) for n in G.nodes()]

    plt.figure(figsize=(16, 13))
    pos = nx.spring_layout(G, k=0.6, seed=42)
    degrees = dict(G.degree())
    node_sizes = [degrees[n] * 120 for n in G.nodes()]

    nx.draw_networkx_nodes(G, pos, node_size=node_sizes, node_color=node_colors, alpha=0.85)
    nx.draw_networkx_edges(G, pos, width=0.8, alpha=0.4, edge_color="grey")
    nx.draw_networkx_labels(G, pos, font_size=8, font_family="sans-serif")

    # Legend for communities
    handles = [
        plt.Line2D([0], [0], marker="o", color="w",
                   markerfacecolor=color_map(i), markersize=10,
                   label=f"Cluster {i + 1}")
        for i in range(min(len(communities), 8))
    ]
    plt.legend(handles=handles, loc="lower left", fontsize=9)
    plt.title("Keyword Co-occurrence Network (Top 100 Edges, Colored by Community)")
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(f"{output_path}/network.png", dpi=150)
    plt.close()

    centrality = nx.degree_centrality(G)
    top_central = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:10]

    cluster_descriptions = []
    for idx, comm in enumerate(communities[:6]):
        top_nodes = sorted(comm, key=lambda n: centrality.get(n, 0), reverse=True)[:5]
        cluster_descriptions.append({"cluster": idx + 1, "top_keywords": list(top_nodes)})

    return top_central, cluster_descriptions


def plot_keyword_trends(df, output_path):
    top_keywords = ["internship", "career", "education", "psychology", "medicine", "engineering"]
    year_range = sorted(df["year"].dropna().unique())

    trend_data = {kw: [] for kw in top_keywords}
    for year in year_range:
        year_df = df[df["year"] == year]
        all_kw = " ".join(year_df["keywords"].fillna("").tolist()).lower()
        for kw in top_keywords:
            trend_data[kw].append(all_kw.count(kw))

    plt.figure(figsize=(12, 7))
    for kw in top_keywords:
        plt.plot(year_range, trend_data[kw], marker="o", label=kw.capitalize())

    plt.title("Keyword Frequency Trends by Year")
    plt.xlabel("Year")
    plt.ylabel("Keyword Occurrences")
    plt.legend(loc="upper left")
    plt.tight_layout()
    plt.savefig(f"{output_path}/keyword_trends.png", dpi=150)
    plt.close()


def perform_topic_modeling(df, output_path, n_topics=5):
    text_data = (df["title"].fillna("") + " " + df["keywords"].fillna("")).str.strip()

    vectorizer = CountVectorizer(stop_words="english", max_features=1000, min_df=3)
    dtm = vectorizer.fit_transform(text_data)

    lda = LatentDirichletAllocation(n_components=n_topics, random_state=42, max_iter=20)
    lda.fit(dtm)

    words = vectorizer.get_feature_names_out()
    topics = {}
    for i, topic in enumerate(lda.components_):
        top_words = [words[j] for j in topic.argsort()[-10:]][::-1]
        topics[f"Topic {i + 1}"] = top_words

    # Visualize topics as horizontal bar charts
    fig, axes = plt.subplots(1, n_topics, figsize=(20, 5))
    for i, (topic_name, top_words) in enumerate(topics.items()):
        scores = lda.components_[i][[list(words).index(w) for w in top_words]]
        axes[i].barh(top_words[::-1], scores[::-1], color=plt.cm.tab10(i))
        axes[i].set_title(topic_name, fontsize=11)
        axes[i].set_xlabel("Weight")
    plt.suptitle("LDA Topic Modeling — Top Keywords per Topic", fontsize=13, y=1.02)
    plt.tight_layout()
    plt.savefig(f"{output_path}/lda_topics.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    return topics


if __name__ == "__main__":
    df = pd.read_csv("assignment4/data/processed/openalex_cleaned.csv")
    fig_path = "assignment4/output/figures"
    os.makedirs(fig_path, exist_ok=True)
    os.makedirs("assignment4/output", exist_ok=True)

    print("Generating visualizations...")
    plot_dynamics(df, fig_path)
    plot_top_10(df, "journal", "Top 10 Journals", fig_path, "top_journals")
    plot_top_10(df, "authors", "Top 10 Authors", fig_path, "top_authors", split=True)
    plot_top_10(df, "countries", "Top 10 Countries", fig_path, "top_countries", split=True)
    plot_citations(df, fig_path)

    print("Building network with community detection...")
    centrality, clusters = build_keyword_network(df, fig_path)
    print("Top Central Keywords:", centrality)
    print("Clusters:", clusters)

    print("Keyword trend analysis...")
    plot_keyword_trends(df, fig_path)

    print("Performing topic modeling...")
    topics = perform_topic_modeling(df, fig_path)

    analytics = {
        "top_centrality": centrality,
        "clusters": clusters,
        "topics": topics,
    }
    with open("assignment4/output/topics.json", "w") as f:
        json.dump(analytics, f, indent=4)
    print("All outputs saved.")

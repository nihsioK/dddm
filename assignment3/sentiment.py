import pandas as pd
import torch
import json
from transformers import pipeline
from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns
from assignment3.config import SENTIMENT_MODEL_NAME, REVIEWS_DIR, OUTPUT_DIR, ensure_dirs

class SentimentAnalyzer:
    def __init__(self, model_name=SENTIMENT_MODEL_NAME):
        print(f"Loading multilingual sentiment model: {model_name}...")
        device = 0 if torch.cuda.is_available() else -1
        # CardiffNLP model outputs labels: 'negative', 'neutral', 'positive'
        self.classifier = pipeline("sentiment-analysis", model=model_name, device=device)
        
    def analyze_reviews(self, df: pd.DataFrame, text_column='review_text'):
        """Runs inference on a dataframe of reviews with multilingual support."""
        print(f"Analyzing {len(df)} reviews (Multilingual mode)...")
        
        texts = df[text_column].fillna("").tolist()
        results = self.classifier(texts, truncation=True)
        
        # Standardize labels to UPPERCASE for comparison
        df['sentiment_label'] = [res['label'].upper() for res in results]
        df['sentiment_score'] = [res['score'] for res in results]
        
        return df

    def add_ground_truth(self, df: pd.DataFrame):
        """Maps AI-generated rating (1-5) to sentiment labels for validation."""
        def map_rating(rating):
            if rating >= 4: return 'POSITIVE'
            if rating == 3: return 'NEUTRAL'
            return 'NEGATIVE'
        
        df['true_sentiment'] = df['rating'].apply(map_rating)
        return df

    def evaluate(self, df: pd.DataFrame):
        """Calculates accuracy metrics and plots confusion matrix."""
        print("\n=== Evaluation Results (Model vs AI-Rating) ===")
        labels = ['NEGATIVE', 'NEUTRAL', 'POSITIVE']
        
        # Valid data check
        valid_df = df[df['sentiment_label'].isin(labels) & df['true_sentiment'].isin(labels)]
        
        report = classification_report(valid_df['true_sentiment'], valid_df['sentiment_label'], target_names=labels, zero_division=0)
        print(report)
        
        with open(OUTPUT_DIR / "sentiment_report.txt", "w") as f:
            f.write(report)
            
        # Confusion Matrix Plot
        cm = confusion_matrix(valid_df['true_sentiment'], valid_df['sentiment_label'], labels=labels)
        plt.figure(figsize=(8, 6))
        sns.heatmap(cm, annot=True, fmt='d', cmap='Greens', xticklabels=labels, yticklabels=labels)
        plt.title("Confusion Matrix: Multilingual Model vs AI-Ratings")
        plt.ylabel("Actual (AI Rating Label)")
        plt.xlabel("Predicted (Multilingual Model)")
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / "confusion_matrix.png", dpi=180)
        plt.close()
            
        return report

    def plot_time_dynamics(self, df: pd.DataFrame):
        """Visualizes average rating shifts from 2023 to 2026."""
        print("Plotting rating dynamics...")
        df['date'] = pd.to_datetime(df['date'])
        df['month_year'] = df['date'].dt.to_period('M')
        
        # Группируем по компании и месяцу, считаем средний рейтинг
        # Это поле 'rating' из твоего JSON
        dynamics = df.groupby(['company', 'month_year'])['rating'].mean().reset_index()
        
        plt.figure(figsize=(12, 7))
        sns.set_style("whitegrid")
        
        plotted = False
        for company in dynamics['company'].unique():
            data = dynamics[dynamics['company'] == company].sort_values('month_year')
            # Рисуем линию и точки
            plt.plot(
                data['month_year'].dt.to_timestamp(), 
                data['rating'], 
                label=f'{company} (Avg Rating)', 
                marker='o', 
                linewidth=2,
                markersize=8
            )
            plotted = True
        
        plt.title("Competitor Rating Dynamics (2023-2026)", fontsize=14)
        plt.ylabel("Average Rating (1-5)", fontsize=12)
        plt.xlabel("Timeline", fontsize=12)
        plt.ylim(0.5, 5.5) # Рейтинг от 1 до 5
        plt.yticks([1, 2, 3, 4, 5])
        
        if plotted:
            plt.legend()
            
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / "sentiment_dynamics.png", dpi=180)
        plt.close()
        print(f"Plot updated with Rating Dynamics: {OUTPUT_DIR / 'sentiment_dynamics.png'}")

    def plot_sentiment_distribution(self, df: pd.DataFrame):
        """Bar chart of sentiment distribution per company."""
        plt.figure(figsize=(10, 6))
        sns.countplot(data=df, x='company', hue='sentiment_label', palette='magma')
        plt.title("Sentiment Distribution by Competitor (Multilingual Model)")
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / "sentiment_distribution.png", dpi=180)
        plt.close()

def main():
    ensure_dirs()
    
    reviews_file = REVIEWS_DIR / "all_reviews.json"
    if not reviews_file.exists():
        print(f"Error: {reviews_file} not found.")
        return

    with open(reviews_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    
    analyzer = SentimentAnalyzer()
    df = analyzer.analyze_reviews(df)
    df = analyzer.add_ground_truth(df)
    
    analyzer.evaluate(df)
    analyzer.plot_time_dynamics(df)
    analyzer.plot_sentiment_distribution(df)
    
    output_csv = OUTPUT_DIR / "analyzed_reviews.csv"
    df.to_csv(output_csv, index=False)
    print(f"Analyzed data saved to {output_csv}")

if __name__ == "__main__":
    main()

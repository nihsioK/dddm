from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "assignment3" / "data"
REVIEWS_DIR = BASE_DIR / "reviews"  # Pointing to the actual reviews folder
OUTPUT_DIR = BASE_DIR / "output" / "assignment3"

# Model configuration
# Мультиязычная модель (поддерживает казахский и русский)
SENTIMENT_MODEL_NAME = "cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual"

# Competitors based on your all_reviews.json
COMPETITORS = ["hh.kz", "enbek.kz", "dvjob.kz"]

TRENDS_FILE = BASE_DIR / "google-trends" / "multiTimeline.csv"

def ensure_dirs():
    for d in [DATA_DIR, OUTPUT_DIR]:
        d.mkdir(parents=True, exist_ok=True)

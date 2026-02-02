"""
Setup script to create sample data and train the sentiment model for demo.
"""
import csv
import pickle
import random
from pathlib import Path

# Sample positive reviews
POSITIVE_REVIEWS = [
    "This product is amazing! Exactly what I needed.",
    "Great quality, fast shipping. Highly recommend!",
    "Love it! Works perfectly and looks beautiful.",
    "Excellent product, exceeded my expectations.",
    "Best purchase I've made in a long time!",
    "Five stars! This is exactly as described.",
    "Perfect fit and great quality material.",
    "Very happy with this purchase. Will buy again!",
    "Fantastic product at a great price.",
    "Impressed with the quality. Worth every penny!",
    "This is exactly what I was looking for. Amazing!",
    "Super fast delivery and product works great!",
    "Highly recommend this to everyone!",
    "Outstanding quality and performance.",
    "Love this product! It's perfect.",
    "Great value for money. Very satisfied!",
    "Exceeded all my expectations. Wonderful!",
    "Beautiful design and works flawlessly.",
    "Absolutely love it! Five stars!",
    "Best product I've ever purchased online.",
]

# Sample negative reviews
NEGATIVE_REVIEWS = [
    "Terrible quality. Broke after one day.",
    "Do not buy this! Complete waste of money.",
    "Very disappointed. Not as described at all.",
    "Poor quality, doesn't work as expected.",
    "Worst purchase ever. Returning immediately.",
    "Cheap material, fell apart quickly.",
    "Not worth the money. Very disappointing.",
    "Product arrived damaged and doesn't work.",
    "Horrible experience. Would not recommend.",
    "Complete garbage. Save your money!",
    "This is awful. Don't waste your time.",
    "Extremely poor quality. Very unhappy.",
    "Doesn't work at all. Total scam!",
    "Regret buying this. Terrible product.",
    "Broke within a week. Very frustrated.",
    "Not what I expected. Very misleading.",
    "Defective product. Customer service unhelpful.",
    "Cheap knockoff. Nothing like the pictures.",
    "Waste of money. Returning this junk.",
    "Absolutely terrible. One star is too generous.",
]

def create_sample_csv():
    """Create sample CSV with reviews for demo."""
    out_path = Path("data/processed/amazon/amazon_sample.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    reviews = []
    # Generate 200 samples (enough for demo)
    for i in range(100):
        reviews.append((random.choice(POSITIVE_REVIEWS), "positive"))
        reviews.append((random.choice(NEGATIVE_REVIEWS), "negative"))
    
    random.shuffle(reviews)
    
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["text", "label"])
        writer.writerows(reviews)
    
    print(f"[OK] Created sample CSV: {out_path} ({len(reviews)} rows)")
    return out_path

def train_model():
    """Train a simple sentiment model."""
    try:
        from sklearn.feature_extraction.text import HashingVectorizer
        from sklearn.linear_model import SGDClassifier
    except ImportError:
        print("[WARN] scikit-learn not installed. Installing...")
        import subprocess
        subprocess.check_call(["pip", "install", "scikit-learn", "-q"])
        from sklearn.feature_extraction.text import HashingVectorizer
        from sklearn.linear_model import SGDClassifier
    
    import numpy as np
    
    # Training data
    texts = POSITIVE_REVIEWS + NEGATIVE_REVIEWS
    labels = [1] * len(POSITIVE_REVIEWS) + [0] * len(NEGATIVE_REVIEWS)
    
    # Create vectorizer
    vectorizer = HashingVectorizer(
        n_features=2**18,  # Smaller for demo
        alternate_sign=False,
        ngram_range=(1, 2),
        norm="l2",
    )
    
    X = vectorizer.transform(texts)
    y = np.array(labels)
    
    # Train classifier
    clf = SGDClassifier(
        loss="log_loss",
        penalty="l2",
        alpha=1e-4,
        max_iter=100,
        tol=1e-3,
        random_state=42,
    )
    clf.fit(X, y)
    
    # Create model wrapper class (inline to avoid import issues)
    class Sentiment3ClassConfig:
        def __init__(self, pos_threshold=0.65, neg_threshold=0.35):
            self.pos_threshold = pos_threshold
            self.neg_threshold = neg_threshold
    
    class SentimentModel3Class:
        def __init__(self, vectorizer, clf, cfg=None, model_version="v1"):
            self.vectorizer = vectorizer
            self.clf = clf
            self.cfg = cfg or Sentiment3ClassConfig()
            self.model_version = model_version
        
        def predict_one(self, text):
            X = self.vectorizer.transform([text])
            if hasattr(self.clf, "predict_proba"):
                p_neg, p_pos = self.clf.predict_proba(X)[0]
            else:
                score = float(self.clf.decision_function(X)[0])
                p_pos = 1.0 / (1.0 + np.exp(-score))
                p_neg = 1.0 - p_pos
            
            if p_pos >= self.cfg.pos_threshold:
                label, score3 = "positive", 1
            elif p_pos <= self.cfg.neg_threshold:
                label, score3 = "negative", -1
            else:
                label, score3 = "neutral", 0
            
            confidence = float(max(p_pos, 1.0 - p_pos))
            probs = {"p_pos": float(p_pos), "p_neg": float(p_neg)}
            return label, score3, confidence, probs
        
        def save(self, path):
            path = Path(path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("wb") as f:
                pickle.dump(self, f)
        
        @staticmethod
        def load(path):
            path = Path(path)
            with path.open("rb") as f:
                return pickle.load(f)
    
    # Create and save model
    cfg = Sentiment3ClassConfig(pos_threshold=0.65, neg_threshold=0.35)
    model = SentimentModel3Class(
        vectorizer=vectorizer, 
        clf=clf, 
        cfg=cfg, 
        model_version="hashing_sgd_3class_v1"
    )
    
    out_path = Path("ml/artifacts/sentiment_model_3class.pkl")
    model.save(out_path)
    
    # Test the model
    test_texts = ["This is great!", "Terrible product", "It's okay I guess"]
    print(f"[OK] Trained and saved model: {out_path}")
    print("\n[TEST] Model Test:")
    for text in test_texts:
        label, score, conf, _ = model.predict_one(text)
        print(f"   '{text}' → {label} (conf: {conf:.2f})")
    
    return out_path

if __name__ == "__main__":
    print("Setting up demo data for Sentiment & Churn Prediction project\n")
    create_sample_csv()
    train_model()
    print("\n[OK] Setup complete! You can now run: docker-compose up -d")

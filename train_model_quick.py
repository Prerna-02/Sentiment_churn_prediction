"""Quick model training script using the shared module."""
import sys
sys.path.insert(0, '.')

from pathlib import Path
import numpy as np
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.linear_model import SGDClassifier
from shared.sentiment_model import SentimentModel3Class, Sentiment3ClassConfig

# Sample training data
POSITIVE = [
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
    "Super fast delivery and product works great!",
    "Outstanding quality and performance.",
    "Great value for money. Very satisfied!",
    "Beautiful design and works flawlessly.",
    "Best product I've ever purchased online.",
]

NEGATIVE = [
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
    "Extremely poor quality. Very unhappy.",
    "Doesn't work at all. Total scam!",
    "Defective product. Customer service unhelpful.",
    "Cheap knockoff. Nothing like the pictures.",
    "Absolutely terrible. One star is too generous.",
]

def main():
    texts = POSITIVE + NEGATIVE
    labels = [1] * len(POSITIVE) + [0] * len(NEGATIVE)
    
    vectorizer = HashingVectorizer(
        n_features=2**18,
        alternate_sign=False,
        ngram_range=(1, 2),
        norm="l2",
    )
    
    X = vectorizer.transform(texts)
    y = np.array(labels)
    
    clf = SGDClassifier(
        loss="log_loss",
        penalty="l2",
        alpha=1e-4,
        max_iter=100,
        tol=1e-3,
        random_state=42,
    )
    clf.fit(X, y)
    
    cfg = Sentiment3ClassConfig(pos_threshold=0.65, neg_threshold=0.35)
    model = SentimentModel3Class(
        vectorizer=vectorizer,
        clf=clf,
        cfg=cfg,
        model_version="hashing_sgd_3class_v1"
    )
    
    out_path = Path("ml/artifacts/sentiment_model_3class.pkl")
    model.save(out_path)
    print(f"[OK] Model saved: {out_path}")
    
    # Test
    for text in ["Great product!", "Terrible!", "It's okay"]:
        label, score, conf, _ = model.predict_one(text)
        print(f"  '{text}' -> {label} (conf: {conf:.2f})")

if __name__ == "__main__":
    main()

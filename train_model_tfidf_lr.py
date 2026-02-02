"""
Training script for sentiment analysis using TF-IDF + Logistic Regression
Trained on real Amazon review data with 3-class classification (positive/neutral/negative)

Strategy:
- Binary labels from Amazon data (__label__1=negative, __label__2=positive)
- Use probability thresholds to create neutral class
- Train on subset of data for faster training (configurable)
"""

import sys
import time
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score, 
    classification_report, 
    confusion_matrix,
    f1_score
)
import pickle

# ==================== CONFIGURATION ====================
# Dataset paths
RAW_DATA_PATH = "data/raw/amazon/train.ft.txt"
SAMPLE_SIZE = 100000  # Use 100K reviews (out of 3.6M) - good balance of speed vs accuracy
TEST_SIZE = 0.2  # 20% for testing

# Model parameters
MAX_FEATURES = 10000  # Top 10K most important words
NGRAM_RANGE = (1, 2)  # Unigrams and bigrams
MAX_ITER = 1000  # Logistic Regression iterations

# Neutral class thresholds (based on prediction probabilities)
POSITIVE_THRESHOLD = 0.65  # Prob >= 0.65 → Positive
NEGATIVE_THRESHOLD = 0.35  # Prob <= 0.35 → Negative
# Between 0.35 and 0.65 → Neutral

# Output
MODEL_OUTPUT_PATH = "ml/artifacts/sentiment_model_tfidf_lr.pkl"
MODEL_VERSION = "amazon_tfidf_lr_v1"

# ==================== HELPER FUNCTIONS ====================

def parse_fasttext_line(line):
    """
    Parse FastText format line: __label__X text
    Returns: (text, label) where label is 'positive' or 'negative'
    """
    line = line.strip()
    if not line:
        return None, None
    
    if line.startswith("__label__1"):
        label = "negative"
        text = line.replace("__label__1", "", 1).strip()
    elif line.startswith("__label__2"):
        label = "positive"
        text = line.replace("__label__2", "", 1).strip()
    else:
        return None, None
    
    return text, label


def load_amazon_data(file_path, sample_size=None):
    """
    Load Amazon reviews from FastText format file.
    Uses reservoir sampling for memory efficiency.
    
    Args:
        file_path: Path to train.ft.txt
        sample_size: Number of reviews to sample (None = load all)
    
    Returns:
        DataFrame with 'text' and 'label' columns
    """
    print(f"📂 Loading data from: {file_path}")
    print(f"   Sample size: {sample_size if sample_size else 'ALL (3.6M reviews)'}")
    
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Cannot find: {file_path}")
    
    texts = []
    labels = []
    
    if sample_size is None:
        # Load all data (slow, memory intensive)
        with file_path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                text, label = parse_fasttext_line(line)
                if text and label:
                    texts.append(text)
                    labels.append(label)
    else:
        # Reservoir sampling for memory efficiency
        sample = []
        seen = 0
        
        with file_path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                text, label = parse_fasttext_line(line)
                if not text or not label:
                    continue
                
                seen += 1
                if len(sample) < sample_size:
                    sample.append((text, label))
                else:
                    # Reservoir sampling: randomly replace
                    j = np.random.randint(1, seen + 1)
                    if j <= sample_size:
                        sample[j - 1] = (text, label)
                
                # Progress indicator
                if seen % 100000 == 0:
                    print(f"   Processed {seen:,} reviews...")
        
        texts = [s[0] for s in sample]
        labels = [s[1] for s in sample]
    
    df = pd.DataFrame({"text": texts, "label": labels})
    print(f"✅ Loaded {len(df):,} reviews")
    print(f"   Positive: {(df['label'] == 'positive').sum():,}")
    print(f"   Negative: {(df['label'] == 'negative').sum():,}")
    
    return df


def apply_neutral_threshold(y_proba, pos_threshold=0.65, neg_threshold=0.35):
    """
    Convert binary probabilities to 3-class labels using thresholds.
    
    Args:
        y_proba: Probability of positive class (from LR.predict_proba)
        pos_threshold: Threshold for positive class
        neg_threshold: Threshold for negative class
    
    Returns:
        Array of labels: 'positive', 'neutral', 'negative'
    """
    labels = []
    for prob_pos in y_proba:
        if prob_pos >= pos_threshold:
            labels.append("positive")
        elif prob_pos <= neg_threshold:
            labels.append("negative")
        else:
            labels.append("neutral")
    return np.array(labels)


def evaluate_model(y_true, y_pred, label="Model"):
    """Print comprehensive evaluation metrics"""
    print(f"\n{'='*60}")
    print(f"📊 {label} Evaluation")
    print(f"{'='*60}")
    
    print(f"\n🎯 Accuracy: {accuracy_score(y_true, y_pred):.4f}")
    print(f"📈 F1-Score (weighted): {f1_score(y_true, y_pred, average='weighted'):.4f}")
    
    print(f"\n📋 Classification Report:")
    print(classification_report(y_true, y_pred))
    
    print(f"\n🔢 Confusion Matrix:")
    cm = confusion_matrix(y_true, y_pred, labels=["positive", "neutral", "negative"])
    print(f"              Predicted")
    print(f"              Pos    Neu    Neg")
    print(f"Actual Pos  {cm[0][0]:5d}  {cm[0][1]:5d}  {cm[0][2]:5d}")
    print(f"       Neu  {cm[1][0]:5d}  {cm[1][1]:5d}  {cm[1][2]:5d}")
    print(f"       Neg  {cm[2][0]:5d}  {cm[2][1]:5d}  {cm[2][2]:5d}")
    print(f"{'='*60}\n")


# ==================== MAIN TRAINING PIPELINE ====================

def main():
    print("\n" + "="*60)
    print("🚀 SENTIMENT MODEL TRAINING - TF-IDF + Logistic Regression")
    print("="*60 + "\n")
    
    start_time = time.time()
    
    # Step 1: Load data
    df = load_amazon_data(RAW_DATA_PATH, sample_size=SAMPLE_SIZE)
    
    # Step 2: Train/test split
    print(f"\n📊 Splitting data: {int((1-TEST_SIZE)*100)}% train, {int(TEST_SIZE*100)}% test")
    X_train, X_test, y_train_binary, y_test_binary = train_test_split(
        df['text'], 
        df['label'], 
        test_size=TEST_SIZE, 
        random_state=42,
        stratify=df['label']  # Maintain class balance
    )
    print(f"   Train: {len(X_train):,} | Test: {len(X_test):,}")
    
    # Step 3: TF-IDF Vectorization
    print(f"\n🔤 TF-IDF Vectorization...")
    print(f"   Max features: {MAX_FEATURES:,}")
    print(f"   N-gram range: {NGRAM_RANGE}")
    
    vectorizer = TfidfVectorizer(
        max_features=MAX_FEATURES,
        ngram_range=NGRAM_RANGE,
        min_df=2,  # Ignore terms that appear in less than 2 documents
        max_df=0.95,  # Ignore terms that appear in more than 95% of documents
        strip_accents='unicode',
        lowercase=True,
        stop_words='english'
    )
    
    X_train_tfidf = vectorizer.fit_transform(X_train)
    X_test_tfidf = vectorizer.transform(X_test)
    print(f"✅ Vocabulary size: {len(vectorizer.vocabulary_):,} words")
    
    # Step 4: Train Logistic Regression (binary classification)
    print(f"\n🤖 Training Logistic Regression...")
    print(f"   Max iterations: {MAX_ITER}")
    
    lr_model = LogisticRegression(
        max_iter=MAX_ITER,
        random_state=42,
        class_weight='balanced',  # Handle class imbalance
        solver='lbfgs',
        verbose=1
    )
    
    lr_model.fit(X_train_tfidf, y_train_binary)
    print(f"✅ Training complete!")
    
    # Step 5: Binary predictions (before applying neutral threshold)
    print(f"\n📊 Evaluating binary classification (positive/negative)...")
    y_pred_binary = lr_model.predict(X_test_tfidf)
    binary_accuracy = accuracy_score(y_test_binary, y_pred_binary)
    print(f"   Binary Accuracy: {binary_accuracy:.4f}")
    
    # Step 6: Apply neutral threshold to create 3-class predictions
    print(f"\n🎯 Applying neutral class thresholds...")
    print(f"   Positive threshold: >= {POSITIVE_THRESHOLD}")
    print(f"   Negative threshold: <= {NEGATIVE_THRESHOLD}")
    print(f"   Neutral range: {NEGATIVE_THRESHOLD} - {POSITIVE_THRESHOLD}")
    
    # Get probabilities for positive class
    y_proba_test = lr_model.predict_proba(X_test_tfidf)[:, 1]  # Prob of positive
    y_pred_3class = apply_neutral_threshold(y_proba_test, POSITIVE_THRESHOLD, NEGATIVE_THRESHOLD)
    
    # For ground truth, we don't have neutral labels, so we apply same threshold
    y_proba_train = lr_model.predict_proba(X_train_tfidf)[:, 1]
    y_test_3class = apply_neutral_threshold(y_proba_test, POSITIVE_THRESHOLD, NEGATIVE_THRESHOLD)
    
    # Count neutral predictions
    neutral_count = (y_pred_3class == "neutral").sum()
    neutral_pct = (neutral_count / len(y_pred_3class)) * 100
    print(f"   Neutral predictions: {neutral_count:,} ({neutral_pct:.1f}%)")
    
    # Step 7: Evaluate 3-class model
    evaluate_model(y_test_3class, y_pred_3class, "3-Class Model (with Neutral)")
    
    # Step 8: Test on sample texts
    print(f"\n🧪 Testing on sample texts:")
    test_samples = [
        "This product is absolutely amazing! Best purchase ever!",
        "Terrible quality. Complete waste of money. Do not buy!",
        "It's okay, nothing special but works fine.",
        "The item arrived on time.",
        "Love it! Highly recommend to everyone!",
        "Broken after one day. Very disappointed."
    ]
    
    for text in test_samples:
        text_tfidf = vectorizer.transform([text])
        prob_pos = lr_model.predict_proba(text_tfidf)[0, 1]
        pred_label = apply_neutral_threshold([prob_pos], POSITIVE_THRESHOLD, NEGATIVE_THRESHOLD)[0]
        print(f"   '{text[:50]}...'")
        print(f"      → {pred_label.upper()} (confidence: {max(prob_pos, 1-prob_pos):.3f})\n")
    
    # Step 9: Save model
    print(f"\n💾 Saving model to: {MODEL_OUTPUT_PATH}")
    
    model_data = {
        'vectorizer': vectorizer,
        'classifier': lr_model,
        'model_version': MODEL_VERSION,
        'positive_threshold': POSITIVE_THRESHOLD,
        'negative_threshold': NEGATIVE_THRESHOLD,
        'training_info': {
            'sample_size': SAMPLE_SIZE,
            'binary_accuracy': binary_accuracy,
            'max_features': MAX_FEATURES,
            'ngram_range': NGRAM_RANGE,
            'trained_on': time.strftime('%Y-%m-%d %H:%M:%S')
        }
    }
    
    output_path = Path(MODEL_OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with output_path.open('wb') as f:
        pickle.dump(model_data, f)
    
    print(f"✅ Model saved successfully!")
    
    # Step 10: Summary
    elapsed_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"✅ TRAINING COMPLETE!")
    print(f"{'='*60}")
    print(f"⏱️  Total time: {elapsed_time:.1f} seconds ({elapsed_time/60:.1f} minutes)")
    print(f"📊 Binary accuracy: {binary_accuracy:.4f}")
    print(f"📁 Model saved: {MODEL_OUTPUT_PATH}")
    print(f"🏷️  Version: {MODEL_VERSION}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()

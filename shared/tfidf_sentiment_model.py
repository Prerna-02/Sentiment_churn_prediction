"""
TF-IDF Sentiment Model Wrapper
Provides consistent interface for TF-IDF + Logistic Regression model
"""

import pickle
import numpy as np
from pathlib import Path
from typing import Tuple, Dict


class TfidfSentimentModel:
    """
    Wrapper for TF-IDF + Logistic Regression sentiment model.
    Provides same interface as SentimentModel3Class for backward compatibility.
    """
    
    def __init__(self, vectorizer, classifier, model_version, 
                 positive_threshold=0.65, negative_threshold=0.35):
        """
        Initialize model with TF-IDF vectorizer and classifier.
        
        Args:
            vectorizer: Fitted TfidfVectorizer
            classifier: Trained LogisticRegression classifier
            model_version: Version string for tracking
            positive_threshold: Probability threshold for positive class
            negative_threshold: Probability threshold for negative class
        """
        self.vectorizer = vectorizer
        self.classifier = classifier
        self.model_version = model_version
        self.positive_threshold = positive_threshold
        self.negative_threshold = negative_threshold
    
    def predict_one(self, text: str) -> Tuple[str, int, float, Dict[str, float]]:
        """
        Predict sentiment for a single text.
        
        Args:
            text: Input text to classify
        
        Returns:
            Tuple of (label, score, confidence, probabilities)
            - label: "positive", "negative", or "neutral"
            - score: 1 (positive), -1 (negative), 0 (neutral)
            - confidence: float between 0 and 1
            - probabilities: dict with class probabilities
        """
        # Vectorize text
        text_tfidf = self.vectorizer.transform([text])
        
        # Get prediction probabilities
        # classifier.predict_proba returns [prob_negative, prob_positive]
        proba = self.classifier.predict_proba(text_tfidf)[0]
        prob_negative = proba[0]  # Probability of negative class
        prob_positive = proba[1]  # Probability of positive class
        
        # Apply thresholds to determine label
        if prob_positive >= self.positive_threshold:
            label = "positive"
            score = 1
            confidence = prob_positive
        elif prob_positive <= self.negative_threshold:
            label = "negative"
            score = -1
            confidence = prob_negative
        else:
            # Neutral (ambiguous)
            label = "neutral"
            score = 0
            # Confidence for neutral is how close to 0.5 (most uncertain)
            confidence = 1.0 - abs(prob_positive - 0.5) * 2
        
        # Prepare probability dict
        probs = {
            "positive": float(prob_positive),
            "negative": float(prob_negative),
            "neutral": float(1.0 - abs(prob_positive - 0.5) * 2) if label == "neutral" else 0.0
        }
        
        return label, score, float(confidence), probs
    
    @classmethod
    def load(cls, model_path: str) -> 'TfidfSentimentModel':
        """
        Load model from pickle file.
        
        Args:
            model_path: Path to pickled model file
        
        Returns:
            TfidfSentimentModel instance
        """
        model_file = Path(model_path)
        if not model_file.exists():
            raise FileNotFoundError(f"Model file not found: {model_path}")
        
        with model_file.open('rb') as f:
            model_data = pickle.load(f)
        
        # Extract components
        vectorizer = model_data['vectorizer']
        classifier = model_data['classifier']
        model_version = model_data['model_version']
        positive_threshold = model_data.get('positive_threshold', 0.65)
        negative_threshold = model_data.get('negative_threshold', 0.35)
        
        return cls(
            vectorizer=vectorizer,
            classifier=classifier,
            model_version=model_version,
            positive_threshold=positive_threshold,
            negative_threshold=negative_threshold
        )
    
    def save(self, model_path: str):
        """
        Save model to pickle file.
        
        Args:
            model_path: Path to save model
        """
        model_data = {
            'vectorizer': self.vectorizer,
            'classifier': self.classifier,
            'model_version': self.model_version,
            'positive_threshold': self.positive_threshold,
            'negative_threshold': self.negative_threshold
        }
        
        model_file = Path(model_path)
        model_file.parent.mkdir(parents=True, exist_ok=True)
        
        with model_file.open('wb') as f:
            pickle.dump(model_data, f)

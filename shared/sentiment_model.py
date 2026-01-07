from __future__ import annotations

import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, Any, Optional

import numpy as np


@dataclass
class Sentiment3ClassConfig:
    """
    3-class thresholds:
      if p_pos >= pos_threshold -> positive
      if p_pos <= neg_threshold -> negative
      else -> neutral
    """
    pos_threshold: float = 0.65
    neg_threshold: float = 0.35


class SentimentModel3Class:
    """
    Wraps a binary classifier (pos vs neg) and converts to 3-class using thresholds.
    Confidence (one number) = max(p_pos, 1 - p_pos)
    """
    def __init__(self, vectorizer, clf, cfg: Optional[Sentiment3ClassConfig] = None, model_version: str = "v1"):
        self.vectorizer = vectorizer
        self.clf = clf
        self.cfg = cfg or Sentiment3ClassConfig()
        self.model_version = model_version

    def predict_one(self, text: str) -> Tuple[str, int, float, Dict[str, float]]:
        X = self.vectorizer.transform([text])

        # Get p_pos
        if hasattr(self.clf, "predict_proba"):
            # expecting [p_neg, p_pos]
            p_neg, p_pos = self.clf.predict_proba(X)[0]
        else:
            # fallback from decision_function
            score = float(self.clf.decision_function(X)[0])
            p_pos = 1.0 / (1.0 + np.exp(-score))
            p_neg = 1.0 - p_pos

        # 3-class decision
        if p_pos >= self.cfg.pos_threshold:
            label, score3 = "positive", 1
        elif p_pos <= self.cfg.neg_threshold:
            label, score3 = "negative", -1
        else:
            label, score3 = "neutral", 0

        # one-number confidence
        confidence = float(max(p_pos, 1.0 - p_pos))

        probs = {"p_pos": float(p_pos), "p_neg": float(p_neg)}
        return label, score3, confidence, probs

    def save(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as f:
            pickle.dump(self, f)

    @staticmethod
    def load(path: str | Path) -> "SentimentModel3Class":
        path = Path(path)
        with path.open("rb") as f:
            return pickle.load(f)

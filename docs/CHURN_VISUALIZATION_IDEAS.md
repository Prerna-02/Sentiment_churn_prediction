# Churn Visualization Ideas for the Dashboard

The dashboard is titled "Sentiment & Churn" but currently shows only **sentiment** (reviews, negative %, sentiment trend, fix-first, channels, word cloud, alerts). There are no visualizations that explicitly support **churn analysis**. Below are options we can add, using the data we already have (`reviews_enriched`: `customer_id`, `product_id`, `sentiment_label`, `sentiment_score`, `confidence`, `channel`, `timestamp_utc`).

---

## 1. **Churn risk index / KPI**
- **What:** A single KPI (e.g. 0–100) combining negative %, sentiment trend, and volume.
- **Why:** Frames “how bad is it?” as “churn risk” without new data.
- **Data:** Reuse KPIs (negative %, high‑risk product count) or add a small backend aggregate.

## 2. **Churn risk over time (line chart)**
- **What:** Line chart of “churn risk” or “negative %” (or “high‑risk product count”) over time, same buckets as Sentiment Trend.
- **Why:** Shows whether churn risk is improving or worsening.
- **Data:** Reuse sentiment-trend aggregation; optionally add a derived “risk” series.

## 3. **Top churn‑risk products**
- **What:** Table or bar chart of products ranked by churn risk (e.g. negative % × volume or reuse Fix‑First logic).
- **Why:** Complements Fix‑First by explicitly framing as “churn risk.”
- **Data:** Reuse Fix‑First API; optionally add a dedicated “churn‑risk” ranking.

## 4. **Customers at risk**
- **What:** Count (or list) of customers with ≥ N negative reviews in the time window.
- **Why:** Connects negative sentiment to “customers we might lose.”
- **Data:** Aggregate by `customer_id`, count negative reviews; new API endpoint.

## 5. **Churn risk by channel**
- **What:** Bar or pie chart of “churn risk” or “negative %” by channel (app, web, email, call center).
- **Why:** Shows which channels drive the most at‑risk feedback.
- **Data:** Reuse or extend channel‑breakdown API with negative‑focused metrics.

## 6. **Sentiment → churn proxy section**
- **What:** Dedicated “Churn proxy” strip: e.g. “Products at risk,” “Customers at risk,” “Churn risk trend” (one small chart or KPI each).
- **Why:** Makes churn explicit without changing the rest of the layout much.

---

## Recommendation

- **Quick wins:** (2) Churn risk over time, (3) Top churn‑risk products. Both can mostly reuse existing APIs (sentiment trend, fix‑first) and add clear “churn” labeling.
- **If we add one new aggregate:** (4) Customers at risk. Requires a new backend endpoint (e.g. `GET /api/churn/customers-at-risk`).

We can pick 1–2 of these to implement next and iterate from there.

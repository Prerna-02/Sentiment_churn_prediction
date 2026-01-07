# Kafka Topics

## 1) reviews_raw

Raw customer messages coming into the system.
Schema: ReviewEvent

## 2) reviews_enriched

Same message after adding sentiment + churn risk.
Schema: EnrichedReviewEvent

# Schemas

## ReviewEvent

- event_id: unique id for each message
- customer_id: customer identifier
- text: customer message text
- channel: where it came from (app/web/email/callcenter/social)
- timestamp_utc: when message was created
- metadata: optional extra fields

## EnrichedReviewEvent

Everything in ReviewEvent plus:

- sentiment_label: negative/neutral/positive
- sentiment_score: numeric sentiment score
- churn_risk_score: 0 to 1
- churn_risk_label: low/medium/high
- model_version: version string for traceability

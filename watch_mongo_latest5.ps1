Write-Host "MongoDB: showing latest 5 docs from itd.reviews_enriched (enriched output)..." -ForegroundColor Cyan

docker exec mongo mongosh --quiet "mongodb://localhost:27017/itd" --eval 'db.reviews_enriched.find({}, { _id: 0, event_id: 1, customer_id: 1, product_id: 1, product_name: 1, text: 1, channel: 1, timestamp_utc: 1, sentiment_label: 1, sentiment_score: 1, confidence: 1, model_version: 1 }).sort({ _id: -1 }).limit(5).pretty()'

Write-Host "`nDone." -ForegroundColor Green

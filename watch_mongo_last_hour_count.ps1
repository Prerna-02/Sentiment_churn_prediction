Write-Host "MongoDB: total + last-1h review count (every 10s; Ctrl+C to stop)..." -ForegroundColor Cyan
$js = @'
var c=new Date(Date.now()-3600000).toISOString();
var t=db.reviews_enriched.countDocuments({});
var n=db.reviews_enriched.countDocuments({timestamp_utc:{$gte:c}});
var l=db.reviews_enriched.find({},{timestamp_utc:1}).sort({_id:-1}).limit(1).toArray()[0];
print('Total: '+t+' | Last1h: '+n+' | Latest: '+(l?l.timestamp_utc:'N/A'));
'@
while ($true) {
    docker exec mongo mongosh --quiet "mongodb://localhost:27017/itd" --eval $js
    Start-Sleep -Seconds 10
}

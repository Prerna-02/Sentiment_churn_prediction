# MongoDB Queries Reference - Working Examples

## 🔌 How to Connect to MongoDB

**IMPORTANT:** These queries must be run inside the MongoDB shell, NOT in PowerShell!

```bash
# Step 1: Connect to MongoDB container
docker exec -it mongo mongosh

# Step 2: Switch to the database
use itd

# Step 3: Now you can run the queries below
```

---

## 📊 Basic Queries

### Count all reviews
```javascript
db.reviews_enriched.countDocuments({})
```

### Get latest 10 reviews
```javascript
db.reviews_enriched.find().sort({ timestamp_utc: -1 }).limit(10)
```

### Get one sample review (formatted)
```javascript
db.reviews_enriched.findOne()
```

---

## ✏️ CRUD Operations - WORKING EXAMPLES

### ✅ CREATE (Insert) - 3 Working Examples

#### Example 1: Insert a positive review
```javascript
db.reviews_enriched.insertOne({
  event_id: "test-event-001",
  customer_id: "C9001",
  product_id: "AMZN-TEST001",
  product_name: "Wireless Headphones",
  text: "Amazing sound quality! Best purchase ever.",
  channel: "web",
  timestamp_utc: new Date().toISOString(),
  sentiment_label: "positive",
  sentiment_score: 0.95,
  confidence: 0.92
})
```

#### Example 2: Insert a negative review
```javascript
db.reviews_enriched.insertOne({
  event_id: "test-event-002",
  customer_id: "C9002",
  product_id: "AMZN-TEST002",
  product_name: "Bluetooth Speaker",
  text: "Terrible quality. Stopped working after 2 days.",
  channel: "app",
  timestamp_utc: new Date().toISOString(),
  sentiment_label: "negative",
  sentiment_score: -0.85,
  confidence: 0.88
})
```

#### Example 3: Insert a neutral review
```javascript
db.reviews_enriched.insertOne({
  event_id: "test-event-003",
  customer_id: "C9003",
  product_id: "AMZN-TEST003",
  product_name: "USB Cable",
  text: "It works as expected. Nothing special.",
  channel: "email",
  timestamp_utc: new Date().toISOString(),
  sentiment_label: "neutral",
  sentiment_score: 0.05,
  confidence: 0.75
})
```

---

### 📖 READ (Find) - 3 Working Examples

#### Example 1: Find all reviews from a specific customer
```javascript
db.reviews_enriched.find({ customer_id: "C9001" })
```

#### Example 2: Find all negative reviews for a product
```javascript
db.reviews_enriched.find({ 
  product_id: "AMZN-TEST002",
  sentiment_label: "negative" 
})
```

#### Example 3: Find reviews with high confidence (>90%)
```javascript
db.reviews_enriched.find({ 
  confidence: { $gt: 0.9 } 
}).limit(10)
```

---

### 🔄 UPDATE - 3 Working Examples

#### Example 1: Update review text for a test review
```javascript
db.reviews_enriched.updateOne(
  { customer_id: "C9001" },
  { $set: { text: "Updated: Amazing sound quality! Highly recommended!" } }
)
```

#### Example 2: Update sentiment for a review
```javascript
db.reviews_enriched.updateOne(
  { customer_id: "C9002" },
  { $set: { 
    sentiment_label: "negative",
    sentiment_score: -0.95,
    confidence: 0.93
  }}
)
```

#### Example 3: Update product name for all test products
```javascript
db.reviews_enriched.updateMany(
  { product_id: { $regex: /^AMZN-TEST/ } },
  { $set: { product_name: "DEMO PRODUCT - FOR TESTING" } }
)
```

---

### 🗑️ DELETE - 3 Working Examples

#### Example 1: Delete a specific test review
```javascript
db.reviews_enriched.deleteOne({ customer_id: "C9001" })
```

#### Example 2: Delete all reviews from a test customer
```javascript
db.reviews_enriched.deleteMany({ customer_id: "C9002" })
```

#### Example 3: Delete all test reviews (products starting with AMZN-TEST)
```javascript
db.reviews_enriched.deleteMany({ 
  product_id: { $regex: /^AMZN-TEST/ } 
})
```

---

## 😊 Sentiment Analysis

### Count by sentiment
```javascript
db.reviews_enriched.aggregate([
  { $group: { _id: "$sentiment_label", count: { $sum: 1 } } }
])
```

### Get all positive reviews
```javascript
db.reviews_enriched.find({ sentiment_label: "positive" }).limit(10)
```

### Get all negative reviews
```javascript
db.reviews_enriched.find({ sentiment_label: "negative" }).limit(10)
```

### Get high-confidence negative reviews (>90%)
```javascript
db.reviews_enriched.find({ 
  sentiment_label: "negative", 
  confidence: { $gt: 0.9 } 
}).limit(10)
```

---

## 📱 Channel Analysis

### Count reviews by channel
```javascript
db.reviews_enriched.aggregate([
  { $group: { _id: "$channel", count: { $sum: 1 } } }
])
```

### Get web channel reviews
```javascript
db.reviews_enriched.find({ channel: "web" }).limit(10)
```

### Sentiment distribution by channel
```javascript
db.reviews_enriched.aggregate([
  { $group: { 
    _id: { channel: "$channel", sentiment: "$sentiment_label" }, 
    count: { $sum: 1 } 
  }}
])
```

---

## 🛍️ Product Analysis

### Top 10 products by review count
```javascript
db.reviews_enriched.aggregate([
  { $group: { 
    _id: "$product_id", 
    count: { $sum: 1 }, 
    product_name: { $first: "$product_name" } 
  }}, 
  { $sort: { count: -1 } }, 
  { $limit: 10 }
])
```

### Get reviews for specific product
```javascript
db.reviews_enriched.find({ product_id: "AMZN-A7F3B2C1D4E5" }).limit(10)
```

### Products with most negative reviews
```javascript
db.reviews_enriched.aggregate([
  { $match: { sentiment_label: "negative" } }, 
  { $group: { 
    _id: "$product_id", 
    count: { $sum: 1 }, 
    product_name: { $first: "$product_name" } 
  }}, 
  { $sort: { count: -1 } }, 
  { $limit: 10 }
])
```

---

## ⏰ Time-Based Queries

### Reviews from last 24 hours
```javascript
db.reviews_enriched.find({ 
  timestamp_utc: { 
    $gte: new Date(Date.now() - 24*60*60*1000).toISOString() 
  } 
})
```

### Reviews from last 7 days
```javascript
db.reviews_enriched.find({ 
  timestamp_utc: { 
    $gte: new Date(Date.now() - 7*24*60*60*1000).toISOString() 
  } 
})
```

### Count reviews by day (last 7 days)
```javascript
db.reviews_enriched.aggregate([
  { $match: { 
    timestamp_utc: { 
      $gte: new Date(Date.now() - 7*24*60*60*1000).toISOString() 
    }
  }},
  { $group: { 
    _id: { 
      $dateToString: { 
        format: "%Y-%m-%d", 
        date: { $toDate: "$timestamp_utc" } 
      } 
    }, 
    count: { $sum: 1 } 
  }}, 
  { $sort: { _id: -1 } }
])
```

---

## 🔍 Advanced Queries

### Average confidence by sentiment
```javascript
db.reviews_enriched.aggregate([
  { $group: { 
    _id: "$sentiment_label", 
    avg_confidence: { $avg: "$confidence" } 
  }}
])
```

### Reviews with specific keywords
```javascript
db.reviews_enriched.find({ 
  text: /amazing|excellent|great/i 
}).limit(10)
```

### Low confidence predictions (< 70%)
```javascript
db.reviews_enriched.find({ 
  confidence: { $lt: 0.7 } 
}).limit(10)
```

---

## 🗑️ Cleanup Queries

### Delete all test reviews (SAFE - only deletes test data)
```javascript
db.reviews_enriched.deleteMany({ 
  product_id: { $regex: /^AMZN-TEST/ } 
})
```

### Delete old reviews (older than 30 days) - USE WITH CAUTION!
```javascript
db.reviews_enriched.deleteMany({ 
  timestamp_utc: { 
    $lt: new Date(Date.now() - 30*24*60*60*1000).toISOString() 
  } 
})
```

---

## 📈 Export Data

### Export to JSON
```bash
# Run this in PowerShell, NOT in mongosh
docker exec mongo mongoexport --db=itd --collection=reviews_enriched --out=/tmp/reviews.json
docker cp mongo:/tmp/reviews.json ./reviews.json
```

### Export to CSV
```bash
# Run this in PowerShell, NOT in mongosh
docker exec mongo mongoexport --db=itd --collection=reviews_enriched --type=csv --fields=customer_id,product_name,text,sentiment_label,confidence,channel,timestamp_utc --out=/tmp/reviews.csv
docker cp mongo:/tmp/reviews.csv ./reviews.csv
```

---

## 💡 Quick Tips

### Format output nicely
```javascript
db.reviews_enriched.find().pretty()
```

### Limit results
```javascript
db.reviews_enriched.find().limit(5)
```

### Count results
```javascript
db.reviews_enriched.aggregate([
  { $group: { 
    _id: "$product_id", 
    count: { $sum: 1 }, 
    product_name: { $first: "$product_name" } 
  }}, 
  { $sort: { count: -1 } }, 
  { $limit: 10 }
])
```

### Explain query performance
```javascript
db.reviews_enriched.find({ sentiment_label: "positive" }).explain()
```

### Exit MongoDB shell
```javascript
exit
```

---

## 🎯 Presentation Demo Flow

### Step 1: Show database stats
```javascript
db.reviews_enriched.countDocuments({})
db.reviews_enriched.aggregate([
  { $group: { _id: "$sentiment_label", count: { $sum: 1 } } }
])
```

### Step 2: Insert a test review
```javascript
db.reviews_enriched.insertOne({
  event_id: "demo-001",
  customer_id: "C9999",
  product_id: "AMZN-DEMO",
  product_name: "Demo Product",
  text: "This is a live demo review!",
  channel: "web",
  timestamp_utc: new Date().toISOString(),
  sentiment_label: "positive",
  sentiment_score: 0.9,
  confidence: 0.85
})
```

### Step 3: Find the review you just created
```javascript
db.reviews_enriched.find({ customer_id: "C9999" })
```

### Step 4: Update it
```javascript
db.reviews_enriched.updateOne(
  { customer_id: "C9999" },
  { $set: { text: "Updated during live demo!" } }
)
```

### Step 5: Verify the update
```javascript
db.reviews_enriched.find({ customer_id: "C9999" })
```

### Step 6: Delete it
```javascript
db.reviews_enriched.deleteOne({ customer_id: "C9999" })
```

### Step 7: Verify deletion
```javascript
db.reviews_enriched.find({ customer_id: "C9999" })
```

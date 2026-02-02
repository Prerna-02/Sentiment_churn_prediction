# services/dashboard_api/app/utils/mongodb.py

import os
from pymongo import MongoClient, DESCENDING
from pymongo.database import Database


class MongoDBConnection:
    """MongoDB connection manager for dashboard API."""
    
    def __init__(self):
        self.uri = os.getenv("MONGO_URI", "mongodb://mongo:27017")
        self.db_name = os.getenv("MONGO_DB", "itd")
        self.collection_name = os.getenv("MONGO_COLLECTION", "reviews_enriched")
        self.client: MongoClient = None
        self.db: Database = None
    
    def connect(self):
        """Establish MongoDB connection."""
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            print(f"✅ Connected to MongoDB: {self.uri}/{self.db_name}")
            return True
        except Exception as e:
            print(f"❌ MongoDB connection failed: {e}")
            return False
    
    def get_collection(self):
        """Get the reviews_enriched collection."""
        if self.db is None:
            raise Exception("MongoDB not connected")
        return self.db[self.collection_name]
    
    def ensure_indexes(self):
        """Create necessary indexes for optimal query performance."""
        if self.db is None:
            return
        
        collection = self.get_collection()
        
        # Single field indexes
        collection.create_index([("timestamp_utc", DESCENDING)])
        collection.create_index("product_id")
        collection.create_index("channel")
        collection.create_index("sentiment_label")
        
        # Compound indexes for common queries
        collection.create_index([("timestamp_utc", DESCENDING), ("product_id", 1)])
        collection.create_index([("timestamp_utc", DESCENDING), ("channel", 1)])
        
        print("✅ MongoDB indexes created/verified")
    
    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            print("MongoDB connection closed")


# Global connection instance
mongo_conn = MongoDBConnection()



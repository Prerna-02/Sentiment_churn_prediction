# services/dashboard_api/app/routers/mongo_query.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, List
import json

from app.utils.mongodb import mongo_conn


router = APIRouter()


class MongoQueryRequest(BaseModel):
    """Schema for MongoDB query execution request."""
    query: str
    
    class Config:
        schema_extra = {
            "example": {
                "query": "db.reviews_enriched.find({sentiment_label: 'positive'}).limit(5)"
            }
        }


class MongoQueryResponse(BaseModel):
    """Schema for MongoDB query execution response."""
    success: bool
    result: Any
    count: int
    query_executed: str
    error: str = None


@router.post("/execute-query", response_model=MongoQueryResponse)
async def execute_mongo_query(request: MongoQueryRequest):
    """
    Execute a raw MongoDB query and return results.
    
    Supported query formats:
    - db.reviews_enriched.find({})
    - db.reviews_enriched.find({sentiment_label: 'positive'})
    - db.reviews_enriched.countDocuments({})
    - db.reviews_enriched.aggregate([...])
    - db.reviews_enriched.insertOne({...})
    - db.reviews_enriched.updateOne({filter}, {update})
    - db.reviews_enriched.deleteOne({filter})
    
    Returns:
        - success: bool
        - result: Query results (list of documents or operation result)
        - count: Number of documents returned
        - query_executed: The query that was executed
        - error: Error message if query failed
    """
    try:
        collection = mongo_conn.get_collection()
        query = request.query.strip()
        
        # Remove "db.reviews_enriched." prefix if present
        if query.startswith("db.reviews_enriched."):
            query = query.replace("db.reviews_enriched.", "")
        elif query.startswith("db."):
            return {
                "success": False,
                "result": None,
                "count": 0,
                "query_executed": request.query,
                "error": "Only 'reviews_enriched' collection is supported. Use: db.reviews_enriched.find({})"
            }
        
        # Parse and execute query
        result = None
        count = 0
        
        # FIND queries
        if query.startswith("find("):
            # Extract filter from find(...)
            filter_str = query[5:-1]  # Remove "find(" and ")"
            
            # Handle empty filter
            if not filter_str or filter_str.strip() == "":
                filter_dict = {}
            else:
                # Convert JavaScript object notation to Python dict
                filter_str = filter_str.replace("'", '"')  # Single quotes to double quotes
                try:
                    filter_dict = json.loads(filter_str)
                except json.JSONDecodeError:
                    # Try evaluating as Python dict
                    filter_dict = eval(filter_str)
            
            # Execute find
            cursor = collection.find(filter_dict).limit(100)  # Limit to 100 for safety
            result = list(cursor)
            count = len(result)
            
            # Convert ObjectId to string for JSON serialization
            for doc in result:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
        
        # COUNT queries
        elif query.startswith("countDocuments("):
            filter_str = query[15:-1]  # Remove "countDocuments(" and ")"
            
            if not filter_str or filter_str.strip() == "":
                filter_dict = {}
            else:
                filter_str = filter_str.replace("'", '"')
                filter_dict = json.loads(filter_str)
            
            count = collection.count_documents(filter_dict)
            result = {"count": count}
        
        # AGGREGATE queries
        elif query.startswith("aggregate("):
            pipeline_str = query[10:-1]  # Remove "aggregate(" and ")"
            pipeline_str = pipeline_str.replace("'", '"')
            pipeline = json.loads(pipeline_str)
            
            cursor = collection.aggregate(pipeline)
            result = list(cursor)
            count = len(result)
            
            # Convert ObjectId to string
            for doc in result:
                if '_id' in doc and hasattr(doc['_id'], '__str__'):
                    doc['_id'] = str(doc['_id'])
        
        # INSERT queries
        elif query.startswith("insertOne("):
            doc_str = query[10:-1]  # Remove "insertOne(" and ")"
            doc_str = doc_str.replace("'", '"')
            doc = json.loads(doc_str)
            
            insert_result = collection.insert_one(doc)
            result = {
                "inserted_id": str(insert_result.inserted_id),
                "acknowledged": insert_result.acknowledged
            }
            count = 1
        
        # UPDATE queries
        elif query.startswith("updateOne(") or query.startswith("updateMany("):
            is_many = query.startswith("updateMany(")
            prefix_len = 11 if is_many else 10
            
            # Extract filter and update
            params_str = query[prefix_len:-1]
            # This is simplified - in production, use proper parsing
            parts = params_str.split("}, {")
            filter_str = parts[0] + "}"
            update_str = "{" + parts[1]
            
            filter_str = filter_str.replace("'", '"')
            update_str = update_str.replace("'", '"')
            
            filter_dict = json.loads(filter_str)
            update_dict = json.loads(update_str)
            
            if is_many:
                update_result = collection.update_many(filter_dict, update_dict)
            else:
                update_result = collection.update_one(filter_dict, update_dict)
            
            result = {
                "matched_count": update_result.matched_count,
                "modified_count": update_result.modified_count,
                "acknowledged": update_result.acknowledged
            }
            count = update_result.modified_count
        
        # DELETE queries
        elif query.startswith("deleteOne(") or query.startswith("deleteMany("):
            is_many = query.startswith("deleteMany(")
            prefix_len = 11 if is_many else 10
            
            filter_str = query[prefix_len:-1]
            filter_str = filter_str.replace("'", '"')
            filter_dict = json.loads(filter_str)
            
            if is_many:
                delete_result = collection.delete_many(filter_dict)
            else:
                delete_result = collection.delete_one(filter_dict)
            
            result = {
                "deleted_count": delete_result.deleted_count,
                "acknowledged": delete_result.acknowledged
            }
            count = delete_result.deleted_count
        
        else:
            return {
                "success": False,
                "result": None,
                "count": 0,
                "query_executed": request.query,
                "error": f"Unsupported query type. Supported: find, countDocuments, aggregate, insertOne, updateOne, updateMany, deleteOne, deleteMany"
            }
        
        return {
            "success": True,
            "result": result,
            "count": count,
            "query_executed": request.query,
            "error": None
        }
    
    except json.JSONDecodeError as e:
        return {
            "success": False,
            "result": None,
            "count": 0,
            "query_executed": request.query,
            "error": f"JSON parsing error: {str(e)}. Make sure to use double quotes for strings."
        }
    except Exception as e:
        return {
            "success": False,
            "result": None,
            "count": 0,
            "query_executed": request.query,
            "error": f"Query execution error: {str(e)}"
        }


@router.get("/query-examples")
async def get_query_examples():
    """Get example MongoDB queries for reference."""
    return {
        "examples": [
            {
                "name": "Find all reviews",
                "query": "db.reviews_enriched.find({})",
                "description": "Returns all reviews (limited to 100)"
            },
            {
                "name": "Find positive reviews",
                "query": 'db.reviews_enriched.find({"sentiment_label": "positive"})',
                "description": "Returns all positive reviews"
            },
            {
                "name": "Count negative reviews",
                "query": 'db.reviews_enriched.countDocuments({"sentiment_label": "negative"})',
                "description": "Returns count of negative reviews"
            },
            {
                "name": "Find reviews by customer",
                "query": 'db.reviews_enriched.find({"customer_id": "C123"})',
                "description": "Returns all reviews from customer C123"
            },
            {
                "name": "Aggregate by sentiment",
                "query": 'db.reviews_enriched.aggregate([{"$group": {"_id": "$sentiment_label", "count": {"$sum": 1}}}])',
                "description": "Groups reviews by sentiment and counts each"
            },
            {
                "name": "Insert new review",
                "query": 'db.reviews_enriched.insertOne({"customer_id": "C999", "text": "Great product!", "sentiment_label": "positive"})',
                "description": "Inserts a new review document"
            },
            {
                "name": "Update review",
                "query": 'db.reviews_enriched.updateOne({"customer_id": "C999"}, {"$set": {"text": "Updated text"}})',
                "description": "Updates the text of a review"
            },
            {
                "name": "Delete review",
                "query": 'db.reviews_enriched.deleteOne({"customer_id": "C999"})',
                "description": "Deletes a review by customer ID"
            }
        ]
    }

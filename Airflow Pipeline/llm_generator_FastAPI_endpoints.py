"""
FastAPI endpoints for Air Quality Assistant Chat Interface
Integrates with the existing Environmental Query System
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
import os
from dotenv import load_dotenv

# Import your existing system classes
from llm_generator import ImprovedEnvironmentalQuerySystem  # Replace with actual module name

load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="Air Quality Assistant API",
    description="Chat interface for environmental data queries",
    version="1.0.0"
)

# CORS middleware for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5000"),
    "dbname": os.getenv("DB_NAME", "db"),
    "user": os.getenv("DB_USER", "db_user"),
    "password": os.getenv("DB_PASSWORD", "db_password")
}

# Initialize the query system globally
query_system = None


# ============================================================================
# REQUEST/RESPONSE MODELS
# ============================================================================

class ChatMessage(BaseModel):
    """Model for chat messages"""
    message: str
    timestamp: Optional[datetime] = None


class ChatResponse(BaseModel):
    """Model for chat responses"""
    response: str
    timestamp: datetime
    needs_clarification: bool = False
    suggested_cities: Optional[List[str]] = None


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    database_connected: bool
    cities_available: int


class CityInfo(BaseModel):
    """Model for city information"""
    city: str
    country: str
    record_count: int
    earliest_date: str
    latest_date: str


class AvailableCitiesResponse(BaseModel):
    """Response with available cities"""
    cities: List[CityInfo]
    total_count: int


# ============================================================================
# STARTUP/SHUTDOWN EVENTS
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize the query system on startup"""
    global query_system
    try:
        query_system = ImprovedEnvironmentalQuerySystem(DB_CONFIG)
        print("✓ Environmental Query System initialized successfully")
    except Exception as e:
        print(f"✗ Failed to initialize query system: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown"""
    global query_system
    if query_system:
        query_system.close()
        print("✓ Query system closed successfully")


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/", tags=["Root"])
async def root():
    """Root endpoint"""
    return {
        "message": "Air Quality Assistant API",
        "version": "1.0.0",
        "endpoints": {
            "chat": "/api/chat",
            "cities": "/api/cities",
            "health": "/api/health"
        }
    }


@app.get("/api/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint
    Returns system status and database connectivity
    """
    if not query_system:
        raise HTTPException(status_code=503, detail="Query system not initialized")
    
    try:
        cities = query_system.db_manager.get_available_cities(limit=1)
        cities_count = len(query_system.db_manager.get_available_cities())
        
        return HealthResponse(
            status="healthy",
            database_connected=True,
            cities_available=cities_count
        )
    except Exception as e:
        return HealthResponse(
            status="unhealthy",
            database_connected=False,
            cities_available=0
        )


@app.post("/api/chat", response_model=ChatResponse, tags=["Chat"])
async def chat(message: ChatMessage):
    """
    Main chat endpoint for air quality queries
    
    Accepts natural language questions and returns environmental data responses
    
    Example queries:
    - "Is it safe to jog in Miami tomorrow?"
    - "What's the air quality in Los Angeles today?"
    - "Tell me about weather conditions in Toronto"
    """
    if not query_system:
        raise HTTPException(status_code=503, detail="Query system not initialized")
    
    if not message.message or not message.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    
    try:
        # Process the query using your existing system
        response_text = query_system.process_query(message.message)
        
        # Check if response is asking for clarification (city not specified)
        needs_clarification = any([
            "which city" in response_text.lower(),
            "specify a city" in response_text.lower(),
            "need to know" in response_text.lower(),
            "please specify" in response_text.lower()
        ])
        
        # Extract suggested cities if present
        suggested_cities = None
        if needs_clarification and "Some cities with available data include:" in response_text:
            # Extract city names from the response
            cities = query_system.db_manager.get_available_cities(limit=5)
            suggested_cities = [f"{c['city']}, {c['country']}" for c in cities]
        
        return ChatResponse(
            response=response_text,
            timestamp=datetime.now(),
            needs_clarification=needs_clarification,
            suggested_cities=suggested_cities
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")


@app.get("/api/cities", response_model=AvailableCitiesResponse, tags=["Cities"])
async def get_available_cities(limit: Optional[int] = 50):
    """
    Get list of available cities with air quality data
    
    Parameters:
    - limit: Maximum number of cities to return (default: 50)
    """
    if not query_system:
        raise HTTPException(status_code=503, detail="Query system not initialized")
    
    try:
        cities_data = query_system.db_manager.get_available_cities(limit=limit)
        
        cities = [
            CityInfo(
                city=c['city'],
                country=c['country'],
                record_count=c['record_count'],
                earliest_date=str(c['earliest_date']),
                latest_date=str(c['latest_date'])
            )
            for c in cities_data
        ]
        
        total_count = len(query_system.db_manager.get_available_cities())
        
        return AvailableCitiesResponse(
            cities=cities,
            total_count=total_count
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching cities: {str(e)}")


@app.get("/api/cities/search", tags=["Cities"])
async def search_cities(query: str, limit: int = 10):
    """
    Search for cities by name
    
    Parameters:
    - query: Search query (city name)
    - limit: Maximum results to return
    """
    if not query_system:
        raise HTTPException(status_code=503, detail="Query system not initialized")
    
    try:
        all_cities = query_system.db_manager.get_available_cities()
        
        # Filter cities matching the query
        matching_cities = [
            c for c in all_cities 
            if query.lower() in c['city'].lower() or query.lower() in c['country'].lower()
        ][:limit]
        
        cities = [
            CityInfo(
                city=c['city'],
                country=c['country'],
                record_count=c['record_count'],
                earliest_date=str(c['earliest_date']),
                latest_date=str(c['latest_date'])
            )
            for c in matching_cities
        ]
        
        return {
            "cities": cities,
            "total_matches": len(matching_cities)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching cities: {str(e)}")


@app.get("/api/city/{city_name}", tags=["Cities"])
async def get_city_info(city_name: str, date: Optional[str] = None):
    """
    Get detailed environmental data for a specific city
    
    Parameters:
    - city_name: Name of the city
    - date: Date for data (YYYY-MM-DD format, optional, defaults to today)
    """
    if not query_system:
        raise HTTPException(status_code=503, detail="Query system not initialized")
    
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    
    try:
        # Get comprehensive data for the city
        data = query_system.db_manager.get_comprehensive_data(
            city=city_name,
            country=None,
            date=date
        )
        
        if not data['location'] and not any([data['air_quality'], data['weather'], data['no2'], data['fire']]):
            raise HTTPException(status_code=404, detail=f"No data found for {city_name}")
        
        return data
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching city data: {str(e)}")


# ============================================================================
# EXAMPLE QUERIES ENDPOINT
# ============================================================================

@app.get("/api/examples", tags=["Help"])
async def get_example_queries():
    """Get example queries to help users"""
    return {
        "examples": [
            "Is it safe to jog in Miami tomorrow?",
            "What's the air quality in Los Angeles today?",
            "Tell me about weather conditions in Toronto",
            "What is the AQI in New York right now?",
            "Can I go for a picnic in Chicago tomorrow?",
            "Show me PM2.5 levels in San Francisco"
        ],
        "tips": [
            "Always specify a city name in your query",
            "You can ask about 'today' or 'tomorrow'",
            "Ask about air quality, weather, AQI, or outdoor activity safety",
            "Available pollutants: PM2.5, PM10, NO2, O3, CO, SO2"
        ]
    }


# ============================================================================
# RUN SERVER
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
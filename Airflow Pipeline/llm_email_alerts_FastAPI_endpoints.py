"""
FastAPI Endpoints for Environmental Query System
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
import os
from datetime import datetime
from dotenv import load_dotenv

# Import all classes from your original code
# Assuming your original code is saved as 'air_quality_system.py'
# If it's in the same file, the imports below will work directly

from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from langchain_openai import AzureChatOpenAI
from dotenv import load_dotenv
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content
from apscheduler.schedulers.background import BackgroundScheduler

# Load environment variables
load_dotenv()

# Import your existing classes here
# (Include all your existing classes: DatabaseManager, AlertGenerator, EmailSender, 
# ContinuousMonitor, AlertManagementSystem - paste them above this section)

# ============================================================================
# FASTAPI MODELS
# ============================================================================

class SubscriptionRequest(BaseModel):
    email: EmailStr
    city: str
    country: Optional[str] = None
    alert_types: Optional[List[str]] = Field(
        default=['pm25', 'pm10', 'no2', 'o3', 'co', 'so2'],
        description="Types of pollutants to monitor"
    )

class AirQualityQuery(BaseModel):
    city: str
    country: Optional[str] = None
    date: Optional[str] = Field(
        default=None,
        description="Date in YYYY-MM-DD format. Defaults to today."
    )

class MonitoringConfig(BaseModel):
    interval_minutes: int = Field(
        default=15,
        ge=5,
        le=1440,
        description="Monitoring interval in minutes (5-1440)"
    )

class UnsubscribeRequest(BaseModel):
    email: EmailStr
    city: str
    country: Optional[str] = None

# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

app = FastAPI(
    title="Air Quality Alert System API",
    description="REST API for monitoring air quality and sending email alerts",
    version="1.0.0"
)

# Global instances
db_manager = None
alert_system = None
monitor = None

# ============================================================================
# STARTUP AND SHUTDOWN
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize database connection and alert system on startup."""
    global db_manager, alert_system, monitor
    
    db_manager = DatabaseManager(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5000"),
        dbname=os.getenv("DB_NAME", "db"),
        user=os.getenv("DB_USER", "db_user"),
        password=os.getenv("DB_PASSWORD", "db_password")
    )
    
    try:
        db_manager.connect()
        db_manager.setup_alert_tables()
        alert_system = AlertManagementSystem(db_manager)
        monitor = ContinuousMonitor(db_manager)
        print("Air Quality Alert System API initialized successfully")
    except Exception as e:
        print(f"Failed to initialize system: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown."""
    global db_manager, alert_system
    
    if alert_system and alert_system.is_running:
        alert_system.stop_monitoring()
    
    if db_manager:
        db_manager.close()
    
    print("Air Quality Alert System API shut down")

# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Air Quality Alert System API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "air_quality": "/air-quality",
            "subscribe": "/subscribe",
            "unsubscribe": "/unsubscribe",
            "subscriptions": "/subscriptions",
            "monitoring": {
                "start": "/monitoring/start",
                "stop": "/monitoring/stop",
                "status": "/monitoring/status"
            },
            "alerts": "/alerts/check"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "database": "connected" if db_manager and db_manager.conn else "disconnected",
        "monitoring": "active" if alert_system and alert_system.is_running else "inactive"
    }

@app.post("/air-quality")
async def get_air_quality(query: AirQualityQuery):
    """Get current air quality data for a city."""
    if not db_manager:
        raise HTTPException(status_code=503, detail="Database not initialized")
    
    date = query.date if query.date else datetime.now().strftime("%Y-%m-%d")
    
    try:
        data = db_manager.query_air_quality(query.city, query.country, date)
        
        if not data:
            raise HTTPException(
                status_code=404,
                detail=f"No air quality data found for {query.city}"
            )
        
        return {
            "success": True,
            "data": data,
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/subscribe")
async def subscribe_to_alerts(subscription: SubscriptionRequest):
    """Subscribe to air quality alerts for a city."""
    if not db_manager:
        raise HTTPException(status_code=503, detail="Database not initialized")
    
    try:
        success = db_manager.add_alert_subscription(
            email=subscription.email,
            city=subscription.city,
            country=subscription.country,
            alert_types=subscription.alert_types
        )
        
        if success:
            return {
                "success": True,
                "message": f"Successfully subscribed {subscription.email} to alerts for {subscription.city}",
                "subscription": {
                    "email": subscription.email,
                    "city": subscription.city,
                    "country": subscription.country,
                    "alert_types": subscription.alert_types
                }
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to create subscription")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/unsubscribe")
async def unsubscribe_from_alerts(request: UnsubscribeRequest):
    """Unsubscribe from air quality alerts."""
    if not db_manager:
        raise HTTPException(status_code=503, detail="Database not initialized")
    
    try:
        cursor = db_manager.conn.cursor()
        cursor.execute("""
            UPDATE alert_subscriptions
            SET is_active = FALSE
            WHERE user_email = %s AND city = %s AND (country = %s OR (country IS NULL AND %s IS NULL))
        """, (request.email, request.city, request.country, request.country))
        
        db_manager.conn.commit()
        cursor.close()
        
        return {
            "success": True,
            "message": f"Successfully unsubscribed {request.email} from {request.city} alerts"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/subscriptions")
async def get_subscriptions(email: Optional[str] = None):
    """Get all active subscriptions, optionally filtered by email."""
    if not db_manager:
        raise HTTPException(status_code=503, detail="Database not initialized")
    
    try:
        cursor = db_manager.conn.cursor(cursor_factory=RealDictCursor)
        
        if email:
            cursor.execute("""
                SELECT user_email, city, country, alert_types, created_at
                FROM alert_subscriptions
                WHERE is_active = TRUE AND user_email = %s
            """, (email,))
        else:
            cursor.execute("""
                SELECT user_email, city, country, alert_types, created_at
                FROM alert_subscriptions
                WHERE is_active = TRUE
            """)
        
        results = cursor.fetchall()
        cursor.close()
        
        return {
            "success": True,
            "count": len(results),
            "subscriptions": results
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/monitoring/start")
async def start_monitoring(config: MonitoringConfig, background_tasks: BackgroundTasks):
    """Start continuous air quality monitoring."""
    if not alert_system:
        raise HTTPException(status_code=503, detail="Alert system not initialized")
    
    if alert_system.is_running:
        return {
            "success": False,
            "message": "Monitoring is already running",
            "status": "active"
        }
    
    try:
        # Start monitoring in background
        if not alert_system.monitor:
            alert_system.monitor = ContinuousMonitor(db_manager)
        
        alert_system.scheduler = BackgroundScheduler()
        alert_system.scheduler.add_job(
            func=alert_system.monitor.monitor_all_subscriptions,
            trigger='interval',
            minutes=config.interval_minutes,
            id='air_quality_monitor',
            replace_existing=True
        )
        alert_system.scheduler.start()
        alert_system.is_running = True
        
        # Run initial check
        background_tasks.add_task(alert_system.monitor.monitor_all_subscriptions)
        
        return {
            "success": True,
            "message": "Monitoring started successfully",
            "interval_minutes": config.interval_minutes,
            "next_check": (datetime.now() + timedelta(minutes=config.interval_minutes)).isoformat()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/monitoring/stop")
async def stop_monitoring():
    """Stop continuous air quality monitoring."""
    if not alert_system:
        raise HTTPException(status_code=503, detail="Alert system not initialized")
    
    if not alert_system.is_running:
        return {
            "success": False,
            "message": "Monitoring is not running",
            "status": "inactive"
        }
    
    try:
        if alert_system.scheduler:
            alert_system.scheduler.shutdown()
        alert_system.is_running = False
        
        return {
            "success": True,
            "message": "Monitoring stopped successfully",
            "status": "inactive"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/monitoring/status")
async def get_monitoring_status():
    """Get current monitoring status."""
    if not alert_system:
        raise HTTPException(status_code=503, detail="Alert system not initialized")
    
    subscriptions = db_manager.get_active_subscriptions()
    
    return {
        "monitoring_active": alert_system.is_running,
        "active_subscriptions": len(subscriptions),
        "timestamp": datetime.now().isoformat()
    }

@app.post("/alerts/check")
async def check_alerts_now(city: str, country: Optional[str] = None):
    """Manually trigger an alert check for a specific city."""
    if not monitor:
        raise HTTPException(status_code=503, detail="Monitor not initialized")
    
    try:
        current_data = db_manager.get_latest_air_quality(city, country)
        
        if not current_data:
            raise HTTPException(
                status_code=404,
                detail=f"No data available for {city}"
            )
        
        alert_generator = AlertGenerator(db_manager)
        alerts = alert_generator.generate_alerts(city, country, current_data)
        
        return {
            "success": True,
            "city": city,
            "country": country,
            "current_data": current_data,
            "alerts": alerts,
            "alert_count": len(alerts),
            "timestamp": datetime.now().isoformat()
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# RUN SERVER
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
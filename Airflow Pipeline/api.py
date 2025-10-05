from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Path
from fastapi.responses import JSONResponse
from langchain_openai import AzureChatOpenAI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, Field
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content
from apscheduler.schedulers.background import BackgroundScheduler

load_dotenv()

from air_quality_forecaster import AirQualityForecaster
from llm_generator import ImprovedEnvironmentalQuerySystem

# Pydantic models
class PollutantData(BaseModel):
    name: str
    value: float
    unit: str = "µg/m³"

class DayForecast(BaseModel):
    date: str
    day_name: str
    aqi: int
    category: str
    temperature: Optional[float] = None
    weather_icon: str
    pollutants: List[PollutantData]
    health_message: str
    color: str

class ForecastResponse(BaseModel):
    city: str
    location_name: str
    coordinates: Dict[str, float]
    last_updated: str
    forecast: List[DayForecast]

class CityInfo(BaseModel):
    city: str
    record_count: int
    current_aqi: Optional[int] = None
    category: Optional[str] = None

class CitiesResponse(BaseModel):
    cities: List[CityInfo]
    total_count: int

class PollutantDetail(BaseModel):
    name: str
    value: float
    unit: str
    description: str
    limit: float
    limit_unit: str
    percentage: float
    status: str
    color: str

class PollutantBreakdownResponse(BaseModel):
    city: str
    timestamp: str
    pollutants: List[PollutantDetail]

class HistoricalDataPoint(BaseModel):
    timestamp: str
    value: float

class PollutantHistoryResponse(BaseModel):
    pollutant: str
    city: str
    unit: str
    data: List[HistoricalDataPoint]
    min_value: float
    max_value: float
    avg_value: float

class ChatMessage(BaseModel):
    message: str
    timestamp: Optional[datetime] = None

class ChatResponse(BaseModel):
    response: str
    timestamp: datetime
    needs_clarification: bool = False
    suggested_cities: Optional[List[str]] = None

class HealthResponse(BaseModel):
    status: str
    database_connected: bool
    cities_available: int

class CityInfoDetailed(BaseModel):
    city: str
    country: str
    record_count: int
    earliest_date: str
    latest_date: str

class AvailableCitiesResponse(BaseModel):
    cities: List[CityInfoDetailed]
    total_count: int

class MonitoringStation(BaseModel):
    station_id: str
    name: str
    network: str  # "EPA AirNow", "OpenAQ", "Pandora", "TOLNet"
    location: str
    aqi: int
    aqi_category: str
    aqi_color: str
    measurement_type: str  # "ground", "varies", "point-based", "local site"
    coverage: str  # "hourly", "80 sec", "continuous"
    latitude: float
    longitude: float
    last_updated: str

class MonitoringStationsResponse(BaseModel):
    city: str
    stations: List[MonitoringStation]
    total_count: int
    timestamp: str

class NetworkStatsResponse(BaseModel):
    network_name: str
    total_stations: int
    active_stations: int
    avg_aqi: float
    coverage_area: str

class HistoricalTrend(BaseModel):
    date: str
    avg_value: float
    min_value: float
    max_value: float
    data_points: int

class TrendAnalysisResponse(BaseModel):
    city: str
    pollutant: str
    period_days: int
    trends: List[HistoricalTrend]
    overall_trend: str  # "improving", "worsening", "stable"
    percent_change: float

class FireProximity(BaseModel):
    fire_count: int
    nearest_distance_km: float
    total_frp: float
    impact_level: str  # "none", "low", "moderate", "high", "severe"
    affected_area_km2: float

class ValidationComparison(BaseModel):
    location: str
    satellite_value: float
    ground_value: float
    difference: float
    percent_difference: float
    correlation: str  # "excellent", "good", "fair", "poor"

class EnsembleForecast(BaseModel):
    date: str
    pollutant: str
    mean_prediction: float
    min_prediction: float
    max_prediction: float
    confidence_interval_95: Tuple[float, float]
    model_agreement: float  # 0-1 score

class AlertSubscription(BaseModel):
    email: EmailStr
    city: str
    threshold_aqi: int = 100
    notification_frequency: str = "immediate"  # "immediate", "daily", "weekly"

class HealthAlert(BaseModel):
    alert_id: str
    city: str
    current_aqi: int
    forecast_aqi: int
    risk_level: str
    affected_groups: List[str]
    recommendations: List[str]
    timestamp: str

class SpatialHotspot(BaseModel):
    location: str
    latitude: float
    longitude: float
    pollutant: str
    value: float
    severity: str
    radius_affected_km: float




# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5000)),
    'dbname': os.getenv('DB_NAME', 'db'),
    'user': os.getenv('DB_USER', 'db_user'),
    'password': os.getenv('DB_PASSWORD', 'db_password')
}

# Global instances
forecaster = None
query_system = None

# MERGED LIFESPAN
@asynccontextmanager
async def lifespan(app: FastAPI):
    global forecaster, query_system
    
    # Initialize forecaster
    print("Initializing Air Quality Forecaster...")
    forecaster = AirQualityForecaster(DB_CONFIG)
    if forecaster.connect_db():
        print("✓ Forecaster connected successfully")
        
        # AUTO-TRAIN MODELS ON STARTUP
        try:
            print("Checking for trained models...")
            if not forecaster.models or len(forecaster.models) == 0:
                print("No models found. Training models automatically (this will take 30-60 minutes)...")
                
                end_date = datetime.now()
                start_date = end_date - timedelta(days=90)
                
                print(f"Fetching data from {start_date.date()} to {end_date.date()}...")
                station_df = forecaster.fetch_station_data(start_date, end_date)
                met_df = forecaster.fetch_meteorological_data(start_date, end_date)
                sat_data = forecaster.fetch_satellite_data(start_date, end_date)
                pblh_df = forecaster.fetch_pblh_data(start_date, end_date)
                fire_df = forecaster.fetch_fire_data(start_date, end_date)
                
                print("Engineering features...")
                feature_df = forecaster.engineer_features(
                    station_df, met_df, sat_data, pblh_df, fire_df
                )
                
                pollutant_cols = ['PM10', 'PM2.5', 'PM25', 'NO2', 'O3']
                available_pollutants = [col for col in pollutant_cols if col in feature_df.columns]
                
                print(f"Training models for: {', '.join(available_pollutants)}")
                
                for pollutant in available_pollutants:
                    try:
                        print(f"Training {pollutant} models...")
                        X, y_dict, metadata, feature_cols = forecaster.prepare_training_data(
                            feature_df.copy(), 
                            target_col=pollutant,
                            horizons=[1, 6, 24]
                        )
                        
                        forecaster.time_series_split_train(
                            X, y_dict, feature_cols,
                            target_name=pollutant,
                            horizons=[1, 6, 24]
                        )
                        print(f"✓ Trained {pollutant} models successfully")
                    except Exception as e:
                        print(f"⚠ Error training {pollutant}: {e}")
                        continue
                
                print(f"✓ Auto-training complete: {len(forecaster.models)} models loaded")
            else:
                print(f"✓ Found {len(forecaster.models)} existing models. Skipping training.")
        except Exception as e:
            print(f"⚠ Auto-training failed: {e}")
            print("API will start but forecasting endpoints will not work until models are trained via /api/train")
    else:
        print("⚠ Warning: Forecaster connection failed")
    
    # Initialize query system
    try:
        query_system = ImprovedEnvironmentalQuerySystem(DB_CONFIG)
        print("✓ Environmental Query System initialized successfully")
    except Exception as e:
        print(f"⚠ Warning: Query system initialization failed: {e}")
    
    yield
    
    # Cleanup
    if forecaster:
        forecaster.close()
        print("✓ Forecaster closed")
    if query_system:
        query_system.close()
        print("✓ Query system closed")

# SINGLE APP CREATION
app = FastAPI(
    title="Air Quality Forecast & Chat API",
    description="Unified API for air quality forecasts, pollutant data, and natural language queries",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Helper functions
def get_aqi_color(aqi: int) -> str:
    if aqi <= 50: return "#00E400"
    elif aqi <= 100: return "#FFFF00"
    elif aqi <= 150: return "#FF7E00"
    elif aqi <= 200: return "#FF0000"
    elif aqi <= 300: return "#8F3F97"
    else: return "#7E0023"

def get_weather_icon(aqi: int) -> str:
    if aqi <= 50: return "sun"
    elif aqi <= 100: return "cloud"
    elif aqi <= 150: return "wind"
    else: return "rain"

def get_health_message(aqi: int) -> str:
    if aqi <= 50:
        return "Air quality is satisfactory, and air pollution poses little or no risk."
    elif aqi <= 100:
        return "Air quality is acceptable. Sensitive individuals should consider limiting prolonged outdoor exertion."
    elif aqi <= 150:
        return "Members of sensitive groups may experience health effects."
    elif aqi <= 200:
        return "Everyone may begin to experience health effects."
    elif aqi <= 300:
        return "Health alert: everyone may experience serious health effects."
    else:
        return "Health warnings of emergency conditions."

def get_pollutant_status(value: float, limit: float) -> tuple:
    percentage = (value / limit) * 100
    if percentage <= 50: return "good", "#00E400"
    elif percentage <= 100: return "moderate", "#00CED1"
    elif percentage <= 150: return "unhealthy_sensitive", "#FF7E00"
    else: return "unhealthy", "#FF0000"

# ENDPOINTS

@app.get("/")
async def root():
    return {
        "message": "Air Quality Forecast & Chat API",
        "version": "1.0.0",
        "status": "operational" if (forecaster and forecaster.conn) or query_system else "limited",
        "features": {
            "forecasting": forecaster is not None and forecaster.conn is not None,
            "chat": query_system is not None
        },
        "endpoints": {
            "forecast": "/api/forecast/{city}",
            "cities": "/api/cities",
            "pollutants": "/api/pollutants/{city}",
            "history": "/api/pollutants/{city}/history/{pollutant}",
            "compare": "/api/pollutants/compare",
            "health_recs": "/api/health-recommendations/{city}",
            "chat": "/api/chat",
            "train": "/api/train (POST)",
            "docs": "/docs"
        }
    }

@app.get("/health")
async def health_check():
    forecaster_status = "connected" if forecaster and forecaster.conn else "disconnected"
    query_system_status = "initialized" if query_system else "not_initialized"
    models_loaded = len(forecaster.models) if forecaster else 0
    
    cities_available = 0
    if query_system:
        try:
            cities_available = len(query_system.db_manager.get_available_cities())
        except:
            pass
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "forecaster": forecaster_status,
        "query_system": query_system_status,
        "models_loaded": models_loaded,
        "cities_available": cities_available
    }

@app.get("/api/models")
async def get_models_info():
    if not forecaster:
        raise HTTPException(status_code=503, detail="Forecaster not initialized")
    
    return {
        "models_loaded": list(forecaster.models.keys()),
        "scalers_loaded": list(forecaster.scalers.keys()),
        "feature_importance_available": list(forecaster.feature_importance.keys()),
        "total_models": len(forecaster.models)
    }

@app.get("/api/cities", response_model=CitiesResponse)
async def get_cities():
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        cities_df = forecaster.get_available_cities()
        
        cities_list = []
        for _, row in cities_df.iterrows():
            current_aqi = None
            category = None
            
            try:
                query = """
                SELECT parameter_name, value
                FROM air_quality_data
                WHERE city = %s
                ORDER BY datetime_utc DESC
                LIMIT 10
                """
                aqi_df = pd.read_sql(query, forecaster.conn, params=[row['city']])
                
                if not aqi_df.empty:
                    pollutants = {}
                    for pol in ['PM10', 'PM2.5', 'PM25']:
                        pol_data = aqi_df[aqi_df['parameter_name'] == pol]
                        if not pol_data.empty:
                            pollutants[pol.lower().replace('.', '')] = float(pol_data['value'].mean())
                    
                    if pollutants:
                        aqi_value = forecaster.calculate_aqi(pollutants)
                        if aqi_value:
                            current_aqi = int(round(aqi_value))
                            category = forecaster._get_aqi_category(current_aqi)
            except Exception as e:
                print(f"Error calculating AQI for {row['city']}: {e}")
            
            cities_list.append(CityInfo(
                city=row['city'],
                record_count=int(row['record_count']),
                current_aqi=current_aqi,
                category=category
            ))
        
        return CitiesResponse(
            cities=cities_list,
            total_count=len(cities_list)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching cities: {str(e)}")

@app.get("/api/forecast/{city}", response_model=ForecastResponse)
async def get_forecast(
    city: str,
    days: int = Query(default=4, ge=1, le=7, description="Number of forecast days")
):
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Forecaster not available")
    
    if not forecaster.models:
        raise HTTPException(
            status_code=503, 
            detail="No trained models available. Train models first using /api/train"
        )
    
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        query = """
        SELECT datetime_utc, latitude, longitude, city, parameter_name, value, location_name, location_id
        FROM air_quality_data
        WHERE city ILIKE %s AND datetime_utc >= %s
        ORDER BY datetime_utc DESC
        LIMIT 1000
        """
        
        station_df = pd.read_sql(query, forecaster.conn, params=[f'%{city}%', start_date])
        
        if station_df.empty:
            raise HTTPException(status_code=404, detail=f"No data found for city: {city}")
        
        latest_time = station_df['datetime_utc'].max()
        latest_data = station_df[station_df['datetime_utc'] == latest_time]
        
        latest_pivot = latest_data.pivot_table(
            index=['datetime_utc', 'latitude', 'longitude', 'location_id', 'city', 'location_name'],
            columns='parameter_name',
            values='value',
            aggfunc='mean'
        ).reset_index()
        
        lat = float(latest_pivot['latitude'].iloc[0])
        lon = float(latest_pivot['longitude'].iloc[0])
        location_name = latest_pivot['location_name'].iloc[0] if 'location_name' in latest_pivot else 'Unknown'
        
        latest_pivot['datetime_utc'] = pd.to_datetime(latest_pivot['datetime_utc'])
        latest_pivot['hour'] = latest_pivot['datetime_utc'].dt.hour
        latest_pivot['day_of_week'] = latest_pivot['datetime_utc'].dt.dayofweek
        latest_pivot['month'] = latest_pivot['datetime_utc'].dt.month
        latest_pivot['is_weekend'] = latest_pivot['day_of_week'].isin([5, 6]).astype(int)
        latest_pivot['hour_sin'] = np.sin(2 * np.pi * latest_pivot['hour'] / 24)
        latest_pivot['hour_cos'] = np.cos(2 * np.pi * latest_pivot['hour'] / 24)
        
        met_query = """
        SELECT granule_time_start as datetime, variable_name, variable_value
        FROM merra2_slv_data
        WHERE latitude BETWEEN %s AND %s AND longitude BETWEEN %s AND %s
        AND granule_time_start >= %s
        ORDER BY granule_time_start DESC
        LIMIT 100
        """
        
        met_df = pd.read_sql(met_query, forecaster.conn, 
                            params=[lat-0.5, lat+0.5, lon-0.5, lon+0.5, start_date])
        
        if not met_df.empty:
            met_latest = met_df.pivot_table(
                index='datetime',
                columns='variable_name',
                values='variable_value',
                aggfunc='mean'
            )
            if len(met_latest) > 0:
                latest_met = met_latest.iloc[-1]
                for var, val in latest_met.items():
                    latest_pivot[var] = val
        
        pblh_query = """
        SELECT timestamp as datetime, pbl_height_m
        FROM pblh_data
        WHERE latitude BETWEEN %s AND %s AND longitude BETWEEN %s AND %s
        AND timestamp >= %s
        ORDER BY timestamp DESC
        LIMIT 50
        """
        
        pblh_df = pd.read_sql(pblh_query, forecaster.conn,
                             params=[lat-0.5, lat+0.5, lon-0.5, lon+0.5, start_date])
        
        if not pblh_df.empty:
            latest_pivot['pbl_height_m'] = pblh_df['pbl_height_m'].iloc[0]
        else:
            latest_pivot['pbl_height_m'] = 0
        
        latest_pivot['fire_count_50km'] = 0
        latest_pivot['fire_frp_sum_50km'] = 0
        latest_pivot = latest_pivot.fillna(0)
        
        forecast_days = []
        forecast_horizons = [24 * i for i in range(days)]
        
        for day_offset, target_horizon in enumerate(forecast_horizons):
            forecast_time = latest_time + timedelta(hours=target_horizon)
            
            if day_offset == 0:
                day_name = "Today"
            elif day_offset == 1:
                day_name = "Tomorrow"
            else:
                day_name = forecast_time.strftime("%A")
            
            pollutants_dict = {}
            pollutant_list = []
            
            for pollutant in ['PM10', 'PM2.5', 'PM25']:
                if pollutant not in latest_pivot.columns:
                    continue
                
                available_horizons = [24, 6, 1]
                use_horizon = None
                
                for h in available_horizons:
                    model_key = f'{pollutant}_{h}h'
                    if model_key in forecaster.models:
                        use_horizon = h
                        break
                
                if use_horizon is None:
                    continue
                
                model_key = f'{pollutant}_{use_horizon}h'
                training_features = forecaster.training_features.get(model_key, [])
                
                if not training_features:
                    continue
                
                try:
                    X_new = pd.DataFrame()
                    for feat in training_features:
                        if feat in latest_pivot.columns:
                            X_new[feat] = latest_pivot[feat]
                        else:
                            X_new[feat] = 0
                    
                    if 'hour' in X_new.columns:
                        X_new['hour'] = forecast_time.hour
                    if 'day_of_week' in X_new.columns:
                        X_new['day_of_week'] = forecast_time.dayofweek
                    if 'is_weekend' in X_new.columns:
                        X_new['is_weekend'] = int(forecast_time.dayofweek in [5, 6])
                    if 'hour_sin' in X_new.columns:
                        X_new['hour_sin'] = np.sin(2 * np.pi * forecast_time.hour / 24)
                    if 'hour_cos' in X_new.columns:
                        X_new['hour_cos'] = np.cos(2 * np.pi * forecast_time.hour / 24)
                    
                    prediction = forecaster.predict_with_uncertainty(X_new, pollutant, use_horizon)
                    
                    decay_factor = 1.0
                    if target_horizon > use_horizon:
                        decay_factor = 0.95 ** ((target_horizon - use_horizon) / use_horizon)
                    
                    predicted_value = prediction['mean'][0] * decay_factor
                    
                    if day_offset == 0:
                        predicted_value = latest_pivot[pollutant].iloc[0]
                    
                    pol_key = pollutant.lower().replace('.', '').replace('25', '25')
                    if pol_key == 'pm25':
                        pol_key = 'pm25'
                    
                    pollutants_dict[pol_key] = predicted_value
                    pollutant_list.append(PollutantData(
                        name=pollutant,
                        value=round(predicted_value, 2),
                        unit="µg/m³"
                    ))
                    
                except Exception as e:
                    print(f"Error predicting {pollutant}: {e}")
                    continue
            
            if pollutants_dict:
                aqi = forecaster.calculate_aqi(pollutants_dict)
                
                if aqi:
                    category = forecaster._get_aqi_category(aqi)
                    health_message = get_health_message(aqi)
                    color = get_aqi_color(aqi)
                    weather_icon = get_weather_icon(aqi)
                    temp = 72 - day_offset * 2
                    
                    forecast_days.append(DayForecast(
                        date=forecast_time.strftime("%Y-%m-%d"),
                        day_name=day_name,
                        aqi=int(aqi),
                        category=category,
                        temperature=temp,
                        weather_icon=weather_icon,
                        pollutants=pollutant_list,
                        health_message=health_message,
                        color=color
                    ))
        
        if not forecast_days:
            raise HTTPException(status_code=500, detail="Could not generate forecast")
        
        return ForecastResponse(
            city=city,
            location_name=location_name,
            coordinates={"latitude": lat, "longitude": lon},
            last_updated=latest_time.strftime("%Y-%m-%d %H:%M UTC"),
            forecast=forecast_days
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/api/pollutants/{city}", response_model=PollutantBreakdownResponse)
async def get_pollutant_breakdown(city: str):
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        query = """
        SELECT parameter_name, value, units, datetime_utc
        FROM air_quality_data
        WHERE city ILIKE %s
        ORDER BY datetime_utc DESC
        LIMIT 100
        """
        
        df = pd.read_sql(query, forecaster.conn, params=[f'%{city}%'])
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data found for city: {city}")
        
        latest_time = df['datetime_utc'].max()
        latest_data = df[df['datetime_utc'] == latest_time]
        
        pollutant_info = {
            'PM25': {'name': 'PM2.5', 'description': 'Fine particles that can penetrate deep into lungs', 'limit': 35, 'unit': 'µg/m³'},
            'PM2.5': {'name': 'PM2.5', 'description': 'Fine particles that can penetrate deep into lungs', 'limit': 35, 'unit': 'µg/m³'},
            'PM10': {'name': 'PM10', 'description': 'Coarse particles from dust and pollen', 'limit': 150, 'unit': 'µg/m³'},
            'O3': {'name': 'O₃', 'description': 'Ground-level ozone, respiratory irritant', 'limit': 100, 'unit': 'ppb'},
            'NO2': {'name': 'NO₂', 'description': 'Nitrogen dioxide from vehicle emissions', 'limit': 100, 'unit': 'ppb'},
            'SO2': {'name': 'SO₂', 'description': 'Sulfur dioxide from industrial sources', 'limit': 75, 'unit': 'ppb'},
            'CO': {'name': 'CO', 'description': 'Carbon monoxide from incomplete combustion', 'limit': 9, 'unit': 'ppm'}
        }
        
        pollutants_list = []
        for param in ['PM25', 'PM2.5', 'PM10', 'O3', 'NO2', 'SO2', 'CO']:
            param_data = latest_data[latest_data['parameter_name'] == param]
            if not param_data.empty and param in pollutant_info:
                value = float(param_data['value'].mean())
                info = pollutant_info[param]
                percentage = (value / info['limit']) * 100
                status, color = get_pollutant_status(value, info['limit'])
                
                pollutants_list.append(PollutantDetail(
                    name=info['name'],
                    value=round(value, 1),
                    unit=info['unit'],
                    description=info['description'],
                    limit=info['limit'],
                    limit_unit=info['unit'],
                    percentage=round(percentage, 1),
                    status=status,
                    color=color
                ))
        
        return PollutantBreakdownResponse(
            city=city,
            timestamp=latest_time.strftime("%Y-%m-%d %H:%M UTC"),
            pollutants=pollutants_list
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/api/pollutants/{city}/history/{pollutant}", response_model=PollutantHistoryResponse)
async def get_pollutant_history(
    city: str,
    pollutant: str,
    hours: int = Query(default=24, ge=1, le=168)
):
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        start_time = datetime.now() - timedelta(hours=hours)
        
        query = """
        SELECT datetime_utc, value, units
        FROM air_quality_data
        WHERE city ILIKE %s AND parameter_name = %s AND datetime_utc >= %s
        ORDER BY datetime_utc ASC
        """
        
        df = pd.read_sql(query, forecaster.conn, params=[f'%{city}%', pollutant, start_time])
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data found for {pollutant} in {city}")
        
        data_points = [
            HistoricalDataPoint(
                timestamp=row['datetime_utc'].strftime("%Y-%m-%d %H:%M"),
                value=float(row['value'])
            )
            for _, row in df.iterrows()
        ]
        
        values = df['value'].astype(float)
        
        return PollutantHistoryResponse(
            pollutant=pollutant,
            city=city,
            unit=df['units'].iloc[0] if 'units' in df.columns else 'µg/m³',
            data=data_points,
            min_value=float(values.min()),
            max_value=float(values.max()),
            avg_value=float(values.mean())
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/api/pollutants/compare")
async def compare_cities(
    cities: str = Query(..., description="Comma-separated city names"),
    pollutant: str = Query(default="PM25")
):
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        city_list = [c.strip() for c in cities.split(',')]
        comparison_data = []
        
        for city in city_list:
            query = """
            SELECT value, datetime_utc
            FROM air_quality_data
            WHERE city ILIKE %s AND parameter_name = %s
            ORDER BY datetime_utc DESC
            LIMIT 1
            """
            
            df = pd.read_sql(query, forecaster.conn, params=[f'%{city}%', pollutant])
            
            if not df.empty:
                comparison_data.append({
                    "city": city,
                    "value": float(df['value'].iloc[0]),
                    "timestamp": df['datetime_utc'].iloc[0].strftime("%Y-%m-%d %H:%M")
                })
        
        if not comparison_data:
            raise HTTPException(status_code=404, detail="No data found")
        
        comparison_data.sort(key=lambda x: x['value'], reverse=True)
        
        return {
            "pollutant": pollutant,
            "cities_count": len(comparison_data),
            "comparison": comparison_data,
            "highest": comparison_data[0],
            "lowest": comparison_data[-1]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/api/health-recommendations/{city}")
async def get_health_recommendations(city: str):
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        query = """
        SELECT parameter_name, value
        FROM air_quality_data
        WHERE city ILIKE %s
        ORDER BY datetime_utc DESC
        LIMIT 10
        """
        
        df = pd.read_sql(query, forecaster.conn, params=[f'%{city}%'])
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data found for city: {city}")
        
        pollutants = {}
        for pol in ['PM10', 'PM2.5', 'PM25']:
            pol_data = df[df['parameter_name'] == pol]
            if not pol_data.empty:
                pollutants[pol.lower().replace('.', '')] = float(pol_data['value'].mean())
        
        aqi = forecaster.calculate_aqi(pollutants) if pollutants else None
        
        if not aqi:
            raise HTTPException(status_code=404, detail="Could not calculate AQI")
        
        aqi = int(round(aqi))
        category = forecaster._get_aqi_category(aqi)
        
        if aqi <= 50:
            recommendations = {
                "general": ["Perfect day for outdoor activities", "Air quality is ideal"],
                "sensitive": ["Enjoy outdoor activities"],
                "activities": ["Running", "Cycling", "Sports", "Walking"],
                "precautions": []
            }
        elif aqi <= 100:
            recommendations = {
                "general": ["Air quality is acceptable for most people"],
                "sensitive": ["Consider reducing prolonged outdoor exertion"],
                "activities": ["Light exercise okay", "Moderate outdoor activities"],
                "precautions": ["Sensitive groups should monitor symptoms"]
            }
        elif aqi <= 150:
            recommendations = {
                "general": ["Reduce prolonged outdoor exertion"],
                "sensitive": ["Avoid prolonged outdoor activities", "Keep rescue medications handy"],
                "activities": ["Indoor activities preferred", "Short outdoor walks okay"],
                "precautions": ["Close windows", "Use air purifiers", "Wear N95 masks outdoors"]
            }
        elif aqi <= 200:
            recommendations = {
                "general": ["Avoid prolonged outdoor activities"],
                "sensitive": ["Stay indoors", "Avoid all outdoor exertion"],
                "activities": ["Indoor activities only"],
                "precautions": ["Keep windows closed", "Use air purifiers", "Wear N95 masks"]
            }
        else:
            recommendations = {
                "general": ["Stay indoors", "Avoid all outdoor activities"],
                "sensitive": ["Remain indoors", "Keep activity levels low"],
                "activities": ["Indoor rest only"],
                "precautions": ["Seal windows and doors", "Use air purifiers", "Seek medical help if symptoms worsen"]
            }
        
        return {
            "city": city,
            "aqi": aqi,
            "category": category,
            "color": get_aqi_color(aqi),
            "recommendations": recommendations,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/api/monitoring-stations/{city}", response_model=MonitoringStationsResponse, tags=["Monitoring"])
async def get_monitoring_stations(city: str):
    """
    Get all monitoring stations for a specific city with real-time AQI
    Shows data from EPA AirNow, OpenAQ, Pandora, and TOLNet networks
    """
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        # Query all stations for the city
        query = """
        SELECT DISTINCT
            location_id as station_id,
            location_name as name,
            city,
            latitude,
            longitude,
            datetime_utc,
            parameter_name,
            value
        FROM air_quality_data
        WHERE city ILIKE %s
        AND datetime_utc >= NOW() - INTERVAL '24 hours'
        ORDER BY datetime_utc DESC
        """
        
        df = pd.read_sql(query, forecaster.conn, params=[f'%{city}%'])
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No monitoring stations found for {city}")
        
        # Group by station and get latest data
        stations = []
        station_groups = df.groupby('station_id')
        
        for station_id, station_data in station_groups:
            # Get latest timestamp for this station
            latest_time = station_data['datetime_utc'].max()
            latest_data = station_data[station_data['datetime_utc'] == latest_time]
            
            # Calculate AQI from available pollutants
            pollutants = {}
            for pol in ['PM10', 'PM2.5', 'PM25']:
                pol_data = latest_data[latest_data['parameter_name'] == pol]
                if not pol_data.empty:
                    pollutants[pol.lower().replace('.', '')] = float(pol_data['value'].mean())
            
            aqi = None
            if pollutants:
                aqi = forecaster.calculate_aqi(pollutants)
            
            if not aqi:
                continue
            
            aqi = int(round(aqi))
            category = forecaster._get_aqi_category(aqi)
            color = get_aqi_color(aqi)
            
            # Determine network type from station name
            name = latest_data['name'].iloc[0]
            if 'EPA' in name or 'AirNow' in name:
                network = "EPA AirNow"
                measurement_type = "ground"
                coverage = "hourly"
            elif 'OpenAQ' in name or 'OWM' in name:
                network = "OpenAQ (Global Stations)"
                measurement_type = "ground"
                coverage = "hourly"
            elif 'Pandora' in name or 'PAN' in name:
                network = "Pandora Spectrometers (Pandonia Network)"
                measurement_type = "point-based"
                coverage = "80 sec"
            elif 'TOLNet' in name or 'LIDAR' in name:
                network = "TOLNet Ozone Lidars"
                measurement_type = "local site"
                coverage = "continuous"
            else:
                network = "OpenAQ (Global Stations)"
                measurement_type = "ground"
                coverage = "varies"
            
            location_desc = latest_data['city'].iloc[0]
            if network == "EPA AirNow":
                location_desc = f"New York"
            elif network == "OpenAQ (Global Stations)":
                location_desc = "Global City Monitors"
            elif "Pandora" in network:
                location_desc = "Research Sites"
            elif "TOLNet" in network:
                location_desc = "Vertical Profiles"
            
            stations.append(MonitoringStation(
                station_id=str(station_id),
                name=network,
                network=network,
                location=location_desc,
                aqi=aqi,
                aqi_category=category,
                aqi_color=color,
                measurement_type=measurement_type,
                coverage=coverage,
                latitude=float(latest_data['latitude'].iloc[0]),
                longitude=float(latest_data['longitude'].iloc[0]),
                last_updated=latest_time.strftime("%Y-%m-%d %H:%M UTC")
            ))
        
        # Sort by AQI (worst first)
        stations.sort(key=lambda x: x.aqi, reverse=True)
        
        return MonitoringStationsResponse(
            city=city,
            stations=stations,
            total_count=len(stations),
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/monitoring-networks", tags=["Monitoring"])
async def get_monitoring_networks():
    """
    Get information about all available monitoring networks
    """
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        query = """
        SELECT DISTINCT location_name
        FROM air_quality_data
        WHERE datetime_utc >= NOW() - INTERVAL '7 days'
        """
        
        df = pd.read_sql(query, forecaster.conn)
        
        networks = {
            "EPA AirNow": {"count": 0, "type": "ground", "coverage": "hourly"},
            "OpenAQ": {"count": 0, "type": "ground", "coverage": "varies"},
            "Pandora": {"count": 0, "type": "point-based", "coverage": "80 sec"},
            "TOLNet": {"count": 0, "type": "local site", "coverage": "continuous"}
        }
        
        for location in df['location_name']:
            if 'EPA' in location or 'AirNow' in location:
                networks["EPA AirNow"]["count"] += 1
            elif 'Pandora' in location or 'PAN' in location:
                networks["Pandora"]["count"] += 1
            elif 'TOLNet' in location or 'LIDAR' in location:
                networks["TOLNet"]["count"] += 1
            else:
                networks["OpenAQ"]["count"] += 1
        
        return {
            "networks": [
                {
                    "name": "EPA AirNow",
                    "full_name": "EPA AirNow",
                    "description": "New York",
                    "stations": networks["EPA AirNow"]["count"],
                    "measurement_type": networks["EPA AirNow"]["type"],
                    "coverage": networks["EPA AirNow"]["coverage"]
                },
                {
                    "name": "OpenAQ",
                    "full_name": "OpenAQ (Global Stations)",
                    "description": "Global City Monitors",
                    "stations": networks["OpenAQ"]["count"],
                    "measurement_type": networks["OpenAQ"]["type"],
                    "coverage": networks["OpenAQ"]["coverage"]
                },
                {
                    "name": "Pandora",
                    "full_name": "Pandora Spectrometers (Pandonia Network)",
                    "description": "Research Sites",
                    "stations": networks["Pandora"]["count"],
                    "measurement_type": networks["Pandora"]["type"],
                    "coverage": networks["Pandora"]["coverage"]
                },
                {
                    "name": "TOLNet",
                    "full_name": "TOLNet Ozone Lidars",
                    "description": "Vertical Profiles",
                    "stations": networks["TOLNet"]["count"],
                    "measurement_type": networks["TOLNet"]["type"],
                    "coverage": networks["TOLNet"]["coverage"]
                }
            ],
            "total_stations": sum(n["count"] for n in networks.values()),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/monitoring-stations/{city}/live", tags=["Monitoring"])
async def get_live_station_data(city: str):
    """
    Get live real-time data from all monitoring stations in a city
    Updates every few seconds for live monitoring
    """
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        query = """
        SELECT 
            location_id,
            location_name,
            parameter_name,
            value,
            datetime_utc
        FROM air_quality_data
        WHERE city ILIKE %s
        AND datetime_utc >= NOW() - INTERVAL '1 hour'
        ORDER BY datetime_utc DESC
        LIMIT 100
        """
        
        df = pd.read_sql(query, forecaster.conn, params=[f'%{city}%'])
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No live data for {city}")
        
        live_data = []
        for location_id in df['location_id'].unique():
            location_data = df[df['location_id'] == location_id]
            latest = location_data.iloc[0]
            
            live_data.append({
                "station_id": str(location_id),
                "station_name": latest['location_name'],
                "pollutant": latest['parameter_name'],
                "value": float(latest['value']),
                "timestamp": latest['datetime_utc'].strftime("%Y-%m-%d %H:%M:%S UTC"),
                "status": "live"
            })
        
        return {
            "city": city,
            "live_data": live_data,
            "update_interval": "real-time",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

# CHAT ENDPOINTS

# CHAT ENDPOINTS (continued)

@app.post("/api/chat", response_model=ChatResponse, tags=["Chat"])
async def chat(message: ChatMessage):
    if not query_system:
        raise HTTPException(status_code=503, detail="Query system not initialized")
    
    if not message.message or not message.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    
    try:
        response_text = query_system.process_query(message.message)
        
        needs_clarification = any([
            "which city" in response_text.lower(),
            "specify a city" in response_text.lower(),
            "need to know" in response_text.lower(),
            "please specify" in response_text.lower()
        ])
        
        suggested_cities = None
        if needs_clarification and "Some cities with available data include:" in response_text:
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


@app.get("/api/cities/available", response_model=AvailableCitiesResponse, tags=["Cities"])
async def get_available_cities_detailed(limit: Optional[int] = 50):
    if not query_system:
        raise HTTPException(status_code=503, detail="Query system not initialized")
    
    try:
        cities_data = query_system.db_manager.get_available_cities(limit=limit)
        
        cities = [
            CityInfoDetailed(
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
    if not query_system:
        raise HTTPException(status_code=503, detail="Query system not initialized")
    
    try:
        all_cities = query_system.db_manager.get_available_cities()
        
        matching_cities = [
            c for c in all_cities 
            if query.lower() in c['city'].lower() or query.lower() in c['country'].lower()
        ][:limit]
        
        cities = [
            CityInfoDetailed(
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
    if not query_system:
        raise HTTPException(status_code=503, detail="Query system not initialized")
    
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    
    try:
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


@app.get("/api/examples", tags=["Help"])
async def get_example_queries():
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


@app.post("/api/train", tags=["Training"])
async def train_models(days: int = Query(default=90, ge=30, le=180)):
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Forecaster not available")
    
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        station_df = forecaster.fetch_station_data(start_date, end_date)
        met_df = forecaster.fetch_meteorological_data(start_date, end_date)
        sat_data = forecaster.fetch_satellite_data(start_date, end_date)
        pblh_df = forecaster.fetch_pblh_data(start_date, end_date)
        fire_df = forecaster.fetch_fire_data(start_date, end_date)
        
        feature_df = forecaster.engineer_features(
            station_df, met_df, sat_data, pblh_df, fire_df
        )
        
        pollutant_cols = ['PM10', 'PM2.5', 'PM25', 'NO2', 'O3']
        available_pollutants = [col for col in pollutant_cols if col in feature_df.columns]
        
        trained_models = []
        
        for pollutant in available_pollutants:
            try:
                X, y_dict, metadata, feature_cols = forecaster.prepare_training_data(
                    feature_df.copy(), 
                    target_col=pollutant,
                    horizons=[1, 6, 24]
                )
                
                forecaster.time_series_split_train(
                    X, y_dict, feature_cols,
                    target_name=pollutant,
                    horizons=[1, 6, 24]
                )
                
                trained_models.append(pollutant)
            except Exception as e:
                print(f"Error training {pollutant}: {e}")
                continue
        
        return {
            "status": "success",
            "trained_pollutants": trained_models,
            "total_models": len(forecaster.models),
            "data_period": f"{start_date.date()} to {end_date.date()}"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Training failed: {str(e)}")
@app.get("/api/trends/{city}/historical", tags=["Analytics"])
async def get_historical_trends(
    city: str,
    days: int = Query(default=30, ge=7, le=90),
    pollutant: str = Query(default="PM25")
):
    """
    Get historical trends for a pollutant showing daily patterns
    """
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        start_date = datetime.now() - timedelta(days=days)
        
        query = """
        SELECT 
            DATE(datetime_utc) as date,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value,
            COUNT(*) as data_points
        FROM air_quality_data
        WHERE city ILIKE %s
        AND parameter_name = %s
        AND datetime_utc >= %s
        GROUP BY DATE(datetime_utc)
        ORDER BY date ASC
        """
        
        df = pd.read_sql(query, forecaster.conn, params=[f'%{city}%', pollutant, start_date])
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data found for {pollutant} in {city}")
        
        trends = [
            HistoricalTrend(
                date=row['date'].strftime("%Y-%m-%d"),
                avg_value=float(row['avg_value']),
                min_value=float(row['min_value']),
                max_value=float(row['max_value']),
                data_points=int(row['data_points'])
            )
            for _, row in df.iterrows()
        ]
        
        # Calculate overall trend
        first_week_avg = df.head(7)['avg_value'].mean()
        last_week_avg = df.tail(7)['avg_value'].mean()
        percent_change = ((last_week_avg - first_week_avg) / first_week_avg) * 100
        
        if percent_change < -10:
            overall_trend = "improving"
        elif percent_change > 10:
            overall_trend = "worsening"
        else:
            overall_trend = "stable"
        
        return TrendAnalysisResponse(
            city=city,
            pollutant=pollutant,
            period_days=days,
            trends=trends,
            overall_trend=overall_trend,
            percent_change=round(percent_change, 2)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/wildfire/impact/{city}", tags=["Emergency"])
async def get_wildfire_impact(city: str, radius_km: int = Query(default=100, ge=10, le=500)):
    """
    Analyze wildfire impact on air quality in a city
    """
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        # Get city coordinates
        city_query = """
        SELECT DISTINCT latitude, longitude
        FROM air_quality_data
        WHERE city ILIKE %s
        LIMIT 1
        """
        
        city_df = pd.read_sql(city_query, forecaster.conn, params=[f'%{city}%'])
        
        if city_df.empty:
            raise HTTPException(status_code=404, detail=f"City {city} not found")
        
        city_lat = city_df['latitude'].iloc[0]
        city_lon = city_df['longitude'].iloc[0]
        
        # Get fires within radius (last 7 days)
        fire_query = """
        SELECT 
            latitude,
            longitude,
            frp,
            acq_date,
            confidence
        FROM fire_detection_data
        WHERE acq_date >= CURRENT_DATE - INTERVAL '7 days'
        AND confidence IN ('h', 'n')
        """
        
        fire_df = pd.read_sql(fire_query, forecaster.conn)
        
        if fire_df.empty:
            return {
                "city": city,
                "fire_impact": FireProximity(
                    fire_count=0,
                    nearest_distance_km=999.9,
                    total_frp=0,
                    impact_level="none",
                    affected_area_km2=0
                ),
                "message": "No active fires detected in the region"
            }
        
        # Calculate distances
        fire_df['distance_km'] = fire_df.apply(
            lambda row: np.sqrt(
                ((row['latitude'] - city_lat) * 111)**2 + 
                ((row['longitude'] - city_lon) * 111 * np.cos(np.radians(city_lat)))**2
            ), axis=1
        )
        
        nearby_fires = fire_df[fire_df['distance_km'] <= radius_km]
        
        if nearby_fires.empty:
            return {
                "city": city,
                "fire_impact": FireProximity(
                    fire_count=0,
                    nearest_distance_km=fire_df['distance_km'].min(),
                    total_frp=0,
                    impact_level="none",
                    affected_area_km2=0
                ),
                "message": f"No fires within {radius_km}km"
            }
        
        fire_count = len(nearby_fires)
        nearest_distance = nearby_fires['distance_km'].min()
        total_frp = nearby_fires['frp'].sum()
        
        # Determine impact level
        if nearest_distance < 50 and total_frp > 1000:
            impact_level = "severe"
        elif nearest_distance < 100 and total_frp > 500:
            impact_level = "high"
        elif nearest_distance < 150 and total_frp > 100:
            impact_level = "moderate"
        elif fire_count > 0:
            impact_level = "low"
        else:
            impact_level = "none"
        
        return {
            "city": city,
            "fire_impact": FireProximity(
                fire_count=fire_count,
                nearest_distance_km=round(nearest_distance, 2),
                total_frp=round(total_frp, 2),
                impact_level=impact_level,
                affected_area_km2=round(np.pi * radius_km**2, 2)
            ),
            "active_fires": fire_count,
            "search_radius_km": radius_km,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/validation/satellite-vs-ground/{city}", tags=["Validation"])
async def validate_satellite_ground(city: str):
    """
    Compare TEMPO satellite data with ground measurements (Pandora/OpenAQ)
    """
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        # Get ground station data (last 24 hours)
        ground_query = """
        SELECT 
            latitude,
            longitude,
            parameter_name,
            AVG(value) as avg_value,
            location_name
        FROM air_quality_data
        WHERE city ILIKE %s
        AND datetime_utc >= NOW() - INTERVAL '24 hours'
        AND parameter_name IN ('NO2', 'O3')
        GROUP BY latitude, longitude, parameter_name, location_name
        """
        
        ground_df = pd.read_sql(ground_query, forecaster.conn, params=[f'%{city}%'])
        
        if ground_df.empty:
            raise HTTPException(status_code=404, detail=f"No ground data for {city}")
        
        # Get TEMPO NO2 data (last 24 hours)
        tempo_query = """
        SELECT 
            latitude,
            longitude,
            AVG(no2_tropospheric_column) as avg_no2
        FROM tempo_no2_data
        WHERE observation_datetime >= NOW() - INTERVAL '24 hours'
        GROUP BY latitude, longitude
        """
        
        tempo_df = pd.read_sql(tempo_query, forecaster.conn)
        
        comparisons = []
        
        for _, ground in ground_df.iterrows():
            if ground['parameter_name'] != 'NO2':
                continue
                
            # Find nearest TEMPO measurement
            if not tempo_df.empty:
                tempo_df['distance'] = np.sqrt(
                    (tempo_df['latitude'] - ground['latitude'])**2 + 
                    (tempo_df['longitude'] - ground['longitude'])**2
                )
                nearest = tempo_df.loc[tempo_df['distance'].idxmin()]
                
                if nearest['distance'] < 0.5:  # Within ~50km
                    diff = nearest['avg_no2'] - ground['avg_value']
                    percent_diff = (diff / ground['avg_value']) * 100 if ground['avg_value'] > 0 else 0
                    
                    if abs(percent_diff) < 20:
                        correlation = "excellent"
                    elif abs(percent_diff) < 40:
                        correlation = "good"
                    elif abs(percent_diff) < 60:
                        correlation = "fair"
                    else:
                        correlation = "poor"
                    
                    comparisons.append(ValidationComparison(
                        location=ground['location_name'],
                        satellite_value=round(nearest['avg_no2'], 2),
                        ground_value=round(ground['avg_value'], 2),
                        difference=round(diff, 2),
                        percent_difference=round(percent_diff, 2),
                        correlation=correlation
                    ))
        
        if not comparisons:
            return {
                "city": city,
                "message": "No overlapping satellite and ground measurements found",
                "comparisons": []
            }
        
        return {
            "city": city,
            "pollutant": "NO2",
            "comparisons": comparisons,
            "total_comparisons": len(comparisons),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/forecast/ensemble/{city}", tags=["Advanced Forecasting"])
async def get_ensemble_forecast(city: str, pollutant: str = "PM25"):
    """
    Generate ensemble forecast using multiple models with uncertainty quantification
    """
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Forecaster not available")
    
    if not forecaster.models:
        raise HTTPException(status_code=503, detail="No trained models")
    
    try:
        # Get recent data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        query = """
        SELECT datetime_utc, latitude, longitude, city, parameter_name, value, location_name, location_id
        FROM air_quality_data
        WHERE city ILIKE %s AND datetime_utc >= %s
        ORDER BY datetime_utc DESC
        LIMIT 1000
        """
        
        station_df = pd.read_sql(query, forecaster.conn, params=[f'%{city}%', start_date])
        
        if station_df.empty:
            raise HTTPException(status_code=404, detail=f"No data for {city}")
        
        latest_time = station_df['datetime_utc'].max()
        latest_data = station_df[station_df['datetime_utc'] == latest_time]
        
        latest_pivot = latest_data.pivot_table(
            index=['datetime_utc', 'latitude', 'longitude', 'location_id', 'city', 'location_name'],
            columns='parameter_name',
            values='value',
            aggfunc='mean'
        ).reset_index()
        
        if pollutant not in latest_pivot.columns:
            raise HTTPException(status_code=404, detail=f"No {pollutant} data")
        
        # Prepare features
        latest_pivot['datetime_utc'] = pd.to_datetime(latest_pivot['datetime_utc'])
        latest_pivot['hour'] = latest_pivot['datetime_utc'].dt.hour
        latest_pivot['day_of_week'] = latest_pivot['datetime_utc'].dt.dayofweek
        latest_pivot['month'] = latest_pivot['datetime_utc'].dt.month
        latest_pivot['is_weekend'] = latest_pivot['day_of_week'].isin([5, 6]).astype(int)
        latest_pivot['hour_sin'] = np.sin(2 * np.pi * latest_pivot['hour'] / 24)
        latest_pivot['hour_cos'] = np.cos(2 * np.pi * latest_pivot['hour'] / 24)
        latest_pivot = latest_pivot.fillna(0)
        
        # Generate ensemble forecasts for 1, 6, 24 hours
        ensemble_forecasts = []
        
        for horizon in [1, 6, 24]:
            model_key = f'{pollutant}_{horizon}h'
            if model_key not in forecaster.models:
                continue
            
            training_features = forecaster.training_features.get(model_key, [])
            if not training_features:
                continue
            
            X_new = pd.DataFrame()
            for feat in training_features:
                X_new[feat] = latest_pivot[feat] if feat in latest_pivot.columns else 0
            
            forecast_time = latest_time + timedelta(hours=horizon)
            if 'hour' in X_new.columns:
                X_new['hour'] = forecast_time.hour
                X_new['hour_sin'] = np.sin(2 * np.pi * forecast_time.hour / 24)
                X_new['hour_cos'] = np.cos(2 * np.pi * forecast_time.hour / 24)
            
            # Get predictions from all models in ensemble
            predictions = []
            for model_name, model in forecaster.models[model_key].items():
                pred = model.predict(X_new)
                predictions.append(pred[0])
            
            predictions = np.array(predictions)
            mean_pred = predictions.mean()
            std_pred = predictions.std()
            min_pred = predictions.min()
            max_pred = predictions.max()
            
            # Calculate model agreement (inverse of coefficient of variation)
            cv = std_pred / mean_pred if mean_pred > 0 else 1
            model_agreement = max(0, 1 - cv)
            
            # 95% confidence interval
            ci_lower = mean_pred - 1.96 * std_pred
            ci_upper = mean_pred + 1.96 * std_pred
            
            ensemble_forecasts.append(EnsembleForecast(
                date=forecast_time.strftime("%Y-%m-%d %H:%M"),
                pollutant=pollutant,
                mean_prediction=round(mean_pred, 2),
                min_prediction=round(min_pred, 2),
                max_prediction=round(max_pred, 2),
                confidence_interval_95=(round(ci_lower, 2), round(ci_upper, 2)),
                model_agreement=round(model_agreement, 3)
            ))
        
        return {
            "city": city,
            "pollutant": pollutant,
            "ensemble_forecasts": ensemble_forecasts,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/health-alerts/{city}/vulnerable-groups", tags=["Health & Safety"])
async def get_vulnerable_groups_alerts(city: str):
    """
    Generate health alerts specific to vulnerable populations
    (elderly, children, asthma patients, outdoor workers)
    """
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        query = """
        SELECT parameter_name, value, datetime_utc
        FROM air_quality_data
        WHERE city ILIKE %s
        ORDER BY datetime_utc DESC
        LIMIT 10
        """
        
        df = pd.read_sql(query, forecaster.conn, params=[f'%{city}%'])
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data for {city}")
        
        # Calculate current AQI
        pollutants = {}
        for pol in ['PM10', 'PM2.5', 'PM25']:
            pol_data = df[df['parameter_name'] == pol]
            if not pol_data.empty:
                pollutants[pol.lower().replace('.', '')] = float(pol_data['value'].mean())
        
        current_aqi = forecaster.calculate_aqi(pollutants) if pollutants else None
        
        if not current_aqi:
            raise HTTPException(status_code=404, detail="Could not calculate AQI")
        
        current_aqi = int(round(current_aqi))
        
        # Define vulnerable groups with different thresholds
        alerts = []
        
        if current_aqi > 100:  # Moderate or worse
            alerts.append({
                "group": "Children & Teenagers",
                "risk_level": "elevated" if current_aqi <= 150 else "high",
                "recommendations": [
                    "Limit prolonged outdoor activities",
                    "Reduce recess time or move activities indoors",
                    "Monitor for breathing difficulties"
                ]
            })
        
        if current_aqi > 100:
            alerts.append({
                "group": "Elderly (65+)",
                "risk_level": "elevated" if current_aqi <= 150 else "high",
                "recommendations": [
                    "Stay indoors when possible",
                    "Keep rescue medications accessible",
                    "Avoid heavy physical exertion"
                ]
            })
        
        if current_aqi > 50:  # Even Good-Moderate affects asthmatics
            alerts.append({
                "group": "Asthma & Respiratory Patients",
                "risk_level": "moderate" if current_aqi <= 100 else "high",
                "recommendations": [
                    "Keep inhaler readily available",
                    "Monitor symptoms closely",
                    "Consider indoor alternatives for exercise"
                ]
            })
        
        if current_aqi > 150:
            alerts.append({
                "group": "Outdoor Workers",
                "risk_level": "high",
                "recommendations": [
                    "Wear N95 masks during work",
                    "Take frequent breaks indoors",
                    "Stay hydrated",
                    "Employers should reduce outdoor work hours"
                ]
            })
        
        return {
            "city": city,
            "current_aqi": current_aqi,
            "category": forecaster._get_aqi_category(current_aqi),
            "color": get_aqi_color(current_aqi),
            "vulnerable_group_alerts": alerts,
            "alert_level": "normal" if current_aqi <= 100 else "caution" if current_aqi <= 150 else "warning",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/spatial/hotspots/{region}", tags=["Spatial Analysis"])
async def get_pollution_hotspots(
    region: str = Path(..., description="City or region name"),  # Changed from Query to Path
    pollutant: str = Query(default="PM25"),
    threshold: float = Query(default=35.0, description="Pollution threshold")
):

    """
    Identify pollution hotspots in a region
    """
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        query = """
        SELECT 
            latitude,
            longitude,
            location_name,
            AVG(value) as avg_value,
            MAX(value) as max_value
        FROM air_quality_data
        WHERE city ILIKE %s
        AND parameter_name = %s
        AND datetime_utc >= NOW() - INTERVAL '24 hours'
        GROUP BY latitude, longitude, location_name
        HAVING AVG(value) > %s
        ORDER BY avg_value DESC
        """
        
        df = pd.read_sql(query, forecaster.conn, params=[f'%{region}%', pollutant, threshold])
        
        if df.empty:
            return {
                "region": region,
                "pollutant": pollutant,
                "hotspots": [],
                "message": f"No locations exceeded threshold of {threshold}"
            }
        
        hotspots = []
        for _, row in df.iterrows():
            value = row['avg_value']
            
            if value > threshold * 3:
                severity = "severe"
                radius = 5.0
            elif value > threshold * 2:
                severity = "high"
                radius = 3.0
            elif value > threshold * 1.5:
                severity = "moderate"
                radius = 2.0
            else:
                severity = "low"
                radius = 1.0
            
            hotspots.append(SpatialHotspot(
                location=row['location_name'],
                latitude=float(row['latitude']),
                longitude=float(row['longitude']),
                pollutant=pollutant,
                value=round(value, 2),
                severity=severity,
                radius_affected_km=radius
            ))
        
        return {
            "region": region,
            "pollutant": pollutant,
            "threshold": threshold,
            "hotspots": hotspots,
            "total_hotspots": len(hotspots),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/weather/wind-dispersion/{city}", tags=["Weather Integration"])
async def get_wind_dispersion_forecast(city: str):
    """
    Analyze wind patterns for pollution dispersion forecast
    """
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        # Get city coordinates
        city_query = """
        SELECT DISTINCT latitude, longitude
        FROM air_quality_data
        WHERE city ILIKE %s
        LIMIT 1
        """
        
        city_df = pd.read_sql(city_query, forecaster.conn, params=[f'%{city}%'])
        
        if city_df.empty:
            raise HTTPException(status_code=404, detail=f"City {city} not found")
        
        lat = float(city_df['latitude'].iloc[0])
        lon = float(city_df['longitude'].iloc[0])
        
        # Try CYGNSS wind data
        try:
            wind_query = """
            SELECT 
                wind_speed_ms,
                timestamp
            FROM cygnss_wind_data
            WHERE latitude BETWEEN %s AND %s
            AND longitude BETWEEN %s AND %s
            AND timestamp >= NOW() - INTERVAL '24 hours'
            ORDER BY timestamp DESC
            LIMIT 100
            """
            
            wind_df = pd.read_sql(wind_query, forecaster.conn, 
                                 params=(lat-1, lat+1, lon-1, lon+1))
            
            if not wind_df.empty:
                avg_wind = float(wind_df['wind_speed_ms'].mean())
                
                # Dispersion forecast
                if avg_wind < 2:
                    dispersion = "poor"
                    forecast = "Pollutants will accumulate. Expect air quality to worsen."
                    impact = "High pollution accumulation risk"
                elif avg_wind < 5:
                    dispersion = "moderate"
                    forecast = "Some dispersion expected. Air quality may gradually improve."
                    impact = "Moderate pollution dispersion"
                elif avg_wind < 10:
                    dispersion = "good"
                    forecast = "Good dispersion expected. Air quality should improve significantly."
                    impact = "Good pollution clearing conditions"
                else:
                    dispersion = "excellent"
                    forecast = "Excellent dispersion. Rapid air quality improvement expected."
                    impact = "Excellent pollution clearing conditions"
                
                return {
                    "city": city,
                    "coordinates": {"latitude": lat, "longitude": lon},
                    "current_wind_speed_ms": round(avg_wind, 2),
                    "current_wind_speed_kmh": round(avg_wind * 3.6, 2),
                    "current_wind_speed_mph": round(avg_wind * 2.237, 2),
                    "dispersion_condition": dispersion,
                    "forecast": forecast,
                    "impact": impact,
                    "data_source": "CYGNSS",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
                }
        except Exception as e:
            print(f"CYGNSS query error: {e}")
        
        # If no CYGNSS data, return unavailable message
        return {
            "city": city,
            "coordinates": {"latitude": lat, "longitude": lon},
            "wind_data_available": False,
            "message": "Wind dispersion data not available for this location. CYGNSS satellite data coverage may be limited in this region.",
            "note": "Wind data is primarily available for coastal and oceanic regions covered by CYGNSS satellites.",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/forecast/nowcast/{city}", tags=["Advanced Forecasting"])
async def get_nowcast(city: str):
    """
    0-6 hour ultra-short-term forecast (nowcast) for immediate planning
    """
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Forecaster not available")
    
    if not forecaster.models:
        raise HTTPException(status_code=503, detail="No trained models")
    
    try:
        # Similar to regular forecast but only 1h and 6h horizons
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=6)
        
        query = """
        SELECT datetime_utc, latitude, longitude, city, parameter_name, value, location_name, location_id
        FROM air_quality_data
        WHERE city ILIKE %s AND datetime_utc >= %s
        ORDER BY datetime_utc DESC
        LIMIT 500
        """
        
        station_df = pd.read_sql(query, forecaster.conn, params=[f'%{city}%', start_date])
        
        if station_df.empty:
            raise HTTPException(status_code=404, detail=f"No recent data for {city}")
        
        latest_time = station_df['datetime_utc'].max()
        latest_data = station_df[station_df['datetime_utc'] == latest_time]
        
        latest_pivot = latest_data.pivot_table(
            index=['datetime_utc', 'latitude', 'longitude', 'location_id', 'city', 'location_name'],
            columns='parameter_name',
            values='value',
            aggfunc='mean'
        ).reset_index()
        
        # Prepare features
        latest_pivot['datetime_utc'] = pd.to_datetime(latest_pivot['datetime_utc'])
        latest_pivot['hour'] = latest_pivot['datetime_utc'].dt.hour
        latest_pivot['day_of_week'] = latest_pivot['datetime_utc'].dt.dayofweek
        latest_pivot['month'] = latest_pivot['datetime_utc'].dt.month
        latest_pivot['is_weekend'] = latest_pivot['day_of_week'].isin([5, 6]).astype(int)
        latest_pivot['hour_sin'] = np.sin(2 * np.pi * latest_pivot['hour'] / 24)
        latest_pivot['hour_cos'] = np.cos(2 * np.pi * latest_pivot['hour'] / 24)
        latest_pivot = latest_pivot.fillna(0)
        
        nowcasts = []
        
        for horizon in [1, 6]:  # 1 hour and 6 hours only
            for pollutant in ['PM10', 'PM2.5', 'PM25']:
                if pollutant not in latest_pivot.columns:
                    continue
                
                model_key = f'{pollutant}_{horizon}h'
                if model_key not in forecaster.models:
                    continue
                
                training_features = forecaster.training_features.get(model_key, [])
                if not training_features:
                    continue
                
                # Create X_new HERE, before try block
                X_new = pd.DataFrame()
                for feat in training_features:
                    X_new[feat] = latest_pivot[feat] if feat in latest_pivot.columns else 0
                
                forecast_time = latest_time + timedelta(hours=horizon)
                if 'hour' in X_new.columns:
                    X_new['hour'] = forecast_time.hour
                    X_new['hour_sin'] = np.sin(2 * np.pi * forecast_time.hour / 24)
                    X_new['hour_cos'] = np.cos(2 * np.pi * forecast_time.hour / 24)
                
                try:
                    prediction = forecaster.predict_with_uncertainty(X_new, pollutant, horizon)
                    
                    nowcasts.append({
                        "time": forecast_time.strftime("%Y-%m-%d %H:%M"),
                        "hours_ahead": horizon,
                        "pollutant": pollutant,
                        "predicted_value": round(prediction['mean'][0], 2),
                        "confidence_lower": round(prediction['lower_95'][0], 2),
                        "confidence_upper": round(prediction['upper_95'][0], 2)
                    })
                except Exception as e:
                    print(f"Error predicting {pollutant} at {horizon}h: {e}")
                    continue
        
        return {
            "city": city,
            "nowcasts": nowcasts,
            "current_time": latest_time.strftime("%Y-%m-%d %H:%M UTC"),
            "forecast_type": "nowcast",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/weather/combined-forecast/{city}", tags=["Weather Integration"])
async def get_combined_weather_aqi_forecast(city: str):
    """
    Combined weather and AQI forecast showing how weather affects air quality
    """
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        # Get city coordinates
        city_query = """
        SELECT DISTINCT latitude, longitude
        FROM air_quality_data
        WHERE city ILIKE %s
        LIMIT 1
        """
        
        city_df = pd.read_sql(city_query, forecaster.conn, params=[f'%{city}%'])
        
        if city_df.empty:
            raise HTTPException(status_code=404, detail=f"City {city} not found")
        
        lat = float(city_df['latitude'].iloc[0])
        lon = float(city_df['longitude'].iloc[0])
        
        # Get weather data from enhanced_weather_grid_data
        weather_query = """
        SELECT 
            timestamp,
            temperature_c,
            humidity_percent,
            wind_speed_kmh,
            precipitation_mm,
            pressure_hpa
        FROM enhanced_weather_grid_data
        WHERE latitude BETWEEN %s AND %s
        AND longitude BETWEEN %s AND %s
        AND timestamp >= NOW() - INTERVAL '24 hours'
        ORDER BY timestamp DESC
        LIMIT 10
        """
        
        weather_df = pd.read_sql(weather_query, forecaster.conn,
                                params=[float(lat-0.5), float(lat+0.5), float(lon-0.5), float(lon+0.5)])

        
        # Get PBLH data
        pblh_query = """
        SELECT 
            timestamp,
            pbl_height_m
        FROM pblh_data
        WHERE latitude BETWEEN %s AND %s
        AND longitude BETWEEN %s AND %s
        AND timestamp >= NOW() - INTERVAL '24 hours'
        ORDER BY timestamp DESC
        LIMIT 10
        """
        
        pblh_df = pd.read_sql(pblh_query, forecaster.conn,
                             params=[lat-0.5, lat+0.5, lon-0.5, lon+0.5])
        
        # Get current AQI
        aqi_query = """
        SELECT parameter_name, value
        FROM air_quality_data
        WHERE city ILIKE %s
        ORDER BY datetime_utc DESC
        LIMIT 10
        """
        
        aqi_df = pd.read_sql(aqi_query, forecaster.conn, params=[f'%{city}%'])
        
        current_weather = {}
        if not weather_df.empty:
            latest_weather = weather_df.iloc[0]
            current_weather = {
                "temperature_c": round(latest_weather['temperature_c'], 1) if pd.notna(latest_weather['temperature_c']) else None,
                "humidity_percent": round(latest_weather['humidity_percent'], 1) if pd.notna(latest_weather['humidity_percent']) else None,
                "wind_speed_kmh": round(latest_weather['wind_speed_kmh'], 1) if pd.notna(latest_weather['wind_speed_kmh']) else None,
                "precipitation_mm": round(latest_weather['precipitation_mm'], 1) if pd.notna(latest_weather['precipitation_mm']) else None
            }
        
        current_pblh = None
        if not pblh_df.empty:
            current_pblh = round(pblh_df.iloc[0]['pbl_height_m'], 1)
        
        # Calculate current AQI
        pollutants = {}
        for pol in ['PM10', 'PM2.5', 'PM25']:
            pol_data = aqi_df[aqi_df['parameter_name'] == pol]
            if not pol_data.empty:
                pollutants[pol.lower().replace('.', '')] = float(pol_data['value'].mean())
        
        current_aqi = forecaster.calculate_aqi(pollutants) if pollutants else None
        
        # Generate weather impact analysis
        impact_factors = []
        
        if current_weather.get('wind_speed_kmh'):
            wind = current_weather['wind_speed_kmh']
            if wind < 5:
                impact_factors.append({
                    "factor": "Low wind speed",
                    "impact": "negative",
                    "description": "Pollutants accumulating due to low wind dispersion"
                })
            elif wind > 15:
                impact_factors.append({
                    "factor": "High wind speed",
                    "impact": "positive",
                    "description": "Good pollutant dispersion expected"
                })
        
        if current_weather.get('humidity_percent'):
            humidity = current_weather['humidity_percent']
            if humidity > 70:
                impact_factors.append({
                    "factor": "High humidity",
                    "impact": "negative",
                    "description": "Particles absorb moisture, worsening visibility and health effects"
                })
        
        if current_weather.get('precipitation_mm') and current_weather['precipitation_mm'] > 0:
            impact_factors.append({
                "factor": "Precipitation",
                "impact": "positive",
                "description": "Rain washing pollutants from atmosphere"
            })
        
        if current_pblh:
            if current_pblh < 500:
                impact_factors.append({
                    "factor": "Low boundary layer",
                    "impact": "negative",
                    "description": "Shallow mixing layer trapping pollutants near surface"
                })
            elif current_pblh > 1500:
                impact_factors.append({
                    "factor": "High boundary layer",
                    "impact": "positive",
                    "description": "Deep mixing allowing pollutant dispersion"
                })
        
        return {
            "city": city,
            "current_aqi": int(round(current_aqi)) if current_aqi else None,
            "current_weather": current_weather,
            "boundary_layer_height_m": current_pblh,
            "weather_impact_factors": impact_factors,
            "forecast_summary": "Weather conditions are " + 
                ("favorable" if len([f for f in impact_factors if f['impact'] == 'positive']) > 
                                len([f for f in impact_factors if f['impact'] == 'negative'])
                else "unfavorable") + " for air quality",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/data-sources/comparison", tags=["Data Integration"])
async def compare_data_sources(
    city: str,
    sources: str = Query(..., description="Comma-separated: TEMPO,Pandora,OpenAQ,MERRA2")
):
    """
    Compare data availability and quality across different sources
    """
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        source_list = [s.strip() for s in sources.split(',')]
        comparison = []
        
        # Get city coordinates
        city_query = """
        SELECT DISTINCT latitude, longitude
        FROM air_quality_data
        WHERE city ILIKE %s
        LIMIT 1
        """
        
        city_df = pd.read_sql(city_query, forecaster.conn, params=[f'%{city}%'])
        
        if city_df.empty:
            raise HTTPException(status_code=404, detail=f"City {city} not found")
        
        lat = city_df['latitude'].iloc[0]
        lon = city_df['longitude'].iloc[0]
        
        for source in source_list:
            if source.upper() == 'TEMPO':
                query = """
                SELECT COUNT(*) as count, MAX(observation_datetime) as latest
                FROM tempo_no2_data
                WHERE latitude BETWEEN %s AND %s
                AND longitude BETWEEN %s AND %s
                AND observation_datetime >= NOW() - INTERVAL '7 days'
                """
                df = pd.read_sql(query, forecaster.conn, params=[lat-1, lat+1, lon-1, lon+1])
                
                comparison.append({
                    "source": "TEMPO",
                    "type": "Satellite",
                    "data_points": int(df['count'].iloc[0]),
                    "latest_update": df['latest'].iloc[0].strftime("%Y-%m-%d %H:%M") if df['latest'].iloc[0] else None,
                    "pollutants": ["NO2"],
                    "status": "active" if int(df['count'].iloc[0]) > 0 else "no_data"
                })
            
            elif source.upper() == 'PANDORA':
                query = """
                SELECT COUNT(*) as count, MAX(utc_datetime) as latest
                FROM pandora_hcho_data
                WHERE latitude BETWEEN %s AND %s
                AND longitude BETWEEN %s AND %s
                AND utc_datetime >= NOW() - INTERVAL '7 days'
                """
                df = pd.read_sql(query, forecaster.conn, params=[lat-1, lat+1, lon-1, lon+1])
                
                comparison.append({
                    "source": "Pandora",
                    "type": "Ground-based spectrometer",
                    "data_points": int(df['count'].iloc[0]),
                    "latest_update": df['latest'].iloc[0].strftime("%Y-%m-%d %H:%M") if df['latest'].iloc[0] else None,
                    "pollutants": ["HCHO"],
                    "status": "active" if int(df['count'].iloc[0]) > 0 else "no_data"
                })
            
            elif source.upper() == 'OPENAQ':
                query = """
                SELECT COUNT(*) as count, MAX(datetime_utc) as latest
                FROM air_quality_data
                WHERE city ILIKE %s
                AND datetime_utc >= NOW() - INTERVAL '7 days'
                """
                df = pd.read_sql(query, forecaster.conn, params=[f'%{city}%'])
                
                comparison.append({
                    "source": "OpenAQ",
                    "type": "Ground stations",
                    "data_points": int(df['count'].iloc[0]),
                    "latest_update": df['latest'].iloc[0].strftime("%Y-%m-%d %H:%M") if df['latest'].iloc[0] else None,
                    "pollutants": ["PM2.5", "PM10", "NO2", "O3", "CO", "SO2"],
                    "status": "active" if int(df['count'].iloc[0]) > 0 else "no_data"
                })
            
            elif source.upper() == 'MERRA2':
                query = """
                SELECT COUNT(*) as count, MAX(granule_time_start) as latest
                FROM merra2_slv_data
                WHERE latitude BETWEEN %s AND %s
                AND longitude BETWEEN %s AND %s
                AND granule_time_start >= NOW() - INTERVAL '7 days'
                """
                df = pd.read_sql(query, forecaster.conn, params=[lat-0.5, lat+0.5, lon-0.5, lon+0.5])
                
                comparison.append({
                    "source": "MERRA-2",
                    "type": "Reanalysis/Meteorological",
                    "data_points": int(df['count'].iloc[0]),
                    "latest_update": df['latest'].iloc[0].strftime("%Y-%m-%d %H:%M") if df['latest'].iloc[0] else None,
                    "pollutants": ["Various meteorological parameters"],
                    "status": "active" if int(df['count'].iloc[0]) > 0 else "no_data"
                })
        
        return {
            "city": city,
            "coordinates": {"latitude": lat, "longitude": lon},
            "data_sources": comparison,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/ml/feature-importance/{pollutant}", tags=["Machine Learning"])
async def get_feature_importance(pollutant: str = "PM25", horizon: int = 24):
    if not forecaster:
        raise HTTPException(status_code=503, detail="Forecaster not initialized")
    
    model_key = f'{pollutant}_{horizon}h'
    
    if model_key not in forecaster.feature_importance:
        available_pollutants = list(set([k.split('_')[0] for k in forecaster.feature_importance.keys()]))
        raise HTTPException(
            status_code=404, 
            detail=f"No models trained for {pollutant}. Available pollutants: {', '.join(available_pollutants)}"
        )

    
    try:
        importance_dict = forecaster.feature_importance[model_key]
        
        # Sort by importance
        sorted_features = sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)
        
        top_features = [
            {
                "feature": feat,
                "importance": round(imp, 4),
                "category": "temporal" if any(x in feat.lower() for x in ['hour', 'day', 'month', 'weekend']) else
                           "meteorological" if any(x in feat.lower() for x in ['temp', 'wind', 'humidity', 'pressure']) else
                           "pollutant" if any(x in feat.lower() for x in ['pm', 'no2', 'o3', 'lag', 'rolling']) else
                           "boundary_layer" if 'pbl' in feat.lower() else
                           "fire" if 'fire' in feat.lower() else "other"
            }
            for feat, imp in sorted_features[:20]
        ]
        
        # Category summary
        category_importance = {}
        for feat in top_features:
            cat = feat['category']
            category_importance[cat] = category_importance.get(cat, 0) + feat['importance']
        
        return {
            "pollutant": pollutant,
            "forecast_horizon_hours": horizon,
            "top_features": top_features,
            "category_importance": category_importance,
            "interpretation": {
                "most_important": top_features[0]['feature'] if top_features else None,
                "dominant_category": max(category_importance.items(), key=lambda x: x[1])[0] if category_importance else None
            },
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/export/geojson/{city}", tags=["Data Export"])
async def export_geojson(
    city: str,
    pollutant: str = Query(default="PM25"),
    hours: int = Query(default=24, ge=1, le=168)
):
    """
    Export air quality data as GeoJSON for mapping applications
    """
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        start_time = datetime.now() - timedelta(hours=hours)
        
        query = """
        SELECT 
            location_id,
            location_name,
            latitude,
            longitude,
            datetime_utc,
            AVG(value) as avg_value
        FROM air_quality_data
        WHERE city ILIKE %s
        AND parameter_name = %s
        AND datetime_utc >= %s
        GROUP BY location_id, location_name, latitude, longitude, datetime_utc
        ORDER BY datetime_utc DESC
        """
        
        df = pd.read_sql(query, forecaster.conn, params=[f'%{city}%', pollutant, start_time])
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data found")
        
        # Create GeoJSON
        features = []
        
        for location_id in df['location_id'].unique():
            location_data = df[df['location_id'] == location_id]
            latest = location_data.iloc[0]
            
            # Calculate AQI if PM pollutant
            if pollutant in ['PM25', 'PM2.5', 'PM10']:
                aqi = forecaster.calculate_aqi({pollutant.lower().replace('.', ''): latest['avg_value']})
                aqi_value = int(round(aqi)) if aqi else None
            else:
                aqi_value = None
            
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(latest['longitude']), float(latest['latitude'])]
                },
                "properties": {
                    "location_id": str(latest['location_id']),
                    "location_name": latest['location_name'],
                    "pollutant": pollutant,
                    "value": round(float(latest['avg_value']), 2),
                    "aqi": aqi_value,
                    "timestamp": latest['datetime_utc'].strftime("%Y-%m-%d %H:%M:%S"),
                    "city": city
                }
            }
            features.append(feature)
        
        geojson = {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "city": city,
                "pollutant": pollutant,
                "period_hours": hours,
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
            }
        }
        
        return JSONResponse(content=geojson)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/trends/{city}/seasonal-patterns", tags=["Analytics"])
async def get_seasonal_patterns(city: str, pollutant: str = "PM25"):
    """
    Analyze seasonal patterns in air quality
    """
    if not forecaster or not forecaster.conn:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    try:
        query = """
        SELECT 
            EXTRACT(MONTH FROM datetime_utc) as month,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value,
            COUNT(*) as data_points
        FROM air_quality_data
        WHERE city ILIKE %s
        AND parameter_name = %s
        GROUP BY EXTRACT(MONTH FROM datetime_utc)
        ORDER BY month
        """
        
        df = pd.read_sql(query, forecaster.conn, params=[f'%{city}%', pollutant])
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data for analysis")
        
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        
        seasonal_data = []
        for _, row in df.iterrows():
            month_idx = int(row['month']) - 1
            seasonal_data.append({
                "month": months[month_idx],
                "month_number": int(row['month']),
                "average": round(float(row['avg_value']), 2),
                "min": round(float(row['min_value']), 2),
                "max": round(float(row['max_value']), 2),
                "data_points": int(row['data_points'])
            })
        
        # Identify worst and best months
        worst_month = max(seasonal_data, key=lambda x: x['average'])
        best_month = min(seasonal_data, key=lambda x: x['average'])
        
        return {
            "city": city,
            "pollutant": pollutant,
            "seasonal_patterns": seasonal_data,
            "worst_month": worst_month['month'],
            "best_month": best_month['month'],
            "variation": round(worst_month['average'] - best_month['average'], 2),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# Update the root endpoint to include new endpoints
@app.get("/")
async def root():
    return {
        "message": "Air Quality Forecast & Chat API",
        "version": "2.0.0",
        "status": "operational" if (forecaster and forecaster.conn) or query_system else "limited",
        "features": {
            "forecasting": forecaster is not None and forecaster.conn is not None,
            "chat": query_system is not None,
            "ml_models": len(forecaster.models) if forecaster else 0
        },
        "endpoint_categories": {
            "forecasting": ["/api/forecast/{city}", "/api/forecast/ensemble/{city}", "/api/forecast/nowcast/{city}"],
            "analytics": ["/api/trends/{city}/historical", "/api/trends/{city}/seasonal-patterns"],
            "health_safety": ["/api/health-recommendations/{city}", "/api/health-alerts/{city}/vulnerable-groups"],
            "emergency": ["/api/wildfire/impact/{city}"],
            "monitoring": ["/api/monitoring-stations/{city}", "/api/monitoring-networks"],
            "validation": ["/api/validation/satellite-vs-ground/{city}"],
            "spatial": ["/api/spatial/hotspots/{region}"],
            "weather": ["/api/weather/wind-dispersion/{city}", "/api/weather/combined-forecast/{city}"],
            "data_integration": ["/api/data-sources/comparison"],
            "ml_insights": ["/api/ml/feature-importance/{pollutant}"],
            "export": ["/api/export/geojson/{city}"],
            "chat": ["/api/chat"],
            "docs": "/docs"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

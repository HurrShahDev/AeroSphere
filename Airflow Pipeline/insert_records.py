import psycopg2

# Move imports to the TOP of the file
from api_requests import (
    fetch_tempo_hcho_data, 
    collect_tempo_no2_data, 
    fetch_merra2_met_data, 
    fetch_all_air_quality_data, 
    fetch_and_process_pandora_hcho_data,
    fetch_pblh_data,
    no2_pipeline,
    run_tolnet_fetcher,
    fetch_nasa_aerosol_data,
    fetch_and_process_goes_data,
    fetch_cygnss_data,
    run_fetch_tempo_o3,
    O3_OZONE_WAQI_DATA,
    FIRE_SMOKE_DETECTION_DATA,
    ENHANCE_METEROLOGY_DATA,
    MISSING_SO2_DATA_WAQI,
    MISSING_CO_DATA_WAQI,
    MISSING_PM_2_POINT_5_DATA,
    MISSING_VEHICLE_EMISSION_DATA_WAQI_WITH_NOX_AND_VOCS
    
)



from datetime import datetime, date
import os
import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




def connect_to_db():
    print("Connecting to the PostgreSQL database...")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5000",
            dbname="db",
            user="db_user",
            password="db_password"
        )
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise

def create_table(conn):
    print("Creating tables if not exist...")
    try:
        cursor = conn.cursor()
        
        # Create table for TEMPO HCHO data with unique constraint
        create_hcho_table_query = """
        CREATE TABLE IF NOT EXISTS tempo_hcho_data (
            id SERIAL PRIMARY KEY,
            source_file VARCHAR(255),
            export_date TIMESTAMP,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            hcho_total_column DOUBLE PRECISION,
            hcho_units VARCHAR(50),
            hcho_uncertainty DOUBLE PRECISION,
            quality_flag INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(latitude, longitude, hcho_total_column, export_date)
        );
        """
        
        # Create table for TEMPO NO2 data with unique constraint
        create_no2_table_query = """
        CREATE TABLE IF NOT EXISTS tempo_no2_data (
            id SERIAL PRIMARY KEY,
            source_file VARCHAR(255),
            observation_datetime TIMESTAMP,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            no2_tropospheric_column DOUBLE PRECISION,
            log_no2 DOUBLE PRECISION,
            hours_old DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(latitude, longitude, no2_tropospheric_column, observation_datetime)
        );
        """
        # Create MERRA-2 meteorological data table
        create_merra2_table_query = """
        CREATE TABLE IF NOT EXISTS merra2_slv_data (
            id SERIAL PRIMARY KEY,
            source_file VARCHAR(255),
            collection VARCHAR(255),
            short_name VARCHAR(50),
            version VARCHAR(20),
            export_date TIMESTAMP,
            granule_time_start TIMESTAMP,
            granule_time_end TIMESTAMP,
            latitude DECIMAL(10, 6) NOT NULL,
            longitude DECIMAL(10, 6) NOT NULL,
            variable_name VARCHAR(50) NOT NULL,
            variable_value DECIMAL(15, 6),
            variable_units VARCHAR(50),
            quality_flag INTEGER,
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(latitude, longitude, variable_name, granule_time_start, variable_value)
        );   
        
        CREATE INDEX IF NOT EXISTS idx_merra2_location 
        ON merra2_slv_data(latitude, longitude);
        
        CREATE INDEX IF NOT EXISTS idx_merra2_time 
        ON merra2_slv_data(granule_time_start);
        
        CREATE INDEX IF NOT EXISTS idx_merra2_variable 
        ON merra2_slv_data(variable_name);
        """


        # Create Air Quality data table
        create_air_quality_table_query = """
        CREATE TABLE IF NOT EXISTS air_quality_data (
            id SERIAL PRIMARY KEY,
            datetime_utc TIMESTAMP NOT NULL,
            datetime_local TIMESTAMP,
            value DOUBLE PRECISION NOT NULL,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            location_id VARCHAR(100),
            location_name VARCHAR(255),
            city VARCHAR(255),
            state VARCHAR(100),
            country VARCHAR(100),
            parameter_name VARCHAR(50) NOT NULL,
            parameter_display_name VARCHAR(50),
            units VARCHAR(50),
            sensor_id VARCHAR(255),
            provider VARCHAR(100),
            data_source VARCHAR(100),
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(datetime_utc, latitude, longitude, parameter_name, value, sensor_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_air_quality_location 
        ON air_quality_data(latitude, longitude);
        
        CREATE INDEX IF NOT EXISTS idx_air_quality_time 
        ON air_quality_data(datetime_utc);
        
        CREATE INDEX IF NOT EXISTS idx_air_quality_parameter 
        ON air_quality_data(parameter_name);
        
        CREATE INDEX IF NOT EXISTS idx_air_quality_city 
        ON air_quality_data(city);
        """

        # Create Pandora HCHO data table
        create_pandora_hcho_table_query = """
        CREATE TABLE IF NOT EXISTS pandora_hcho_data (
            id SERIAL PRIMARY KEY,
            utc_datetime TIMESTAMP NOT NULL,
            fractional_day DOUBLE PRECISION,
            solar_zenith_angle DOUBLE PRECISION,
            solar_azimuth_angle DOUBLE PRECISION,
            elevation_angle DOUBLE PRECISION,
            azimuth_angle DOUBLE PRECISION,
            hcho_slant_column DOUBLE PRECISION,
            hcho_slant_column_error DOUBLE PRECISION,
            hcho_vertical_column DOUBLE PRECISION,
            hcho_vertical_column_error DOUBLE PRECISION,
            air_mass_factor DOUBLE PRECISION,
            fitting_rms DOUBLE PRECISION,
            temperature DOUBLE PRECISION,
            quality_flag INTEGER,
            processing_code INTEGER,
            site VARCHAR(100),
            location VARCHAR(255),
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            instrument VARCHAR(100),
            data_level VARCHAR(10),
            source_file VARCHAR(255),
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(utc_datetime, latitude, longitude, hcho_vertical_column, site, instrument)
        );
        
        CREATE INDEX IF NOT EXISTS idx_pandora_hcho_location 
        ON pandora_hcho_data(latitude, longitude);
        
        CREATE INDEX IF NOT EXISTS idx_pandora_hcho_time 
        ON pandora_hcho_data(utc_datetime);
        
        CREATE INDEX IF NOT EXISTS idx_pandora_hcho_site 
        ON pandora_hcho_data(site);
        
        CREATE INDEX IF NOT EXISTS idx_pandora_hcho_quality 
        ON pandora_hcho_data(quality_flag);
        """
        
        create_pblh_table_query = """
        CREATE TABLE IF NOT EXISTS pblh_data (
            id SERIAL PRIMARY KEY,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            pbl_height_m DOUBLE PRECISION NOT NULL,
            data_source VARCHAR(100),
            variable VARCHAR(50),
            unit VARCHAR(50),
            collection VARCHAR(100),
            collection_version VARCHAR(50),
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(latitude, longitude, timestamp, pbl_height_m)
        );
        
        CREATE INDEX IF NOT EXISTS idx_pblh_location 
        ON pblh_data(latitude, longitude);
        
        CREATE INDEX IF NOT EXISTS idx_pblh_time 
        ON pblh_data(timestamp);
        
        CREATE INDEX IF NOT EXISTS idx_pblh_height 
        ON pblh_data(pbl_height_m);
        """

        # Create NO2 Pipeline data table
        create_no2_pipeline_table_query = """
        CREATE TABLE IF NOT EXISTS no2_pipeline_data (
            id SERIAL PRIMARY KEY,
            no2_tropospheric_column DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            latitude DOUBLE PRECISION NOT NULL,
            observation_datetime_utc TIMESTAMP NOT NULL,
            file_name VARCHAR(255),
            data_source VARCHAR(100),
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(latitude, longitude, no2_tropospheric_column, observation_datetime_utc, file_name)
        );
        
        CREATE INDEX IF NOT EXISTS idx_no2_pipeline_location 
        ON no2_pipeline_data(latitude, longitude);
        
        CREATE INDEX IF NOT EXISTS idx_no2_pipeline_time 
        ON no2_pipeline_data(observation_datetime_utc);
        
        CREATE INDEX IF NOT EXISTS idx_no2_pipeline_value 
        ON no2_pipeline_data(no2_tropospheric_column);
        """


        create_tolnet_table_query = """
        CREATE TABLE IF NOT EXISTS tolnet_data (
            id SERIAL PRIMARY KEY,
            granule_id VARCHAR(100) NOT NULL,
            title VARCHAR(255),
            collection VARCHAR(255),
            start_date TIMESTAMP NOT NULL,
            end_date TIMESTAMP NOT NULL,
            updated TIMESTAMP,
            download_url TEXT,
            file_count INTEGER,
            variable VARCHAR(100),
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(granule_id, start_date, end_date)
        );
        
        CREATE INDEX IF NOT EXISTS idx_tolnet_granule 
        ON tolnet_data(granule_id);
        
        CREATE INDEX IF NOT EXISTS idx_tolnet_time 
        ON tolnet_data(start_date, end_date);
        
        CREATE INDEX IF NOT EXISTS idx_tolnet_collection 
        ON tolnet_data(collection);
        """
        
        create_aerosol_table_query = """
        CREATE TABLE IF NOT EXISTS nasa_aerosol_data (
            id SERIAL PRIMARY KEY,
            satellite VARCHAR(50) NOT NULL,
            time_start TIMESTAMP NOT NULL,
            title VARCHAR(255),
            hour TIMESTAMP,
            granule_count INTEGER,
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(satellite, time_start, title)
        );
        
        CREATE INDEX IF NOT EXISTS idx_aerosol_satellite 
        ON nasa_aerosol_data(satellite);
        
        CREATE INDEX IF NOT EXISTS idx_aerosol_time 
        ON nasa_aerosol_data(time_start);
        
        CREATE INDEX IF NOT EXISTS idx_aerosol_hour 
        ON nasa_aerosol_data(hour);
        """
        
        create_goes_table_query = """
        CREATE TABLE IF NOT EXISTS goes_satellite_data (
            id SERIAL PRIMARY KEY,
            satellite VARCHAR(50) NOT NULL,
            product VARCHAR(100) NOT NULL,
            filename VARCHAR(255) NOT NULL,
            size_mb DOUBLE PRECISION,
            timestamp TIMESTAMP NOT NULL,
            bucket VARCHAR(100),
            key TEXT,
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(satellite, filename, timestamp)
        );
        
        CREATE INDEX IF NOT EXISTS idx_goes_satellite 
        ON goes_satellite_data(satellite);
        
        CREATE INDEX IF NOT EXISTS idx_goes_timestamp 
        ON goes_satellite_data(timestamp);
        
        CREATE INDEX IF NOT EXISTS idx_goes_product 
        ON goes_satellite_data(product);
        """
        # Table for GOES processed data (main data)
        create_goes_processed_data_query = """
        CREATE TABLE IF NOT EXISTS goes_processed_data (
            id SERIAL PRIMARY KEY,
            satellite VARCHAR(50) NOT NULL,
            variable VARCHAR(100) NOT NULL,
            value DOUBLE PRECISION,
            data_type VARCHAR(50),
            row_index INTEGER,
            col_index INTEGER,
            index_1d INTEGER,
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(satellite, variable, row_index, col_index, index_1d, collection_timestamp)
        );
        
        CREATE INDEX IF NOT EXISTS idx_goes_proc_satellite 
        ON goes_processed_data(satellite);
        
        CREATE INDEX IF NOT EXISTS idx_goes_proc_variable 
        ON goes_processed_data(variable);
        
        CREATE INDEX IF NOT EXISTS idx_goes_proc_type 
        ON goes_processed_data(data_type);
        """
        
        # Table for GOES imagery data (2D arrays only)
        create_goes_imagery_query = """
        CREATE TABLE IF NOT EXISTS goes_imagery_data (
            id SERIAL PRIMARY KEY,
            satellite VARCHAR(50) NOT NULL,
            variable VARCHAR(100) NOT NULL,
            row_index INTEGER NOT NULL,
            col_index INTEGER NOT NULL,
            value DOUBLE PRECISION NOT NULL,
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(satellite, variable, row_index, col_index, collection_timestamp)
        );
        
        CREATE INDEX IF NOT EXISTS idx_goes_img_satellite 
        ON goes_imagery_data(satellite);
        
        CREATE INDEX IF NOT EXISTS idx_goes_img_variable 
        ON goes_imagery_data(variable);
        
        CREATE INDEX IF NOT EXISTS idx_goes_img_location 
        ON goes_imagery_data(row_index, col_index);
        """

        # Table for CYGNSS wind data
        create_cygnss_data_query = """
        CREATE TABLE IF NOT EXISTS cygnss_wind_data (
            id SERIAL PRIMARY KEY,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            wind_speed_ms DOUBLE PRECISION NOT NULL,
            hours_ago INTEGER NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            data_mode VARCHAR(50),
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(latitude, longitude, timestamp, hours_ago)
        );
        
        CREATE INDEX IF NOT EXISTS idx_cygnss_location 
        ON cygnss_wind_data(latitude, longitude);
        
        CREATE INDEX IF NOT EXISTS idx_cygnss_time 
        ON cygnss_wind_data(timestamp);
        
        CREATE INDEX IF NOT EXISTS idx_cygnss_hours 
        ON cygnss_wind_data(hours_ago);
        """
        
        # Table for CYGNSS temporal analysis (hourly summaries)
        create_cygnss_temporal_query = """
        CREATE TABLE IF NOT EXISTS cygnss_temporal_analysis (
            id SERIAL PRIMARY KEY,
            hours_ago INTEGER NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            point_count INTEGER,
            mean_wind DOUBLE PRECISION,
            min_wind DOUBLE PRECISION,
            max_wind DOUBLE PRECISION,
            std_wind DOUBLE PRECISION,
            data_mode VARCHAR(50),
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(hours_ago, timestamp, collection_timestamp)
        );
        
        CREATE INDEX IF NOT EXISTS idx_cygnss_temporal_hours 
        ON cygnss_temporal_analysis(hours_ago);
        
        CREATE INDEX IF NOT EXISTS idx_cygnss_temporal_time 
        ON cygnss_temporal_analysis(timestamp);
        """
        create_tempo_o3_table_query = """
        CREATE TABLE IF NOT EXISTS tempo_o3_data (
            id SERIAL PRIMARY KEY,
            short_name VARCHAR(100),
            version VARCHAR(50),
            time_start TIMESTAMP,
            time_end TIMESTAMP,
            size_mb DOUBLE PRECISION,
            data_url TEXT,
            raw_data TEXT,
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(data_url, time_start)
        );
        
        CREATE INDEX IF NOT EXISTS idx_tempo_o3_time 
        ON tempo_o3_data(time_start);
        
        CREATE INDEX IF NOT EXISTS idx_tempo_o3_version 
        ON tempo_o3_data(version);
        """

        create_o3_waqi_table_query = """
        CREATE TABLE IF NOT EXISTS o3_waqi_data (
            id SERIAL PRIMARY KEY,
            city VARCHAR(100),
            station_name VARCHAR(255),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            datetime_utc TIMESTAMP,
            o3_aqi DOUBLE PRECISION,
            dominant_pollutant VARCHAR(50),
            overall_aqi INTEGER,
            temperature DOUBLE PRECISION,
            humidity DOUBLE PRECISION,
            pressure DOUBLE PRECISION,
            station_url TEXT,
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(station_name, datetime_utc, latitude, longitude)
        );
        
        CREATE INDEX IF NOT EXISTS idx_o3_waqi_city 
        ON o3_waqi_data(city);
        
        CREATE INDEX IF NOT EXISTS idx_o3_waqi_location 
        ON o3_waqi_data(latitude, longitude);
        
        CREATE INDEX IF NOT EXISTS idx_o3_waqi_time 
        ON o3_waqi_data(datetime_utc);
        
        CREATE INDEX IF NOT EXISTS idx_o3_waqi_aqi 
        ON o3_waqi_data(o3_aqi);
        """
        create_fire_data_table_query = """
        CREATE TABLE IF NOT EXISTS fire_detection_data (
            id SERIAL PRIMARY KEY,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            bright_ti4 DOUBLE PRECISION,
            bright_ti5 DOUBLE PRECISION,
            scan DOUBLE PRECISION,
            track DOUBLE PRECISION,
            acq_date DATE NOT NULL,
            acq_time INTEGER,
            satellite VARCHAR(50),
            instrument VARCHAR(50),
            confidence VARCHAR(10),
            version VARCHAR(50),
            frp DOUBLE PRECISION,
            daynight CHAR(1),
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(latitude, longitude, acq_date, acq_time, satellite)
        );
        
        CREATE INDEX IF NOT EXISTS idx_fire_location 
        ON fire_detection_data(latitude, longitude);
        
        CREATE INDEX IF NOT EXISTS idx_fire_date 
        ON fire_detection_data(acq_date);
        
        CREATE INDEX IF NOT EXISTS idx_fire_confidence 
        ON fire_detection_data(confidence);
        
        CREATE INDEX IF NOT EXISTS idx_fire_frp 
        ON fire_detection_data(frp);
        """

        create_weather_grid_table_query = """
        CREATE TABLE IF NOT EXISTS enhanced_weather_grid_data (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            temperature_c DOUBLE PRECISION,
            humidity_percent DOUBLE PRECISION,
            precipitation_mm DOUBLE PRECISION,
            wind_speed_kmh DOUBLE PRECISION,
            pressure_hpa DOUBLE PRECISION,
            cloud_cover_percent DOUBLE PRECISION,
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(timestamp, latitude, longitude)
        );
        
        CREATE INDEX IF NOT EXISTS idx_weather_grid_location 
        ON enhanced_weather_grid_data(latitude, longitude);
        
        CREATE INDEX IF NOT EXISTS idx_weather_grid_time 
        ON enhanced_weather_grid_data(timestamp);
        
        CREATE INDEX IF NOT EXISTS idx_weather_grid_temp 
        ON enhanced_weather_grid_data(temperature_c);
        """

        
        # Create Air Quality Station Forecast data table
        # Create Air Quality Station Forecast data table
        create_aq_station_forecast_table_query = """
        CREATE TABLE IF NOT EXISTS aq_station_forecast_data (
            id SERIAL PRIMARY KEY,
            station_idx INTEGER NOT NULL,
            station_uid VARCHAR(50),
            aqi INTEGER,
            dominant_pollutant VARCHAR(50),
            city_name VARCHAR(255),
            city_url TEXT,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            observation_time TIMESTAMP NOT NULL,
            timezone VARCHAR(50),
            h DOUBLE PRECISION,
            dew DOUBLE PRECISION,
            t DOUBLE PRECISION,
            p DOUBLE PRECISION,
            w DOUBLE PRECISION,
            wg DOUBLE PRECISION,
            co DOUBLE PRECISION,
            no2 DOUBLE PRECISION,
            o3 DOUBLE PRECISION,
            so2 DOUBLE PRECISION,
            pm10 DOUBLE PRECISION,
            pm25 DOUBLE PRECISION,
            forecast_date DATE,
            forecast_pm10_avg DOUBLE PRECISION,
            forecast_pm10_max DOUBLE PRECISION,
            forecast_pm10_min DOUBLE PRECISION,
            forecast_pm25_avg DOUBLE PRECISION,
            forecast_pm25_max DOUBLE PRECISION,
            forecast_pm25_min DOUBLE PRECISION,
            forecast_uvi_avg DOUBLE PRECISION,
            forecast_uvi_max DOUBLE PRECISION,
            forecast_uvi_min DOUBLE PRECISION,
            data_source VARCHAR(255),
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(station_idx, observation_time, forecast_date)
        );
        
        CREATE INDEX IF NOT EXISTS idx_aq_station_forecast_location 
        ON aq_station_forecast_data(latitude, longitude);
        
        CREATE INDEX IF NOT EXISTS idx_aq_station_forecast_time 
        ON aq_station_forecast_data(observation_time);
        
        CREATE INDEX IF NOT EXISTS idx_aq_station_forecast_date 
        ON aq_station_forecast_data(forecast_date);
        
        CREATE INDEX IF NOT EXISTS idx_aq_station_forecast_idx 
        ON aq_station_forecast_data(station_idx);
        
        CREATE INDEX IF NOT EXISTS idx_aq_station_forecast_aqi 
        ON aq_station_forecast_data(aqi);
        
        CREATE INDEX IF NOT EXISTS idx_aq_station_date_location 
        ON aq_station_forecast_data(forecast_date, latitude, longitude);
        
        CREATE INDEX IF NOT EXISTS idx_aq_station_timeseries 
        ON aq_station_forecast_data(station_idx, observation_time DESC);
        """
        
        # Create WAQI City Air Quality Forecast data table
        create_waqi_city_forecast_table_query = """
        CREATE TABLE IF NOT EXISTS waqi_city_forecast_data (
            id SERIAL PRIMARY KEY,
            station_idx INTEGER NOT NULL,
            aqi INTEGER,
            dominant_pollutant VARCHAR(50),
            city_name VARCHAR(255),
            city_url TEXT,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            observation_time TIMESTAMP NOT NULL,
            timezone VARCHAR(50),
            h DOUBLE PRECISION,
            dew DOUBLE PRECISION,
            t DOUBLE PRECISION,
            p DOUBLE PRECISION,
            w DOUBLE PRECISION,
            wg DOUBLE PRECISION,
            wd DOUBLE PRECISION,
            co DOUBLE PRECISION,
            no2 DOUBLE PRECISION,
            o3 DOUBLE PRECISION,
            so2 DOUBLE PRECISION,
            pm10 DOUBLE PRECISION,
            pm25 DOUBLE PRECISION,
            forecast_date DATE,
            forecast_o3_avg DOUBLE PRECISION,
            forecast_o3_max DOUBLE PRECISION,
            forecast_o3_min DOUBLE PRECISION,
            forecast_pm10_avg DOUBLE PRECISION,
            forecast_pm10_max DOUBLE PRECISION,
            forecast_pm10_min DOUBLE PRECISION,
            forecast_pm25_avg DOUBLE PRECISION,
            forecast_pm25_max DOUBLE PRECISION,
            forecast_pm25_min DOUBLE PRECISION,
            forecast_uvi_avg DOUBLE PRECISION,
            forecast_uvi_max DOUBLE PRECISION,
            forecast_uvi_min DOUBLE PRECISION,
            data_source VARCHAR(100),
            collection_timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(station_idx, observation_time, forecast_date)
        );
        
        CREATE INDEX IF NOT EXISTS idx_waqi_city_location 
        ON waqi_city_forecast_data(latitude, longitude);
        
        CREATE INDEX IF NOT EXISTS idx_waqi_city_time 
        ON waqi_city_forecast_data(observation_time);
        
        CREATE INDEX IF NOT EXISTS idx_waqi_city_forecast_date 
        ON waqi_city_forecast_data(forecast_date);
        
        CREATE INDEX IF NOT EXISTS idx_waqi_city_idx 
        ON waqi_city_forecast_data(station_idx);
        
        CREATE INDEX IF NOT EXISTS idx_waqi_city_name 
        ON waqi_city_forecast_data(city_name);
        
        CREATE INDEX IF NOT EXISTS idx_waqi_city_forecast_aqi 
        ON waqi_city_forecast_data(aqi);
        
        CREATE INDEX IF NOT EXISTS idx_waqi_city_date_location 
        ON waqi_city_forecast_data(forecast_date, latitude, longitude);
        
        CREATE INDEX IF NOT EXISTS idx_waqi_city_timeseries 
        ON waqi_city_forecast_data(station_idx, observation_time DESC);
        """

        
        
        
       


 

        cursor.execute(create_pblh_table_query)
        cursor.execute(create_hcho_table_query)
        cursor.execute(create_no2_table_query)
        cursor.execute(create_merra2_table_query) 
        cursor.execute(create_air_quality_table_query) 
        cursor.execute(create_pandora_hcho_table_query)
        cursor.execute(create_no2_pipeline_table_query)
        cursor.execute(create_tolnet_table_query)
        cursor.execute(create_aerosol_table_query)
        cursor.execute(create_goes_table_query)
        cursor.execute(create_goes_processed_data_query)
        cursor.execute(create_goes_imagery_query)
        cursor.execute(create_cygnss_data_query)
        cursor.execute(create_cygnss_temporal_query)
        cursor.execute(create_tempo_o3_table_query)        
        cursor.execute(create_o3_waqi_table_query)
        cursor.execute(create_fire_data_table_query)
        cursor.execute(create_weather_grid_table_query)
        cursor.execute(create_aq_station_forecast_table_query)
        cursor.execute(create_waqi_city_forecast_table_query)
        


        conn.commit()
        cursor.close()
        print("Tables were created successfully.")
    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        raise

def validate_hcho_record(record, metadata):
    """Validate HCHO record data before insertion"""
    errors = []
    
    # Required fields
    if record.get('latitude') is None:
        errors.append("Missing latitude")
    elif not (-90 <= record.get('latitude') <= 90):
        errors.append(f"Invalid latitude: {record.get('latitude')}")
    
    if record.get('longitude') is None:
        errors.append("Missing longitude")
    elif not (-180 <= record.get('longitude') <= 180):
        errors.append(f"Invalid longitude: {record.get('longitude')}")
    
    if record.get('hcho_total_column') is None:
        errors.append("Missing hcho_total_column")
    elif not isinstance(record.get('hcho_total_column'), (int, float)):
        errors.append(f"Invalid hcho_total_column type: {type(record.get('hcho_total_column'))}")
    
    if metadata.get('export_date') is None:
        errors.append("Missing export_date")
    
    # Optional but should be valid if present
    if record.get('hcho_uncertainty') is not None:
        if not isinstance(record.get('hcho_uncertainty'), (int, float)):
            errors.append(f"Invalid hcho_uncertainty type")
        elif record.get('hcho_uncertainty') < 0:
            errors.append(f"Invalid hcho_uncertainty: must be non-negative")
    
    if record.get('quality_flag') is not None:
        if not isinstance(record.get('quality_flag'), int):
            errors.append(f"Invalid quality_flag type: must be integer")
    
    return errors

def validate_no2_record(record):
    """Validate NO2 record data before insertion"""
    errors = []
    
    # Required fields
    if record.get('latitude') is None:
        errors.append("Missing latitude")
    elif not (-90 <= record.get('latitude') <= 90):
        errors.append(f"Invalid latitude: {record.get('latitude')}")
    
    if record.get('longitude') is None:
        errors.append("Missing longitude")
    elif not (-180 <= record.get('longitude') <= 180):
        errors.append(f"Invalid longitude: {record.get('longitude')}")
    
    if record.get('no2_tropospheric_column') is None:
        errors.append("Missing no2_tropospheric_column")
    elif not isinstance(record.get('no2_tropospheric_column'), (int, float)):
        errors.append(f"Invalid no2_tropospheric_column type")
    
    if record.get('observation_datetime') is None:
        errors.append("Missing observation_datetime")
    
    # Optional but should be valid if present
    if record.get('log_no2') is not None:
        if not isinstance(record.get('log_no2'), (int, float)):
            errors.append(f"Invalid log_no2 type")
    
    if record.get('hours_old') is not None:
        if not isinstance(record.get('hours_old'), (int, float)):
            errors.append(f"Invalid hours_old type")
        elif record.get('hours_old') < 0:
            errors.append(f"Invalid hours_old: must be non-negative")
    
    return errors

def validate_merra2_record(record, metadata):
    """Validate a MERRA-2 data record"""
    errors = []
    
    # Check required fields
    if 'latitude' not in record or record['latitude'] is None:
        errors.append("Missing latitude")
    elif not (-90 <= record['latitude'] <= 90):
        errors.append(f"Invalid latitude: {record['latitude']}")
    
    if 'longitude' not in record or record['longitude'] is None:
        errors.append("Missing longitude")
    elif not (-180 <= record['longitude'] <= 180):
        errors.append(f"Invalid longitude: {record['longitude']}")
    
    if 'variable' not in record or not record['variable']:
        errors.append("Missing variable")
    
    if 'value' not in record or record['value'] is None:
        errors.append("Missing value")
    
    if 'timestamp' not in record or not record['timestamp']:
        errors.append("Missing timestamp")
    
    return errors

def validate_air_quality_record(record, metadata):
    """Validate air quality record data before insertion"""
    errors = []
    
    if 'latitude' not in record or record['latitude'] is None:
        errors.append("Missing latitude")
    elif not (-90 <= record['latitude'] <= 90):
        errors.append(f"Invalid latitude: {record['latitude']}")
    
    if 'longitude' not in record or record['longitude'] is None:
        errors.append("Missing longitude")
    elif not (-180 <= record['longitude'] <= 180):
        errors.append(f"Invalid longitude: {record['longitude']}")
    
    if 'parameter_name' not in record or not record['parameter_name']:
        errors.append("Missing parameter_name")
    
    if 'value' not in record or record['value'] is None:
        errors.append("Missing value")
    
    if 'datetime_utc' not in record or not record['datetime_utc']:
        errors.append("Missing datetime_utc")
    
    return errors

def validate_pandora_hcho_record(record, metadata):
    """Validate Pandora HCHO record data before insertion"""
    errors = []
    
    if 'latitude' not in record or record['latitude'] is None:
        errors.append("Missing latitude")
    elif not (-90 <= record['latitude'] <= 90):
        errors.append(f"Invalid latitude: {record['latitude']}")
    
    if 'longitude' not in record or record['longitude'] is None:
        errors.append("Missing longitude")
    elif not (-180 <= record['longitude'] <= 180):
        errors.append(f"Invalid longitude: {record['longitude']}")
    
    if 'utc_datetime' not in record or not record['utc_datetime']:
        errors.append("Missing utc_datetime")
    
    if 'site' not in record or not record['site']:
        errors.append("Missing site")
    
    if 'hcho_vertical_column' not in record or record['hcho_vertical_column'] is None:
        errors.append("Missing hcho_vertical_column")
    
    return errors

def validate_pblh_record(record, metadata):
    """Validate PBLH record data before insertion"""
    errors = []
    
    if 'latitude' not in record or record['latitude'] is None:
        errors.append("Missing latitude")
    elif not (-90 <= record['latitude'] <= 90):
        errors.append(f"Invalid latitude: {record['latitude']}")
    
    if 'longitude' not in record or record['longitude'] is None:
        errors.append("Missing longitude")
    elif not (-180 <= record['longitude'] <= 180):
        errors.append(f"Invalid longitude: {record['longitude']}")
    
    if 'timestamp' not in record or not record['timestamp']:
        errors.append("Missing timestamp")
    
    if 'pbl_height_m' not in record or record['pbl_height_m'] is None:
        errors.append("Missing pbl_height_m")
    elif not isinstance(record['pbl_height_m'], (int, float)):
        errors.append(f"Invalid pbl_height_m type")
    
    return errors

def validate_no2_pipeline_record(record, metadata):
    """Validate NO2 pipeline record data before insertion"""
    errors = []
    
    if 'latitude' not in record or record['latitude'] is None:
        errors.append("Missing latitude")
    elif not (-90 <= record['latitude'] <= 90):
        errors.append(f"Invalid latitude: {record['latitude']}")
    
    if 'longitude' not in record or record['longitude'] is None:
        errors.append("Missing longitude")
    elif not (-180 <= record['longitude'] <= 180):
        errors.append(f"Invalid longitude: {record['longitude']}")
    
    if 'observation_datetime_utc' not in record or not record['observation_datetime_utc']:
        errors.append("Missing observation_datetime_utc")
    
    if 'no2_tropospheric_column' not in record or record['no2_tropospheric_column'] is None:
        errors.append("Missing no2_tropospheric_column")
    elif not isinstance(record['no2_tropospheric_column'], (int, float)):
        errors.append(f"Invalid no2_tropospheric_column type")
    
    return errors

def validate_tolnet_record(record, metadata):
    """Validate TOLNet record data before insertion"""
    errors = []
    
    if 'granule_id' not in record or not record['granule_id']:
        errors.append("Missing granule_id")
    
    if 'start_date' not in record or not record['start_date']:
        errors.append("Missing start_date")
    
    if 'end_date' not in record or not record['end_date']:
        errors.append("Missing end_date")
    
    if 'collection' not in record or not record['collection']:
        errors.append("Missing collection")
    
    return errors

def validate_aerosol_record(record, metadata):
    """Validate NASA Aerosol record data before insertion"""
    errors = []
    
    if 'satellite' not in record or not record['satellite']:
        errors.append("Missing satellite")
    
    if 'time_start' not in record or not record['time_start']:
        errors.append("Missing time_start")
    
    if 'title' not in record or not record['title']:
        errors.append("Missing title")
    
    # Optional validation for satellite type
    if 'satellite' in record and record['satellite']:
        valid_satellites = ['MODIS_Terra', 'MODIS_Aqua']
        if record['satellite'] not in valid_satellites:
            errors.append(f"Invalid satellite: {record['satellite']}")
    
    return errors

def validate_goes_record(record, metadata):
    """Validate GOES satellite record data before insertion"""
    errors = []
    
    if 'satellite' not in record or not record['satellite']:
        errors.append("Missing satellite")
    
    if 'filename' not in record or not record['filename']:
        errors.append("Missing filename")
    
    if 'timestamp' not in record or not record['timestamp']:
        errors.append("Missing timestamp")
    
    if 'product' not in record or not record['product']:
        errors.append("Missing product")
    
    # Optional validation for satellite type
    if 'satellite' in record and record['satellite']:
        valid_satellites = ['GOES16', 'GOES18']
        if record['satellite'] not in valid_satellites:
            errors.append(f"Invalid satellite: {record['satellite']}")
    
    return errors


def validate_goes_processed_record(record, metadata):
    """Validate GOES processed data record"""
    errors = []
    
    if 'variable' not in record or not record['variable']:
        errors.append("Missing variable")
    
    if 'value' not in record or record['value'] is None:
        errors.append("Missing value")
    
    if 'type' not in record or not record['type']:
        errors.append("Missing type")
    
    return errors

def validate_goes_imagery_record(record, metadata):
    """Validate GOES imagery data record"""
    errors = []
    
    if 'variable' not in record or not record['variable']:
        errors.append("Missing variable")
    
    if 'row' not in record or record['row'] is None:
        errors.append("Missing row")
    
    if 'col' not in record or record['col'] is None:
        errors.append("Missing col")
    
    if 'value' not in record or record['value'] is None:
        errors.append("Missing value")
    
    return errors


def validate_cygnss_record(record, metadata):
    """Validate CYGNSS wind data record"""
    errors = []
    
    if 'latitude' not in record or record['latitude'] is None:
        errors.append("Missing latitude")
    elif not (-90 <= record['latitude'] <= 90):
        errors.append(f"Invalid latitude: {record['latitude']}")
    
    if 'longitude' not in record or record['longitude'] is None:
        errors.append("Missing longitude")
    elif not (-180 <= record['longitude'] <= 180):
        errors.append(f"Invalid longitude: {record['longitude']}")
    
    if 'wind_speed_ms' not in record or record['wind_speed_ms'] is None:
        errors.append("Missing wind_speed_ms")
    elif not isinstance(record['wind_speed_ms'], (int, float)):
        errors.append("Invalid wind_speed_ms type")
    
    if 'hours_ago' not in record or record['hours_ago'] is None:
        errors.append("Missing hours_ago")
    
    if 'timestamp' not in record or not record['timestamp']:
        errors.append("Missing timestamp")
    
    return errors

def validate_cygnss_temporal_record(record, metadata):
    """Validate CYGNSS temporal analysis record"""
    errors = []
    
    if 'hours_ago' not in record or record['hours_ago'] is None:
        errors.append("Missing hours_ago")
    
    if 'timestamp' not in record or not record['timestamp']:
        errors.append("Missing timestamp")
    
    if 'point_count' not in record or record['point_count'] is None:
        errors.append("Missing point_count")
    
    if 'mean_wind' not in record or record['mean_wind'] is None:
        errors.append("Missing mean_wind")
    
    return errors


def validate_tempo_o3_record(record, metadata):
    """Validate TEMPO O3 record"""
    errors = []
    
    if not isinstance(record, dict):
        errors.append("Record is not a dictionary")
        return errors
    
    # Check if we have either URL or raw data
    if 'data_url' not in record and 'raw_data' not in record:
        errors.append("Missing data_url and raw_data")
    
    return errors

def validate_o3_waqi_record(record, metadata):
    """Validate O3 WAQI record"""
    errors = []
    
    if 'city' not in record or not record['city']:
        errors.append("Missing city")
    
    if 'latitude' not in record or record['latitude'] is None:
        errors.append("Missing latitude")
    elif not (-90 <= record['latitude'] <= 90):
        errors.append(f"Invalid latitude: {record['latitude']}")
    
    if 'longitude' not in record or record['longitude'] is None:
        errors.append("Missing longitude")
    elif not (-180 <= record['longitude'] <= 180):
        errors.append(f"Invalid longitude: {record['longitude']}")
    
    if 'o3_aqi' not in record or record['o3_aqi'] is None:
        errors.append("Missing o3_aqi")
    
    if 'datetime_utc' not in record or not record['datetime_utc']:
        errors.append("Missing datetime_utc")
    
    return errors

def validate_fire_record(record, metadata):
    """Validate fire detection record"""
    errors = []
    
    if 'latitude' not in record or record['latitude'] is None:
        errors.append("Missing latitude")
    elif not (-90 <= record['latitude'] <= 90):
        errors.append(f"Invalid latitude: {record['latitude']}")
    
    if 'longitude' not in record or record['longitude'] is None:
        errors.append("Missing longitude")
    elif not (-180 <= record['longitude'] <= 180):
        errors.append(f"Invalid longitude: {record['longitude']}")
    
    if 'acq_date' not in record or not record['acq_date']:
        errors.append("Missing acq_date")
    
    if 'bright_ti4' not in record or record['bright_ti4'] is None:
        errors.append("Missing bright_ti4")
    
    return errors

def validate_weather_grid_record(record, metadata):
    """Validate enhanced weather grid record"""
    errors = []
    
    if 'latitude' not in record or record['latitude'] is None:
        errors.append("Missing latitude")
    elif not (-90 <= record['latitude'] <= 90):
        errors.append(f"Invalid latitude: {record['latitude']}")
    
    if 'longitude' not in record or record['longitude'] is None:
        errors.append("Missing longitude")
    elif not (-180 <= record['longitude'] <= 180):
        errors.append(f"Invalid longitude: {record['longitude']}")
    
    if 'timestamp' not in record or not record['timestamp']:
        errors.append("Missing timestamp")
    
    if 'temperature_c' not in record or record['temperature_c'] is None:
        errors.append("Missing temperature_c")
    
    return errors



def validate_aq_station_forecast_record(record_data, forecast_data):
    """Validate air quality station forecast record"""
    errors = []
    
    station_idx = record_data.get('station_idx')
    if station_idx is None or station_idx == '':
        errors.append("Missing station_idx")
    
    lat = record_data.get('latitude')
    if lat is None:
        errors.append("Missing latitude")
    else:
        try:
            lat = float(lat)
            if not (-90 <= lat <= 90):
                errors.append(f"Invalid latitude: {lat}")
        except (ValueError, TypeError):
            errors.append(f"Invalid latitude format: {lat}")
    
    lon = record_data.get('longitude')
    if lon is None:
        errors.append("Missing longitude")
    else:
        try:
            lon = float(lon)
            if not (-180 <= lon <= 180):
                errors.append(f"Invalid longitude: {lon}")
        except (ValueError, TypeError):
            errors.append(f"Invalid longitude format: {lon}")
    
    obs_time = record_data.get('observation_time')
    if not obs_time:
        errors.append("Missing observation_time")
    elif not isinstance(obs_time, (datetime, date)):
        errors.append(f"observation_time must be datetime object, got {type(obs_time)}")
    
    forecast_date = record_data.get('forecast_date')
    if not forecast_date:
        errors.append("Missing forecast_date")
    elif not isinstance(forecast_date, date):
        errors.append(f"forecast_date must be date object, got {type(forecast_date)}")
    
    if errors:
        print(f"  Validation errors for station {station_idx}: {errors}")
    
    return errors


def validate_waqi_city_forecast_record(record, metadata):
    """Validate WAQI city forecast record data before insertion"""
    errors = []
    
    station_idx = record.get('station_idx')
    if station_idx is None or station_idx == '':
        errors.append("Missing station_idx")
    
    lat = record.get('latitude')
    if lat is None:
        errors.append("Missing latitude")
    else:
        try:
            lat = float(lat)
            if not (-90 <= lat <= 90):
                errors.append(f"Invalid latitude: {lat}")
        except (ValueError, TypeError):
            errors.append(f"Invalid latitude format: {lat}")
    
    lon = record.get('longitude')
    if lon is None:
        errors.append("Missing longitude")
    else:
        try:
            lon = float(lon)
            if not (-180 <= lon <= 180):
                errors.append(f"Invalid longitude: {lon}")
        except (ValueError, TypeError):
            errors.append(f"Invalid longitude format: {lon}")
    
    obs_time = record.get('observation_time')
    if not obs_time:
        errors.append("Missing observation_time")
    elif not isinstance(obs_time, (datetime, date)):
        errors.append(f"observation_time must be datetime object, got {type(obs_time)}")
    
    if errors:
        print(f"  Validation errors for city {station_idx}: {errors}")
    
    return errors

def validate_waqi_record(record):
    """
    Validate a single WAQI forecast record
    Allows NaN/None for optional pollutant fields
    
    Args:
        record: Dictionary containing forecast data
        
    Returns:
        tuple: (is_valid, error_message)
    """
    errors = []
    
    # Required fields
    required_fields = ['station_id', 'latitude', 'longitude', 'timestamp']
    for field in required_fields:
        if field not in record:
            errors.append(f"Missing required field: {field}")
        elif record[field] is None or (isinstance(record[field], float) and pd.isna(record[field])):
            errors.append(f"Required field is null: {field}")
    
    # Validate station_id
    if 'station_id' in record and record['station_id'] is not None:
        try:
            station_id = int(record['station_id'])
            if station_id <= 0:
                errors.append(f"Invalid station_id: {station_id}")
        except (ValueError, TypeError):
            errors.append(f"station_id must be an integer: {record.get('station_id')}")
    
    # Validate latitude
    if 'latitude' in record and record['latitude'] is not None:
        try:
            lat = float(record['latitude'])
            if not -90 <= lat <= 90:
                errors.append(f"Latitude out of range: {lat}")
        except (ValueError, TypeError):
            errors.append(f"Invalid latitude: {record.get('latitude')}")
    
    # Validate longitude
    if 'longitude' in record and record['longitude'] is not None:
        try:
            lon = float(record['longitude'])
            if not -180 <= lon <= 180:
                errors.append(f"Longitude out of range: {lon}")
        except (ValueError, TypeError):
            errors.append(f"Invalid longitude: {record.get('longitude')}")
    
    # Validate AQI values (should be 0-500) - ALLOW NaN/None/"-"
    aqi_fields = ['aqi', 'pm25_aqi', 'pm10_aqi', 'o3_aqi', 'no2_aqi', 'so2_aqi', 'co_aqi', 'nox_aqi']
    
    has_any_pollutant = False
    
    for field in aqi_fields:
        if field in record:
            value = record[field]
            
            # Skip if value is None (already cleaned)
            if value is None:
                continue
            
            # Skip if it's a float NaN
            if isinstance(value, float) and pd.isna(value):
                continue
            
            # If value exists and is valid, mark that we have data
            has_any_pollutant = True
            
            try:
                aqi_val = float(value)
                
                # Check if it's NaN after conversion
                if pd.isna(aqi_val):
                    continue
                
                # Validate range
                if not 0 <= aqi_val <= 500:
                    errors.append(f"{field} out of valid range (0-500): {aqi_val}")
            except (ValueError, TypeError):
                # Value exists but can't be converted - this is an error
                errors.append(f"Invalid {field}: {value}")
    
    # Check that at least ONE pollutant value exists
    if not has_any_pollutant:
        errors.append("No valid pollutant data available (all values are null/NaN)")
    
    # Validate date format
    if 'timestamp' in record and record['timestamp'] is not None:
        try:
            if isinstance(record['timestamp'], str):
                # Try multiple date formats
                try:
                    datetime.strptime(record['timestamp'], '%Y-%m-%d')
                except ValueError:
                    # Try ISO format
                    datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
        except ValueError:
            errors.append(f"Invalid date format for timestamp: {record.get('timestamp')}")
    
    if errors:
        return False, "; ".join(errors)
    
    return True, None

def clean_waqi_record(record):
    """
    Clean a WAQI record by converting NaN strings, dashes, and floats to None
    
    Args:
        record: Dictionary containing WAQI data
        
    Returns:
        dict: Cleaned record
    """
    cleaned = {}
    
    for key, value in record.items():
        # Convert various invalid representations to None
        if value is None:
            cleaned[key] = None
        elif isinstance(value, float) and pd.isna(value):
            cleaned[key] = None
        elif isinstance(value, str):
            # Handle string representations of missing data
            stripped = value.strip()
            if stripped.lower() in ['nan', 'null', '', '-', 'n/a', 'na']:
                cleaned[key] = None
            else:
                cleaned[key] = value
        else:
            cleaned[key] = value
    
    return cleaned


   
def insert_tempo_o3_records(conn, data11):
    print("Inserting TEMPO O3 data into the database...")
    try:
        if data11 is None or not isinstance(data11, dict):
            print("No TEMPO O3 data to insert")
            return
        
        cursor = conn.cursor()
        summary = data11.get('summary', {})
        collection_timestamp = summary.get('timestamp', '')
        
        records = data11.get('records', [])
        
        if not records:
            print("No TEMPO O3 records to insert")
            return
        
        print(f"Found {len(records)} TEMPO O3 records to process")
        
        insert_query = """
        INSERT INTO tempo_o3_data 
        (short_name, version, time_start, time_end, size_mb, data_url, raw_data, collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (data_url, time_start) 
        DO NOTHING
        """
        
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            validation_errors = validate_tempo_o3_record(record, summary)
            
            if validation_errors:
                invalid_count += 1
                if i < 5:
                    print(f"Skipping invalid record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            # Parse timestamps
            time_start = record.get('time_start')
            if time_start and isinstance(time_start, str):
                try:
                    time_start = datetime.fromisoformat(time_start.replace('Z', '+00:00'))
                except ValueError:
                    time_start = None
            
            time_end = record.get('time_end')
            if time_end and isinstance(time_end, str):
                try:
                    time_end = datetime.fromisoformat(time_end.replace('Z', '+00:00'))
                except ValueError:
                    time_end = None
            
            insert_data.append((
                record.get('short_name'),
                record.get('version'),
                time_start,
                time_end,
                record.get('size_mb', 0.0),
                record.get('data_url'),
                record.get('raw_data', ''),
                collection_timestamp
            ))
        
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            print(f"TEMPO O3 data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {initial_count - inserted_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
        else:
            print("No valid TEMPO O3 records to insert")
        
        cursor.close()
        
    except Exception as e:
        print(f"Error inserting TEMPO O3 data: {e}")
        conn.rollback()
        raise

def insert_records(conn, data):
    print("Inserting HCHO data into the database...")
    try:
        cursor = conn.cursor()
        
        # Extract metadata
        metadata = data.get('metadata', {})
        source_file = metadata.get('source_file', 'unknown')
        export_date = metadata.get('export_date', None)
        
        # Extract data records
        records = data.get('data', [])
        
        if not records:
            print("No HCHO data records to insert")
            return
        
        # Insert query with ON CONFLICT DO NOTHING
        insert_query = """
        INSERT INTO tempo_hcho_data 
        (source_file, export_date, latitude, longitude, hcho_total_column, 
         hcho_units, hcho_uncertainty, quality_flag)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (latitude, longitude, hcho_total_column, export_date) DO NOTHING
        """
        
        # Validate and prepare data for batch insert
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            # Validate record
            validation_errors = validate_hcho_record(record, metadata)
            
            if validation_errors:
                invalid_count += 1
                print(f"Skipping invalid HCHO record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            insert_data.append((
                source_file,
                export_date,
                record.get('latitude'),
                record.get('longitude'),
                record.get('hcho_total_column'),
                record.get('hcho_units'),
                record.get('hcho_uncertainty'),
                record.get('quality_flag')
            ))
        
        # Execute batch insert
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            duplicate_count = initial_count - inserted_count
            print(f"HCHO data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {duplicate_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
        else:
            print("No valid HCHO records to insert")
        
        cursor.close()
        
    except psycopg2.Error as e:
        print(f"Error inserting HCHO data into the database: {e}")
        conn.rollback()
        raise

def insert_no2_records(conn, data1):
    print("Inserting NO2 data into the database...")
    try:
        cursor = conn.cursor()
        
        # Check if data collection was successful
        if not data1 or data1.get('status') != 'success':
            print("No NO2 data available or collection failed")
            return
        
        # Extract data records
        records = data1.get('data', [])
        
        if not records:
            print("No NO2 data records to insert")
            return
        
        # Insert query with ON CONFLICT DO NOTHING
        insert_query = """
        INSERT INTO tempo_no2_data 
        (source_file, observation_datetime, latitude, longitude, 
         no2_tropospheric_column, log_no2, hours_old)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (latitude, longitude, no2_tropospheric_column, observation_datetime) DO NOTHING
        """
        
        # Validate and prepare data for batch insert
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            # Validate record
            validation_errors = validate_no2_record(record)
            
            if validation_errors:
                invalid_count += 1
                print(f"Skipping invalid NO2 record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            insert_data.append((
                record.get('file_name', 'unknown'),
                record.get('observation_datetime'),
                record.get('latitude'),
                record.get('longitude'),
                record.get('no2_tropospheric_column'),
                record.get('log_no2'),
                record.get('hours_old')
            ))
        
        # Execute batch insert
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            duplicate_count = initial_count - inserted_count
            print(f"NO2 data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {duplicate_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
        else:
            print("No valid NO2 records to insert")
        
        cursor.close()
        
    except psycopg2.Error as e:
        print(f"Error inserting NO2 data into the database: {e}")
        conn.rollback()
        raise

def insert_merra2_records(conn, data2):
    print("Inserting MERRA-2 data into the database...")
    try:
        cursor = conn.cursor()
        
        # Extract summary/metadata
        summary = data2.get('summary', {})
        source_file = summary.get('source_file', 'MERRA-2')
        collection = summary.get('collection', '')
        short_name = summary.get('short_name', '')
        version = summary.get('version', '')
        collection_timestamp = summary.get('collection_timestamp', '')
        
        # Parse granule times from summary
        granule_time_start = summary.get('granule_time_start')
        granule_time_end = summary.get('granule_time_end')
        
        # Convert ISO timestamps to datetime if needed
        if granule_time_start and isinstance(granule_time_start, str):
            granule_time_start = datetime.fromisoformat(granule_time_start.replace('Z', '+00:00'))
        if granule_time_end and isinstance(granule_time_end, str):
            granule_time_end = datetime.fromisoformat(granule_time_end.replace('Z', '+00:00'))
        
        # Extract data records - try multiple possible keys
        records = data2.get('records', data2.get('data', data2.get('records_preview', [])))
        
        if not records:
            print("No MERRA-2 data records to insert")
            print(f"Available keys in data2: {list(data2.keys())}")
            return
        
        print(f"Found {len(records)} MERRA-2 records to process")
        
        # Insert query with ON CONFLICT DO NOTHING
        insert_query = """
        INSERT INTO merra2_slv_data 
        (source_file, collection, short_name, version, export_date, 
         granule_time_start, granule_time_end, latitude, longitude, 
         variable_name, variable_value, variable_units, quality_flag, 
         collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (latitude, longitude, variable_name, granule_time_start, variable_value) 
        DO NOTHING
        """
        
        # Validate and prepare data for batch insert
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            # Validate record
            validation_errors = validate_merra2_record(record, summary)
            
            if validation_errors:
                invalid_count += 1
                if i < 5:  # Only print first 5 errors to avoid spam
                    print(f"Skipping invalid MERRA-2 record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            # Parse the timestamp from the record
            record_timestamp = record.get('timestamp')
            if record_timestamp and isinstance(record_timestamp, str):
                record_timestamp = datetime.fromisoformat(record_timestamp.replace('Z', ''))
            
            # Determine units based on variable name
            variable_name = record.get('variable')
            variable_units = ''
            if variable_name == 'T2M':
                variable_units = 'K'
            elif variable_name == 'QV2M':
                variable_units = 'kg/kg'
            elif variable_name in ['U10M', 'V10M']:
                variable_units = 'm/s'
            elif variable_name in ['PS', 'SLP']:
                variable_units = 'Pa'
            
            insert_data.append((
                source_file,
                collection,
                short_name,
                version,
                record_timestamp,  # export_date = timestamp from record
                granule_time_start,
                granule_time_end,
                record.get('latitude'),
                record.get('longitude'),
                variable_name,  # variable instead of variable_name
                record.get('value'),  # value instead of variable_value
                variable_units,
                None,  # quality_flag not in this structure
                collection_timestamp
            ))
        
        # Execute batch insert
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            duplicate_count = initial_count - inserted_count
            print(f"MERRA-2 data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {duplicate_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
            print(f"  - Total records processed: {len(records)}")
        else:
            print("No valid MERRA-2 records to insert")
        
        cursor.close()
        
    except psycopg2.Error as e:
        print(f"Error inserting MERRA-2 data into the database: {e}")
        conn.rollback()
        raise
    except Exception as e:
        print(f"Unexpected error in insert_merra2_records: {e}")
        conn.rollback()
        raise

def insert_air_quality_records(conn, data3):
    print("Inserting Air Quality data into the database...")
    try:
        cursor = conn.cursor()
        
        # Extract summary/metadata
        summary = data3.get('summary', {})
        collection_timestamp = summary.get('collection_timestamp', '')
        
        # Extract data records
        records = data3.get('records', data3.get('data', data3.get('records_preview', [])))
        
        if not records:
            print("No Air Quality data records to insert")
            return
        
        print(f"Found {len(records)} Air Quality records to process")
        
        # Insert query with ON CONFLICT DO NOTHING
        insert_query = """
        INSERT INTO air_quality_data 
        (datetime_utc, datetime_local, value, latitude, longitude, 
         location_id, location_name, city, state, country, 
         parameter_name, parameter_display_name, units, sensor_id, 
         provider, data_source, collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (datetime_utc, latitude, longitude, parameter_name, value, sensor_id) 
        DO NOTHING
        """
        
        # Validate and prepare data for batch insert
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            # Validate record
            validation_errors = validate_air_quality_record(record, summary)
            
            if validation_errors:
                invalid_count += 1
                if i < 5:
                    print(f"Skipping invalid Air Quality record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            # Parse timestamps
            datetime_utc = record.get('datetime_utc')
            if datetime_utc and isinstance(datetime_utc, str):
                datetime_utc = datetime.fromisoformat(datetime_utc.replace('Z', '+00:00'))
            
            datetime_local = record.get('datetime_local')
            if datetime_local and isinstance(datetime_local, str) and datetime_local:
                datetime_local = datetime.fromisoformat(datetime_local.replace('Z', '+00:00'))
            else:
                datetime_local = None
            
            insert_data.append((
                datetime_utc,
                datetime_local,
                record.get('value'),
                record.get('latitude'),
                record.get('longitude'),
                record.get('location_id'),
                record.get('location_name'),
                record.get('city'),
                record.get('state'),
                record.get('country'),
                record.get('parameter_name'),
                record.get('parameter_display_name'),
                record.get('units'),
                record.get('sensor_id'),
                record.get('provider'),
                record.get('data_source'),
                collection_timestamp
            ))
        
        # Execute batch insert
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            duplicate_count = initial_count - inserted_count
            print(f"Air Quality data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {duplicate_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
            print(f"  - Total records processed: {len(records)}")
        else:
            print("No valid Air Quality records to insert")
        
        cursor.close()
        
    except psycopg2.Error as e:
        print(f"Error inserting Air Quality data into the database: {e}")
        conn.rollback()
        raise
    except Exception as e:
        print(f"Unexpected error in insert_air_quality_records: {e}")
        conn.rollback()
        raise

def insert_pandora_hcho_records(conn, data4):
    print("Inserting Pandora HCHO data into the database...")
    try:
        cursor = conn.cursor()
        
        # Extract summary/metadata
        summary = data4.get('summary', {})
        collection_timestamp = summary.get('collection_timestamp', '')
        
        # Extract data records
        records = data4.get('records', data4.get('data', data4.get('records_preview', [])))
        
        if not records:
            print("No Pandora HCHO data records to insert")
            return
        
        print(f"Found {len(records)} Pandora HCHO records to process")
        
        # Insert query with ON CONFLICT DO NOTHING
        insert_query = """
        INSERT INTO pandora_hcho_data 
        (utc_datetime, fractional_day, solar_zenith_angle, solar_azimuth_angle,
         elevation_angle, azimuth_angle, hcho_slant_column, hcho_slant_column_error,
         hcho_vertical_column, hcho_vertical_column_error, air_mass_factor,
         fitting_rms, temperature, quality_flag, processing_code,
         site, location, latitude, longitude, instrument, data_level,
         source_file, collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (utc_datetime, latitude, longitude, hcho_vertical_column, site, instrument) 
        DO NOTHING
        """
        
        # Validate and prepare data for batch insert
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            # Validate record
            validation_errors = validate_pandora_hcho_record(record, summary)
            
            if validation_errors:
                invalid_count += 1
                if i < 5:
                    print(f"Skipping invalid Pandora HCHO record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            # Parse UTC datetime - format: "20210915T163633.3Z"
            utc_datetime_str = record.get('utc_datetime')
            utc_datetime = None
            if utc_datetime_str:
                try:
                    # Remove 'Z' and parse
                    utc_datetime = datetime.strptime(utc_datetime_str.replace('Z', ''), '%Y%m%dT%H%M%S.%f')
                except ValueError:
                    try:
                        # Try without microseconds
                        utc_datetime = datetime.strptime(utc_datetime_str.replace('Z', ''), '%Y%m%dT%H%M%S')
                    except ValueError:
                        invalid_count += 1
                        continue
            
            insert_data.append((
                utc_datetime,
                record.get('fractional_day'),
                record.get('solar_zenith_angle'),
                record.get('solar_azimuth_angle'),
                record.get('elevation_angle'),
                record.get('azimuth_angle'),
                record.get('hcho_slant_column'),
                record.get('hcho_slant_column_error'),
                record.get('hcho_vertical_column'),
                record.get('hcho_vertical_column_error'),
                record.get('air_mass_factor'),
                record.get('fitting_rms'),
                record.get('temperature'),
                record.get('quality_flag'),
                record.get('processing_code'),
                record.get('site'),
                record.get('location'),
                record.get('latitude'),
                record.get('longitude'),
                record.get('instrument'),
                record.get('data_level'),
                record.get('source_file'),
                collection_timestamp
            ))
        
        # Execute batch insert
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            duplicate_count = initial_count - inserted_count
            print(f"Pandora HCHO data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {duplicate_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
            print(f"  - Total records processed: {len(records)}")
        else:
            print("No valid Pandora HCHO records to insert")
        
        cursor.close()
        
    except psycopg2.Error as e:
        print(f"Error inserting Pandora HCHO data into the database: {e}")
        conn.rollback()
        raise
    except Exception as e:
        print(f"Unexpected error in insert_pandora_hcho_records: {e}")
        conn.rollback()
        raise

def insert_pblh_records(conn, data5):
    print("Inserting PBLH data into the database...")
    try:
        cursor = conn.cursor()
        
        # Extract summary/metadata
        summary = data5.get('summary', {})
        collection = summary.get('collection', 'M2T1NXFLX')
        collection_version = summary.get('collection_version', '')
        collection_timestamp = summary.get('collection_timestamp', '')
        data_source = summary.get('data_source', 'MERRA-2')
        variable = summary.get('variable', 'PBLH')
        
        # Extract data records
        records = data5.get('records', data5.get('data', data5.get('records_preview', [])))
        
        if not records:
            print("No PBLH data records to insert")
            return
        
        print(f"Found {len(records)} PBLH records to process")
        
        # Insert query with ON CONFLICT DO NOTHING
        insert_query = """
        INSERT INTO pblh_data 
        (latitude, longitude, timestamp, pbl_height_m, data_source, 
         variable, unit, collection, collection_version, collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (latitude, longitude, timestamp, pbl_height_m) 
        DO NOTHING
        """
        
        # Validate and prepare data for batch insert
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            # Validate record
            validation_errors = validate_pblh_record(record, summary)
            
            if validation_errors:
                invalid_count += 1
                if i < 5:
                    print(f"Skipping invalid PBLH record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            # Parse timestamp
            timestamp = record.get('timestamp')
            if timestamp and isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp.replace('Z', ''))
            
            insert_data.append((
                record.get('latitude'),
                record.get('longitude'),
                timestamp,
                record.get('pbl_height_m'),
                record.get('data_source', data_source),
                record.get('variable', variable),
                record.get('unit', 'meters'),
                collection,
                collection_version,
                collection_timestamp
            ))
        
        # Execute batch insert
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            duplicate_count = initial_count - inserted_count
            print(f"PBLH data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {duplicate_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
            print(f"  - Total records processed: {len(records)}")
        else:
            print("No valid PBLH records to insert")
        
        cursor.close()
        
    except psycopg2.Error as e:
        print(f"Error inserting PBLH data into the database: {e}")
        conn.rollback()
        raise
    except Exception as e:
        print(f"Unexpected error in insert_pblh_records: {e}")
        conn.rollback()
        raise

def insert_no2_pipeline_records(conn, data6):
    print("Inserting NO2 Pipeline data into the database...")
    
    # Check if data6 is None
    if data6 is None:
        print("ERROR: NO2 Pipeline returned None - no data available")
        print("Possible causes:")
        print("  - Authentication failed")
        print("  - No files found for the specified time range")
        print("  - Download failed")
        print("  - No valid data could be extracted from files")
        return
    
    # Check if data6 is a dict
    if not isinstance(data6, dict):
        print(f"ERROR: NO2 Pipeline returned unexpected type: {type(data6)}")
        return
    
    try:
        cursor = conn.cursor()
        
        # Extract summary/metadata
        summary = data6.get('summary', {})
        collection_timestamp = summary.get('collection_timestamp', '')
        
        # Extract data records - try multiple possible keys
        records = data6.get('records', data6.get('data', data6.get('records_preview', [])))
        
        if not records:
            print("No NO2 Pipeline data records to insert")
            print(f"Data structure received: {list(data6.keys())}")
            return
        
        print(f"Found {len(records)} NO2 Pipeline records to process")
        
        # Insert query with ON CONFLICT DO NOTHING
        insert_query = """
        INSERT INTO no2_pipeline_data 
        (no2_tropospheric_column, longitude, latitude, observation_datetime_utc, 
         file_name, data_source, collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (latitude, longitude, no2_tropospheric_column, observation_datetime_utc, file_name) 
        DO NOTHING
        """
        
        # Validate and prepare data for batch insert
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            if not record:  # Skip None or empty dict
                invalid_count += 1
                if i < 5:
                    print(f"Skipping invalid NO2 Pipeline record {i+1}: record is None or empty")
                continue

            # Validate record
            validation_errors = validate_no2_pipeline_record(record, summary)
            
            if validation_errors:
                invalid_count += 1
                if i < 5:
                    print(f"Skipping invalid NO2 Pipeline record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            # Parse observation datetime
            observation_datetime = record.get('observation_datetime_utc')
            if observation_datetime and isinstance(observation_datetime, str):
                try:
                    observation_datetime = datetime.fromisoformat(observation_datetime.replace('Z', '+00:00'))
                except (ValueError, AttributeError) as e:
                    if i < 5:
                        print(f"Skipping record {i+1}: Invalid datetime format: {e}")
                    invalid_count += 1
                    continue
            
            insert_data.append((
                record.get('no2_tropospheric_column'),
                record.get('longitude'),
                record.get('latitude'),
                observation_datetime,
                record.get('file_name'),
                record.get('data_source', 'TEMPO_NO2_L2'),
                collection_timestamp
            ))
        
        # Execute batch insert
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            duplicate_count = initial_count - inserted_count
            print(f"NO2 Pipeline data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {duplicate_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
            print(f"  - Total records processed: {len(records)}")
        else:
            print("No valid NO2 Pipeline records to insert after validation")
        
        cursor.close()
        
    except psycopg2.Error as e:
        print(f"Error inserting NO2 Pipeline data into the database: {e}")
        conn.rollback()
        raise
    except Exception as e:
        print(f"Unexpected error in insert_no2_pipeline_records: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        raise

        
def insert_tolnet_records(conn, data7):
    print("Inserting TOLNet data into the database...")
    try:
        cursor = conn.cursor()
        
        # Extract summary/metadata
        summary = data7.get('summary', {})
        collection_timestamp = summary.get('generated_at', '')
        variable = summary.get('variable', 'O3')
        
        # Extract data records
        records = data7.get('records', data7.get('data', []))
        
        if not records:
            print("No TOLNet data records to insert")
            return
        
        print(f"Found {len(records)} TOLNet records to process")
        
        # Insert query with ON CONFLICT DO NOTHING
        insert_query = """
        INSERT INTO tolnet_data 
        (granule_id, title, collection, start_date, end_date, updated, 
         download_url, file_count, variable, collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (granule_id, start_date, end_date) 
        DO NOTHING
        """
        
        # Validate and prepare data for batch insert
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            # Validate record
            validation_errors = validate_tolnet_record(record, summary)
            
            if validation_errors:
                invalid_count += 1
                if i < 5:
                    print(f"Skipping invalid TOLNet record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            # Parse timestamps
            start_date = record.get('start_date')
            if start_date and isinstance(start_date, str):
                start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            
            end_date = record.get('end_date')
            if end_date and isinstance(end_date, str):
                end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            
            updated = record.get('updated')
            if updated and isinstance(updated, str):
                updated = datetime.fromisoformat(updated.replace('Z', '+00:00'))
            
            insert_data.append((
                record.get('granule_id'),
                record.get('title'),
                record.get('collection'),
                start_date,
                end_date,
                updated,
                record.get('download_url'),
                record.get('file_count'),
                variable,
                collection_timestamp
            ))
        
        # Execute batch insert
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            duplicate_count = initial_count - inserted_count
            print(f"TOLNet data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {duplicate_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
            print(f"  - Total records processed: {len(records)}")
        else:
            print("No valid TOLNet records to insert")
        
        cursor.close()
        
    except psycopg2.Error as e:
        print(f"Error inserting TOLNet data into the database: {e}")
        conn.rollback()
        raise
    except Exception as e:
        print(f"Unexpected error in insert_tolnet_records: {e}")
        conn.rollback()
        raise

def insert_aerosol_records(conn, data8):
    print("Inserting NASA Aerosol data into the database...")
    try:
        cursor = conn.cursor()
        
        # Extract summary/metadata
        summary = data8.get('summary', {})
        collection_timestamp = summary.get('timestamp', '')
        
        # Extract data records
        records = data8.get('records', data8.get('data', []))
        
        if not records:
            print("No NASA Aerosol data records to insert")
            return
        
        print(f"Found {len(records)} NASA Aerosol records to process")
        
        # Insert query with ON CONFLICT DO NOTHING
        insert_query = """
        INSERT INTO nasa_aerosol_data 
        (satellite, time_start, title, hour, granule_count, collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (satellite, time_start, title) 
        DO NOTHING
        """
        
        # Validate and prepare data for batch insert
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            # Validate record
            validation_errors = validate_aerosol_record(record, summary)
            
            if validation_errors:
                invalid_count += 1
                if i < 5:
                    print(f"Skipping invalid Aerosol record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            # Parse timestamps
            time_start = record.get('time_start')
            if time_start and isinstance(time_start, str):
                time_start = datetime.fromisoformat(time_start.replace('Z', '+00:00'))
            
            hour = record.get('hour')
            if hour and isinstance(hour, str):
                # Handle timezone-aware string format like "2025-10-02 15:00:00+00:00"
                hour = datetime.fromisoformat(hour)
            
            insert_data.append((
                record.get('satellite'),
                time_start,
                record.get('title'),
                hour,
                record.get('granule_count'),
                collection_timestamp
            ))
        
        # Execute batch insert
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            duplicate_count = initial_count - inserted_count
            print(f"NASA Aerosol data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {duplicate_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
            print(f"  - Total records processed: {len(records)}")
        else:
            print("No valid NASA Aerosol records to insert")
        
        cursor.close()
        
    except psycopg2.Error as e:
        print(f"Error inserting NASA Aerosol data into the database: {e}")
        conn.rollback()
        raise
    except Exception as e:
        print(f"Unexpected error in insert_aerosol_records: {e}")
        conn.rollback()
        raise

def insert_goes_records(conn, data9):
    print("Inserting GOES Satellite data into the database...")
    try:
        # Check if data9 is None or empty
        if data9 is None:
            print("No GOES data returned from fetcher (data9 is None)")
            return
        
        if not isinstance(data9, dict):
            print(f"Invalid GOES data type: {type(data9)}")
            return
        
        cursor = conn.cursor()
        
        # Extract summary/metadata
        summary = data9.get('summary', {})
        collection_timestamp = summary.get('timestamp', '')
        
        # Extract data records
        records = data9.get('records', data9.get('data', []))
        
        if not records:
            print("No GOES data records to insert")
            return
        
        print(f"Found {len(records)} GOES records to process")
        
        # Insert query with ON CONFLICT DO NOTHING
        insert_query = """
        INSERT INTO goes_satellite_data 
        (satellite, product, filename, size_mb, timestamp, bucket, key, collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (satellite, filename, timestamp) 
        DO NOTHING
        """
        
        # Validate and prepare data for batch insert
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            # Validate record
            validation_errors = validate_goes_record(record, summary)
            
            if validation_errors:
                invalid_count += 1
                if i < 5:
                    print(f"Skipping invalid GOES record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            # Parse timestamp - format: "2025-10-02 22:50:04 UTC"
            timestamp = record.get('timestamp')
            if timestamp and isinstance(timestamp, str):
                try:
                    # Remove ' UTC' and parse
                    timestamp = datetime.strptime(timestamp.replace(' UTC', ''), '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    invalid_count += 1
                    continue
            
            insert_data.append((
                record.get('satellite'),
                record.get('product'),
                record.get('filename'),
                record.get('size_mb'),
                timestamp,
                record.get('bucket'),
                record.get('key'),
                collection_timestamp
            ))
        
        # Execute batch insert
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            duplicate_count = initial_count - inserted_count
            print(f"GOES Satellite data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {duplicate_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
            print(f"  - Total records processed: {len(records)}")
        else:
            print("No valid GOES records to insert")
        
        cursor.close()
        
    except psycopg2.Error as e:
        print(f"Error inserting GOES data into the database: {e}")
        conn.rollback()
        raise
    except Exception as e:
        print(f"Unexpected error in insert_goes_records: {e}")
        conn.rollback()
        raise
def insert_goes_processed_data(conn, data9):
    """Insert GOES processed data from CSV files"""
    print("Inserting GOES processed data into the database...")
    try:
        if data9 is None or not isinstance(data9, dict):
            print("No GOES processed data to insert")
            return
        
        cursor = conn.cursor()
        summary = data9.get('summary', {})
        collection_timestamp = summary.get('timestamp', '')
        csv_files = data9.get('csv_files', {})
        
        insert_query = """
        INSERT INTO goes_processed_data 
        (satellite, variable, value, data_type, row_index, col_index, index_1d, collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (satellite, variable, row_index, col_index, index_1d, collection_timestamp) 
        DO NOTHING
        """
        
        for sat in ['GOES18', 'GOES16']:
            csv_file = csv_files.get(f'{sat}_data')
            if not csv_file or not os.path.exists(csv_file):
                continue
            
            print(f"Reading {sat} processed data from {csv_file}")
            df = pd.read_csv(csv_file)
            
            insert_data = []
            for _, row in df.iterrows():
                row_idx = row.get('row') if pd.notna(row.get('row')) else None
                col_idx = row.get('col') if pd.notna(row.get('col')) else None
                idx_1d = row.get('index') if pd.notna(row.get('index')) else None
                
                insert_data.append((
                    sat,
                    row.get('variable'),
                    row.get('value'),
                    row.get('type'),
                    row_idx,
                    col_idx,
                    idx_1d,
                    collection_timestamp
                ))
            
            if insert_data:
                initial_count = len(insert_data)
                cursor.executemany(insert_query, insert_data)
                inserted_count = cursor.rowcount
                conn.commit()
                
                print(f"{sat} processed data insertion:")
                print(f"  - New records inserted: {inserted_count}")
                print(f"  - Duplicate records skipped: {initial_count - inserted_count}")
        
        cursor.close()
        
    except Exception as e:
        print(f"Error inserting GOES processed data: {e}")
        conn.rollback()
        raise

def insert_goes_imagery_data(conn, data9):
    """Insert GOES imagery data from CSV files"""
    print("Inserting GOES imagery data into the database...")
    try:
        if data9 is None or not isinstance(data9, dict):
            print("No GOES imagery data to insert")
            return
        
        cursor = conn.cursor()
        summary = data9.get('summary', {})
        collection_timestamp = summary.get('timestamp', '')
        csv_files = data9.get('csv_files', {})
        
        insert_query = """
        INSERT INTO goes_imagery_data 
        (satellite, variable, row_index, col_index, value, collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (satellite, variable, row_index, col_index, collection_timestamp) 
        DO NOTHING
        """
        
        for sat in ['GOES18', 'GOES16']:
            csv_file = csv_files.get(f'{sat}_imagery')
            if not csv_file or not os.path.exists(csv_file):
                continue
            
            print(f"Reading {sat} imagery data from {csv_file}")
            df = pd.read_csv(csv_file)
            
            insert_data = []
            for _, row in df.iterrows():
                insert_data.append((
                    sat,
                    row.get('variable'),
                    row.get('row'),
                    row.get('col'),
                    row.get('value'),
                    collection_timestamp
                ))
            
            if insert_data:
                initial_count = len(insert_data)
                cursor.executemany(insert_query, insert_data)
                inserted_count = cursor.rowcount
                conn.commit()
                
                print(f"{sat} imagery data insertion:")
                print(f"  - New records inserted: {inserted_count}")
                print(f"  - Duplicate records skipped: {initial_count - inserted_count}")
        
        cursor.close()
        
    except Exception as e:
        print(f"Error inserting GOES imagery data: {e}")
        conn.rollback()
        raise

def insert_cygnss_records(conn, data10):
    """Insert CYGNSS wind data records"""
    print("Inserting CYGNSS wind data into the database...")
    try:
        if data10 is None or not isinstance(data10, dict):
            print("No CYGNSS data to insert")
            return
        
        cursor = conn.cursor()
        summary = data10.get('summary', {})
        collection_timestamp = summary.get('timestamp', '')
        data_mode = summary.get('mode', 'synthetic')
        
        records = data10.get('records', [])
        
        if not records:
            print("No CYGNSS wind data records to insert")
            return
        
        print(f"Found {len(records)} CYGNSS wind records to process")
        
        insert_query = """
        INSERT INTO cygnss_wind_data 
        (latitude, longitude, wind_speed_ms, hours_ago, timestamp, data_mode, collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (latitude, longitude, timestamp, hours_ago) 
        DO NOTHING
        """
        
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            validation_errors = validate_cygnss_record(record, summary)
            
            if validation_errors:
                invalid_count += 1
                if i < 5:
                    print(f"Skipping invalid CYGNSS record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            # Parse timestamp
            timestamp = record.get('timestamp')
            if timestamp and isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(str(timestamp))
                except ValueError:
                    invalid_count += 1
                    continue
            
            insert_data.append((
                record.get('latitude'),
                record.get('longitude'),
                record.get('wind_speed_ms'),
                record.get('hours_ago'),
                timestamp,
                data_mode,
                collection_timestamp
            ))
        
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            print(f"CYGNSS wind data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {initial_count - inserted_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
        else:
            print("No valid CYGNSS wind records to insert")
        
        cursor.close()
        
    except Exception as e:
        print(f"Error inserting CYGNSS wind data: {e}")
        conn.rollback()
        raise

def insert_cygnss_temporal_analysis(conn, data10):
    """Insert CYGNSS temporal analysis (hourly summaries)"""
    print("Inserting CYGNSS temporal analysis into the database...")
    try:
        if data10 is None or not isinstance(data10, dict):
            print("No CYGNSS temporal analysis to insert")
            return
        
        cursor = conn.cursor()
        summary = data10.get('summary', {})
        collection_timestamp = summary.get('timestamp', '')
        data_mode = summary.get('mode', 'synthetic')
        
        records = data10.get('temporal_analysis', [])
        
        if not records:
            print("No CYGNSS temporal analysis records to insert")
            return
        
        print(f"Found {len(records)} CYGNSS temporal analysis records to process")
        
        insert_query = """
        INSERT INTO cygnss_temporal_analysis 
        (hours_ago, timestamp, point_count, mean_wind, min_wind, max_wind, 
         std_wind, data_mode, collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (hours_ago, timestamp, collection_timestamp) 
        DO NOTHING
        """
        
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            validation_errors = validate_cygnss_temporal_record(record, summary)
            
            if validation_errors:
                invalid_count += 1
                if i < 5:
                    print(f"Skipping invalid temporal record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            # Parse timestamp
            timestamp = record.get('timestamp')
            if timestamp and isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(str(timestamp))
                except ValueError:
                    invalid_count += 1
                    continue
            
            insert_data.append((
                record.get('hours_ago'),
                timestamp,
                record.get('point_count'),
                record.get('mean_wind'),
                record.get('min_wind'),
                record.get('max_wind'),
                record.get('std_wind'),
                data_mode,
                collection_timestamp
            ))
        
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            print(f"CYGNSS temporal analysis insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {initial_count - inserted_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
        else:
            print("No valid temporal analysis records to insert")
        
        cursor.close()
        
    except Exception as e:
        print(f"Error inserting CYGNSS temporal analysis: {e}")
        conn.rollback()
        raise


def insert_o3_waqi_records(conn, data12):
    print("Inserting O3 WAQI data into the database...")
    try:
        if data12 is None or not isinstance(data12, dict):
            print("No O3 WAQI data to insert")
            return
        
        cursor = conn.cursor()
        summary = data12.get('summary', {})
        collection_timestamp = summary.get('timestamp', '')
        
        records = data12.get('records', [])
        
        if not records:
            print("No O3 WAQI records to insert")
            return
        
        print(f"Found {len(records)} O3 WAQI records to process")
        
        insert_query = """
        INSERT INTO o3_waqi_data 
        (city, station_name, latitude, longitude, datetime_utc, o3_aqi, 
         dominant_pollutant, overall_aqi, temperature, humidity, pressure, 
         station_url, collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_name, datetime_utc, latitude, longitude) 
        DO NOTHING
        """
        
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            validation_errors = validate_o3_waqi_record(record, summary)
            
            if validation_errors:
                invalid_count += 1
                if i < 5:
                    print(f"Skipping invalid record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            # Parse timestamp - format: "2025-10-03T11:00:00-06:00"
            datetime_utc = record.get('datetime_utc')
            if datetime_utc and isinstance(datetime_utc, str):
                try:
                    datetime_utc = datetime.fromisoformat(datetime_utc)
                except ValueError:
                    invalid_count += 1
                    continue
            
            insert_data.append((
                record.get('city'),
                record.get('station_name'),
                record.get('latitude'),
                record.get('longitude'),
                datetime_utc,
                record.get('o3_aqi'),
                record.get('dominant_pollutant'),
                record.get('overall_aqi'),
                record.get('temperature'),
                record.get('humidity'),
                record.get('pressure'),
                record.get('station_url'),
                collection_timestamp
            ))
        
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            print(f"O3 WAQI data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {initial_count - inserted_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
        else:
            print("No valid O3 WAQI records to insert")
        
        cursor.close()
        
    except Exception as e:
        print(f"Error inserting O3 WAQI data: {e}")
        conn.rollback()
        raise
def insert_fire_detection_records(conn, data13):
    print("Inserting Fire Detection data into the database...")
    try:
        if data13 is None or not isinstance(data13, dict):
            print("No Fire Detection data to insert")
            return
        
        cursor = conn.cursor()
        summary = data13.get('summary', {})
        collection_timestamp = summary.get('timestamp', '')
        
        records = data13.get('records', [])
        
        if not records:
            print("No Fire Detection records to insert")
            return
        
        print(f"Found {len(records)} Fire Detection records to process")
        
        insert_query = """
        INSERT INTO fire_detection_data 
        (latitude, longitude, bright_ti4, bright_ti5, scan, track, acq_date, 
         acq_time, satellite, instrument, confidence, version, frp, daynight, 
         collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (latitude, longitude, acq_date, acq_time, satellite) 
        DO NOTHING
        """
        
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            validation_errors = validate_fire_record(record, summary)
            
            if validation_errors:
                invalid_count += 1
                if i < 5:
                    print(f"Skipping invalid record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            # Parse date - format: "2025-10-03"
            acq_date = record.get('acq_date')
            if acq_date and isinstance(acq_date, str):
                try:
                    acq_date = datetime.strptime(acq_date, '%Y-%m-%d').date()
                except ValueError:
                    invalid_count += 1
                    continue
            
            insert_data.append((
                record.get('latitude'),
                record.get('longitude'),
                record.get('bright_ti4'),
                record.get('bright_ti5'),
                record.get('scan'),
                record.get('track'),
                acq_date,
                record.get('acq_time'),
                record.get('satellite'),
                record.get('instrument'),
                record.get('confidence'),
                record.get('version'),
                record.get('frp'),
                record.get('daynight'),
                collection_timestamp
            ))
        
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            print(f"Fire Detection data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {initial_count - inserted_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
        else:
            print("No valid Fire Detection records to insert")
        
        cursor.close()
        
    except Exception as e:
        print(f"Error inserting Fire Detection data: {e}")
        conn.rollback()
        raise

def insert_enhanced_weather_grid_records(conn, data14):
    print("Inserting Enhanced Weather Grid data into the database...")
    try:
        if data14 is None or not isinstance(data14, dict):
            print("No Enhanced Weather Grid data to insert")
            return
        
        cursor = conn.cursor()
        summary = data14.get('summary', {})
        collection_timestamp = summary.get('execution', {}).get('start_time', '')
        
        records = data14.get('records', [])
        
        if not records:
            print("No Enhanced Weather Grid records to insert")
            return
        
        print(f"Found {len(records)} Enhanced Weather Grid records to process")
        
        insert_query = """
        INSERT INTO enhanced_weather_grid_data 
        (timestamp, latitude, longitude, temperature_c, humidity_percent, 
         precipitation_mm, wind_speed_kmh, pressure_hpa, cloud_cover_percent, 
         collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (timestamp, latitude, longitude) 
        DO NOTHING
        """
        
        insert_data = []
        invalid_count = 0
        
        for i, record in enumerate(records):
            validation_errors = validate_weather_grid_record(record, summary)
            
            if validation_errors:
                invalid_count += 1
                if i < 5:
                    print(f"Skipping invalid record {i+1}: {'; '.join(validation_errors)}")
                continue
            
            # Parse timestamp
            timestamp = record.get('timestamp')
            if timestamp and isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except ValueError:
                    invalid_count += 1
                    continue
            
            insert_data.append((
                timestamp,
                record.get('latitude'),
                record.get('longitude'),
                record.get('temperature_c'),
                record.get('humidity_percent'),
                record.get('precipitation_mm'),
                record.get('wind_speed_kmh'),
                record.get('pressure_hpa'),
                record.get('cloud_cover_percent'),
                collection_timestamp
            ))
        
        if insert_data:
            initial_count = len(insert_data)
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            print(f"Enhanced Weather Grid data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Duplicate records skipped: {initial_count - inserted_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
        else:
            print("No valid Enhanced Weather Grid records to insert")
        
        cursor.close()
        
    except Exception as e:
        print(f"Error inserting Enhanced Weather Grid data: {e}")
        conn.rollback()
        raise

def insert_so2_co_station_records(conn, records, pollutant_type):
    """Insert flattened SO2/CO station records"""
    print(f"Inserting {pollutant_type} Station data into the database...")
    
    if not records:
        print(f"No {pollutant_type} records to insert")
        return
    
    try:
        cursor = conn.cursor()
        collection_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        print(f"Found {len(records)} {pollutant_type} records to process")
        
        insert_query = """
        INSERT INTO aq_station_forecast_data 
        (station_idx, station_uid, aqi, dominant_pollutant, city_name, city_url,
         latitude, longitude, observation_time, timezone, h, dew, t, p, w, wg,
         co, no2, o3, so2, pm10, pm25, forecast_date, 
         forecast_pm10_avg, forecast_pm10_max, forecast_pm10_min,
         forecast_pm25_avg, forecast_pm25_max, forecast_pm25_min,
         forecast_uvi_avg, forecast_uvi_max, forecast_uvi_min,
         data_source, collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_idx, observation_time, forecast_date) DO NOTHING
        """
        
        insert_data = []
        invalid_count = 0
        
        for record in records:
            try:
                # Parse timestamp
                timestamp_str = record.get('timestamp_utc') or record.get('timestamp_local')
                if not timestamp_str:
                    invalid_count += 1
                    continue
                
                try:
                    obs_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except:
                    obs_time = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                
                # Extract station info
                station_uid = record.get('station_uid')
                station_name = record.get('station_name')
                latitude = record.get('latitude')
                longitude = record.get('longitude')
                aqi = record.get('aqi')
                value = record.get('value')
                
                # Convert "-" to None for AQI
                if aqi == '-' or aqi == '' or aqi is None:
                    aqi = None
                else:
                    try:
                        aqi = int(aqi)
                    except (ValueError, TypeError):
                        aqi = None
                
                # Convert "-" to None for value
                if value == '-' or value == '' or value is None:
                    value = None
                else:
                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        value = None
                
                # Validation
                if not all([station_uid, latitude, longitude]):
                    invalid_count += 1
                    continue
                
                # Determine which pollutant field to populate
                so2_val = value if pollutant_type == 'SO2' else None
                co_val = value if pollutant_type == 'CO' else None
                
                insert_data.append((
                    hash(station_uid) % 2147483647,  # Generate station_idx from uid
                    str(station_uid),
                    aqi,
                    pollutant_type.lower(),
                    station_name,
                    None,  # city_url
                    float(latitude),
                    float(longitude),
                    obs_time,
                    None,  # timezone
                    None, None, None, None, None, None,  # h, dew, t, p, w, wg
                    co_val, None, None, so2_val, None, None,  # co, no2, o3, so2, pm10, pm25
                    obs_time.date(),  # forecast_date
                    None, None, None, None, None, None, None, None, None,  # forecast fields
                    'WAQI',
                    collection_timestamp
                ))
                
            except Exception as e:
                print(f"  Error processing record: {e}")
                invalid_count += 1
                continue
        
        # Execute batch insert
        if insert_data:
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            print(f"{pollutant_type} data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
        else:
            print(f"No valid {pollutant_type} records to insert")
        
        cursor.close()
        
    except Exception as e:
        print(f"Error inserting {pollutant_type} data: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()


def insert_pm25_city_records(conn, records):
    """Insert flattened PM2.5 city records"""
    print("Inserting PM2.5 City data into the database...")
    
    if not records:
        print("No PM2.5 records to insert")
        return
    
    try:
        cursor = conn.cursor()
        collection_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        print(f"Found {len(records)} PM2.5 records to process")
        
        insert_query = """
        INSERT INTO waqi_city_forecast_data 
        (station_idx, aqi, dominant_pollutant, city_name, city_url,
         latitude, longitude, observation_time, timezone, h, dew, t, p, w, wg, wd,
         co, no2, o3, so2, pm10, pm25, forecast_date, 
         forecast_o3_avg, forecast_o3_max, forecast_o3_min,
         forecast_pm10_avg, forecast_pm10_max, forecast_pm10_min,
         forecast_pm25_avg, forecast_pm25_max, forecast_pm25_min,
         forecast_uvi_avg, forecast_uvi_max, forecast_uvi_min,
         data_source, collection_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_idx, observation_time, forecast_date) DO NOTHING
        """
        
        insert_data = []
        invalid_count = 0
        
        for record in records:
            try:
                # Parse timestamp
                timestamp_str = record.get('timestamp')
                if not timestamp_str:
                    invalid_count += 1
                    continue
                
                try:
                    obs_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except:
                    obs_time = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                
                # Extract info
                location = record.get('location')
                city = record.get('city')
                latitude = record.get('latitude')
                longitude = record.get('longitude')
                pm25_value = record.get('pm25_value')
                
                # Validation
                if not all([location, latitude, longitude]):
                    invalid_count += 1
                    continue
                
                insert_data.append((
                    hash(location) % 2147483647,  # Generate station_idx from location
                    None,  # aqi
                    'pm25',
                    city,
                    None,  # city_url
                    float(latitude),
                    float(longitude),
                    obs_time,
                    None,  # timezone
                    None, None, None, None, None, None, None,  # h, dew, t, p, w, wg, wd
                    None, None, None, None, None, pm25_value,  # co, no2, o3, so2, pm10, pm25
                    obs_time.date(),  # forecast_date
                    None, None, None, None, None, None, None, None, None, None, None, None,  # forecast fields
                    'WAQI',
                    collection_timestamp
                ))
                
            except Exception as e:
                print(f"  Error processing record: {e}")
                invalid_count += 1
                continue
        
        # Execute batch insert
        if insert_data:
            cursor.executemany(insert_query, insert_data)
            inserted_count = cursor.rowcount
            conn.commit()
            
            print(f"PM2.5 data insertion completed:")
            print(f"  - New records inserted: {inserted_count}")
            print(f"  - Invalid records skipped: {invalid_count}")
        else:
            print("No valid PM2.5 records to insert")
        
        cursor.close()
        
    except Exception as e:
        print(f"Error inserting PM2.5 data: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()



def main():
    conn = None
    try:
        # Fetch both HCHO and NO2 data
        data = fetch_tempo_hcho_data()
        data1 = collect_tempo_no2_data(hours_back=7, max_files=5)
        data2 = fetch_merra2_met_data()
        data3 = fetch_all_air_quality_data()
        data4 = fetch_and_process_pandora_hcho_data()
        data5 = fetch_pblh_data()
        data6 = no2_pipeline()
        data7 = run_tolnet_fetcher()
        data8 = fetch_nasa_aerosol_data()
        data9 = fetch_and_process_goes_data()
        data10 = fetch_cygnss_data()
        data11 = run_fetch_tempo_o3()
        data12 = O3_OZONE_WAQI_DATA()
        data13 = FIRE_SMOKE_DETECTION_DATA()
        data14 = ENHANCE_METEROLOGY_DATA()
        data_so2_wrapper = MISSING_SO2_DATA_WAQI()
        data_co_wrapper = MISSING_CO_DATA_WAQI()
        data_pm25_wrapper = MISSING_PM_2_POINT_5_DATA()  # Move this INSIDE main()
        
        
        

        # Connect to database
        conn = connect_to_db()
        
        
        # Create both tables
        create_table(conn)
        
        # Insert HCHO data (with validation and conflict handling)
        insert_records(conn, data)
        
        # Insert NO2 data (with validation and conflict handling)
        insert_no2_records(conn, data1)

        # Insert MERRA-2 data (with validation and conflict handling)
        insert_merra2_records(conn, data2)

        # Insert Air Quality data (with validation and conflict handling)
        insert_air_quality_records(conn, data3)

        # Insert Pandora HCHO data (with validation and conflict handling)
        insert_pandora_hcho_records(conn, data4)

        # Insert PBLH data (with validation and conflict handling)
        insert_pblh_records(conn, data5)

        # Insert NO2 Pipeline data (with validation and conflict handling)
        insert_no2_pipeline_records(conn, data6)

        # Insert TOLNet data (with validation and conflict handling)
        insert_tolnet_records(conn, data7)

        # Insert NASA Aerosol data (with validation and conflict handling)
        insert_aerosol_records(conn, data8)

        # Insert GOES data (with validation and conflict handling)
        insert_goes_records(conn, data9)  # Insert file metadata
        insert_goes_processed_data(conn, data9)  # Insert processed data
        insert_goes_imagery_data(conn, data9)

        # Insert CYGNSS wind data and temporal analysis
        insert_cygnss_records(conn, data10)
        insert_cygnss_temporal_analysis(conn, data10)

        # Insert TEMPO O3 data
        insert_tempo_o3_records(conn, data11)

        # Insert O3 WAQI data
        insert_o3_waqi_records(conn, data12)

        # Insert Fire Detection data
        insert_fire_detection_records(conn, data13)

        # Insert Enhanced Meteorology Data
        insert_enhanced_weather_grid_records(conn, data14)

        # Fetch data
        data_so2_wrapper = MISSING_SO2_DATA_WAQI()
        data_co_wrapper = MISSING_CO_DATA_WAQI()
        data_pm25_wrapper = MISSING_PM_2_POINT_5_DATA()
        
        # Extract records
        data_so2 = data_so2_wrapper.get('records', []) if data_so2_wrapper else []
        data_co = data_co_wrapper.get('records', []) if data_co_wrapper else []
        data_pm25 = data_pm25_wrapper.get('records', []) if data_pm25_wrapper else []
        
        print(f"SO2 records: {len(data_so2)}")
        print(f"CO records: {len(data_co)}")
        print(f"PM2.5 records: {len(data_pm25)}")
        
        print("\n=== INSERTING DATA ===")
        
        # Insert using the new functions
        if data_so2:
            insert_so2_co_station_records(conn, data_so2, 'SO2')
        
        if data_co:
            insert_so2_co_station_records(conn, data_co, 'CO')
        
        if data_pm25:
            insert_pm25_city_records(conn, data_pm25)


        print("Main function completed successfully!")
        return "Success"
    except Exception as e:
        raise
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()


    
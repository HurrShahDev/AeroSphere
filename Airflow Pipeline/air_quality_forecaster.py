import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.model_selection import TimeSeriesSplit
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
import lightgbm as lgb
import warnings
warnings.filterwarnings('ignore')

class AirQualityForecaster:
    """
    Comprehensive Air Quality Forecasting System
    Implements multiple modeling approaches with time-series aware training
    """
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.models = {}
        self.scalers = {}
        self.feature_importance = {}
        self.training_features = {}  # Store feature names used during training
        
    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            print("✓ Connected to database")
            return True
        except Exception as e:
            print(f"✗ Database connection failed: {e}")
            return False
    
    def fetch_station_data(self, start_date, end_date, cities=None):
        """
        Fetch air quality station data with meteorological variables
        """
        query = """
        SELECT 
            datetime_utc,
            latitude,
            longitude,
            city,
            parameter_name,
            value,
            units,
            location_name,
            location_id
        FROM air_quality_data
        WHERE datetime_utc BETWEEN %s AND %s
        """
        
        params = [start_date, end_date]
        
        if cities:
            placeholders = ','.join(['%s'] * len(cities))
            query += f" AND city IN ({placeholders})"
            params.extend(cities)
        
        query += " ORDER BY datetime_utc, location_id, parameter_name"
        
        df = pd.read_sql(query, self.conn, params=params)
        print(f"✓ Fetched {len(df)} station records")
        return df
    
    def fetch_meteorological_data(self, start_date, end_date):
        """
        Fetch MERRA-2 meteorological data
        """
        query = """
        SELECT 
            granule_time_start as datetime,
            latitude,
            longitude,
            variable_name,
            variable_value,
            variable_units
        FROM merra2_slv_data
        WHERE granule_time_start BETWEEN %s AND %s
        ORDER BY granule_time_start, latitude, longitude
        """
        
        df = pd.read_sql(query, self.conn, params=[start_date, end_date])
        print(f"✓ Fetched {len(df)} meteorological records")
        return df
    
    def fetch_satellite_data(self, start_date, end_date):
        """
        Fetch satellite observations (TEMPO NO2, HCHO, O3)
        """
        # TEMPO NO2
        no2_query = """
        SELECT 
            observation_datetime as datetime,
            latitude,
            longitude,
            no2_tropospheric_column,
            log_no2
        FROM tempo_no2_data
        WHERE observation_datetime BETWEEN %s AND %s
        """
        
        # TEMPO HCHO
        hcho_query = """
        SELECT 
            export_date as datetime,
            latitude,
            longitude,
            hcho_total_column,
            quality_flag
        FROM tempo_hcho_data
        WHERE export_date BETWEEN %s AND %s
        AND quality_flag >= 0
        """
        
        # O3 WAQI
        o3_query = """
        SELECT 
            datetime_utc as datetime,
            latitude,
            longitude,
            o3_aqi,
            overall_aqi
        FROM o3_waqi_data
        WHERE datetime_utc BETWEEN %s AND %s
        """
        
        no2_df = pd.read_sql(no2_query, self.conn, params=[start_date, end_date])
        hcho_df = pd.read_sql(hcho_query, self.conn, params=[start_date, end_date])
        o3_df = pd.read_sql(o3_query, self.conn, params=[start_date, end_date])
        
        print(f"✓ Fetched satellite data: NO2={len(no2_df)}, HCHO={len(hcho_df)}, O3={len(o3_df)}")
        return {'no2': no2_df, 'hcho': hcho_df, 'o3': o3_df}
    
    def fetch_pblh_data(self, start_date, end_date):
        """
        Fetch Planetary Boundary Layer Height data
        """
        query = """
        SELECT 
            timestamp as datetime,
            latitude,
            longitude,
            pbl_height_m
        FROM pblh_data
        WHERE timestamp BETWEEN %s AND %s
        ORDER BY timestamp, latitude, longitude
        """
        
        df = pd.read_sql(query, self.conn, params=[start_date, end_date])
        print(f"✓ Fetched {len(df)} PBLH records")
        return df
    
    def fetch_fire_data(self, start_date, end_date):
        """
        Fetch fire detection data
        """
        query = """
        SELECT 
            acq_date,
            latitude,
            longitude,
            frp,
            confidence,
            bright_ti4,
            bright_ti5
        FROM fire_detection_data
        WHERE acq_date BETWEEN %s AND %s
        AND confidence IN ('h', 'n')
        ORDER BY acq_date
        """
        
        df = pd.read_sql(query, self.conn, params=[start_date, end_date])
        print(f"✓ Fetched {len(df)} fire detection records")
        return df
    
    def engineer_features(self, station_df, met_df, sat_data, pblh_df, fire_df):
        """
        Engineer features from multiple data sources
        """
        # Pivot station data to wide format
        station_wide = station_df.pivot_table(
            index=['datetime_utc', 'latitude', 'longitude', 'location_id', 'city'],
            columns='parameter_name',
            values='value',
            aggfunc='mean'
        )
        
        # CRITICAL: Reset index to make latitude/longitude regular columns
        station_wide = station_wide.reset_index()
        
        # Flatten column names if MultiIndex
        if isinstance(station_wide.columns, pd.MultiIndex):
            station_wide.columns = ['_'.join(map(str, col)).strip('_') if isinstance(col, tuple) else col 
                                   for col in station_wide.columns]
        
        # Convert datetime
        station_wide['datetime_utc'] = pd.to_datetime(station_wide['datetime_utc'])
        
        print(f"  Station columns after pivot: {list(station_wide.columns)[:10]}")  # Debug
        
        # Time-based features
        station_wide['hour'] = station_wide['datetime_utc'].dt.hour
        station_wide['day_of_week'] = station_wide['datetime_utc'].dt.dayofweek
        station_wide['month'] = station_wide['datetime_utc'].dt.month
        station_wide['is_weekend'] = station_wide['day_of_week'].isin([5, 6]).astype(int)
        
        # Cyclical encoding for hour
        station_wide['hour_sin'] = np.sin(2 * np.pi * station_wide['hour'] / 24)
        station_wide['hour_cos'] = np.cos(2 * np.pi * station_wide['hour'] / 24)
        
        # Lag features for autocorrelation (if PM2.5 exists)
        if 'pm25' in station_wide.columns:
            for lag in [1, 6, 24]:
                station_wide[f'pm25_lag_{lag}h'] = station_wide.groupby('location_id')['pm25'].shift(lag)
        
        if 'pm10' in station_wide.columns:
            for lag in [1, 6, 24]:
                station_wide[f'pm10_lag_{lag}h'] = station_wide.groupby('location_id')['pm10'].shift(lag)
        
        # Rolling statistics
        if 'pm25' in station_wide.columns:
            station_wide['pm25_rolling_mean_6h'] = station_wide.groupby('location_id')['pm25'].transform(
                lambda x: x.rolling(window=6, min_periods=1).mean()
            )
            station_wide['pm25_rolling_std_6h'] = station_wide.groupby('location_id')['pm25'].transform(
                lambda x: x.rolling(window=6, min_periods=1).std()
            )
        
        # Merge meteorological data
        if not met_df.empty:
            met_df['datetime'] = pd.to_datetime(met_df['datetime'])
            met_wide = met_df.pivot_table(
                index=['datetime', 'latitude', 'longitude'],
                columns='variable_name',
                values='variable_value',
                aggfunc='mean'
            ).reset_index()
            
            # Rename met columns to avoid conflicts
            met_wide = met_wide.rename(columns={'latitude': 'met_lat', 'longitude': 'met_lon'})
            
            # Spatial-temporal join (nearest neighbor in space and time)
            station_wide = self._merge_nearest(station_wide, met_wide, 
                                               time_col='datetime_utc', 
                                               max_time_diff='1H',
                                               max_spatial_dist=0.5,
                                               lat_col='latitude',
                                               lon_col='longitude',
                                               lat_col2='met_lat',
                                               lon_col2='met_lon')
        
        # Merge PBLH data
        if not pblh_df.empty:
            pblh_df['datetime'] = pd.to_datetime(pblh_df['datetime'])
            station_wide = self._merge_nearest(station_wide, pblh_df,
                                               time_col='datetime_utc',
                                               max_time_diff='1H',
                                               max_spatial_dist=0.5)
        
        # Add fire proximity features
        if not fire_df.empty:
            station_wide = self._add_fire_proximity(station_wide, fire_df)
        
        print(f"✓ Engineered features: {station_wide.shape[1]} columns")
        return station_wide
    
    def _merge_nearest(self, df1, df2, time_col='datetime_utc', 
                       max_time_diff='1H', max_spatial_dist=0.5,
                       lat_col='latitude', lon_col='longitude',
                       lat_col2='latitude', lon_col2='longitude'):
        """
        Merge two dataframes based on nearest spatial-temporal match
        """
        # Ensure latitude and longitude are columns, not index
        if lat_col not in df1.columns and lat_col in df1.index.names:
            df1 = df1.reset_index()
        if lat_col2 not in df2.columns and lat_col2 in df2.index.names:
            df2 = df2.reset_index()
            
        df2_renamed = df2.copy()
        if 'datetime' in df2_renamed.columns:
            df2_renamed.rename(columns={'datetime': time_col}, inplace=True)
        
        # Simple merge on rounded coordinates and time
        df1['lat_round'] = df1[lat_col].round(1)
        df1['lon_round'] = df1[lon_col].round(1)
        df2_renamed['lat_round'] = df2_renamed[lat_col2].round(1)
        df2_renamed['lon_round'] = df2_renamed[lon_col2].round(1)
        
        # Drop the secondary lat/lon columns from df2 to avoid conflicts
        cols_to_drop = [lat_col2, lon_col2]
        df2_renamed = df2_renamed.drop(columns=[c for c in cols_to_drop if c in df2_renamed.columns])
        
        merged = pd.merge_asof(
            df1.sort_values(time_col),
            df2_renamed.sort_values(time_col),
            on=time_col,
            by=['lat_round', 'lon_round'],
            tolerance=pd.Timedelta(max_time_diff),
            direction='nearest',
            suffixes=('', '_met')
        )
        
        merged.drop(['lat_round', 'lon_round'], axis=1, inplace=True)
        return merged
    
    def _add_fire_proximity(self, station_df, fire_df, radius_km=50):
        """
        Add features based on fire proximity
        """
        station_df['fire_count_50km'] = 0
        station_df['fire_frp_sum_50km'] = 0
        
        for idx, station in station_df.iterrows():
            date = station['datetime_utc'].date()
            nearby_fires = fire_df[fire_df['acq_date'] == date]
            
            if not nearby_fires.empty:
                # Calculate distances (simple Euclidean approximation)
                distances = np.sqrt(
                    (nearby_fires['latitude'] - station['latitude'])**2 + 
                    (nearby_fires['longitude'] - station['longitude'])**2
                ) * 111  # Convert to km
                
                within_radius = nearby_fires[distances <= radius_km]
                station_df.at[idx, 'fire_count_50km'] = len(within_radius)
                station_df.at[idx, 'fire_frp_sum_50km'] = within_radius['frp'].sum()
        
        return station_df
    
    def prepare_training_data(self, df, target_col='pm25', horizons=[1, 6, 24]):
        """
        Prepare training data with multiple forecast horizons
        """
        print(f"  Initial dataframe shape: {df.shape}")
        print(f"  Available columns: {df.columns.tolist()}")
        
        # Check if target column exists, if not find available pollutant
        available_pollutants = ['PM25', 'PM10', 'NO2', 'O3', 'CO', 'SO2', 'pm25', 'pm10', 'no2', 'o3', 'co', 'so2']
        
        if target_col not in df.columns:
            for pol in available_pollutants:
                if pol in df.columns:
                    # Check how many non-null values
                    non_null_count = df[pol].notna().sum()
                    print(f"  Found '{pol}' with {non_null_count} non-null values")
                    if non_null_count > 100:  # Need at least 100 samples
                        target_col = pol
                        print(f"  Using '{pol}' as target column")
                        break
            else:
                raise ValueError(f"No suitable pollutant column found with sufficient data. Available columns: {df.columns.tolist()}")
        
        print(f"  Target column '{target_col}' has {df[target_col].notna().sum()} non-null values")
        
        # Create target variables for different horizons
        for h in horizons:
            df[f'{target_col}_target_{h}h'] = df.groupby('location_id')[target_col].shift(-h)
        
        # Drop rows with NaN in target column (not all targets)
        df_clean = df.dropna(subset=[target_col])
        print(f"  After dropping NaN in target: {df_clean.shape[0]} samples")
        
        # Drop rows where ALL target horizons are NaN
        target_cols = [f'{target_col}_target_{h}h' for h in horizons]
        df_clean = df_clean.dropna(subset=target_cols, how='all')
        print(f"  After dropping rows with all NaN targets: {df_clean.shape[0]} samples")
        
        if len(df_clean) < 20:
            raise ValueError(f"Insufficient data after cleaning: only {len(df_clean)} samples. Need at least 20.")
        
        # Feature selection
        exclude_cols = ['datetime_utc', 'location_id', 'location_name', 'city',
                       'latitude', 'longitude'] + target_cols
        
        feature_cols = [col for col in df_clean.columns if col not in exclude_cols and df_clean[col].dtype in ['float64', 'int64']]
        
        X = df_clean[feature_cols].fillna(0)  # Fill remaining NaNs in features with 0
        
        # Handle NaN in targets - drop rows where target is NaN for each horizon
        y_dict = {}
        for h in horizons:
            target_series = df_clean[f'{target_col}_target_{h}h']
            y_dict[h] = target_series[target_series.notna()]
        
        # Filter X and metadata to match the indices that have valid targets for at least one horizon
        valid_indices = set()
        for h in horizons:
            valid_indices.update(y_dict[h].index)
        valid_indices = sorted(valid_indices)
        
        metadata = df_clean.loc[valid_indices, ['datetime_utc', 'location_id', 'latitude', 'longitude']]
        
        print(f"✓ Prepared training data: {X.shape[0]} samples, {X.shape[1]} features")
        print(f"  Target: {target_col}")
        return X, y_dict, metadata, feature_cols
    
    def train_models(self, X_train, y_train, X_val, y_val, horizon, target_name='pm25'):
        """
        Train multiple models for a specific forecast horizon
        """
        models = {}
        results = {}
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_val_scaled = scaler.transform(X_val)
        
        self.scalers[f'{target_name}_{horizon}h'] = scaler
        
        # Store feature names for later alignment
        self.training_features[f'{target_name}_{horizon}h'] = list(X_train.columns)
        
        # 1. Random Forest
        print(f"  Training Random Forest for {horizon}h horizon...")
        rf = RandomForestRegressor(
            n_estimators=100,
            max_depth=20,
            min_samples_split=10,
            min_samples_leaf=5,
            random_state=42,
            n_jobs=-1
        )
        rf.fit(X_train, y_train)
        rf_pred = rf.predict(X_val)
        
        models['rf'] = rf
        results['rf'] = {
            'rmse': np.sqrt(mean_squared_error(y_val, rf_pred)),
            'mae': mean_absolute_error(y_val, rf_pred),
            'predictions': rf_pred
        }
        
        # 2. XGBoost
        print(f"  Training XGBoost for {horizon}h horizon...")
        xgb_model = xgb.XGBRegressor(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            n_jobs=-1
        )
        xgb_model.fit(X_train, y_train)
        xgb_pred = xgb_model.predict(X_val)
        
        models['xgb'] = xgb_model
        results['xgb'] = {
            'rmse': np.sqrt(mean_squared_error(y_val, xgb_pred)),
            'mae': mean_absolute_error(y_val, xgb_pred),
            'predictions': xgb_pred
        }
        
        # 3. LightGBM
        print(f"  Training LightGBM for {horizon}h horizon...")
        lgb_model = lgb.LGBMRegressor(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            num_leaves=31,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            n_jobs=-1,
            verbose=-1
        )
        lgb_model.fit(X_train, y_train)
        lgb_pred = lgb_model.predict(X_val)
        
        models['lgb'] = lgb_model
        results['lgb'] = {
            'rmse': np.sqrt(mean_squared_error(y_val, lgb_pred)),
            'mae': mean_absolute_error(y_val, lgb_pred),
            'predictions': lgb_pred
        }
        
        # Store feature importance from best model
        best_model_name = min(results.keys(), key=lambda k: results[k]['rmse'])
        best_model = models[best_model_name]
        
        if hasattr(best_model, 'feature_importances_'):
            self.feature_importance[f'{target_name}_{horizon}h'] = dict(
                zip(X_train.columns, best_model.feature_importances_)
            )
        
        return models, results
    
    def time_series_split_train(self, X, y_dict, feature_cols, 
                                target_name='pm25', horizons=[1, 6, 24]):
        """
        Train models with time-series aware split
        """
        all_results = {}
        
        for horizon in horizons:
            print(f"\n→ Training models for {horizon}h forecast horizon")
            
            # Get valid samples for this horizon
            y_full = y_dict[horizon]
            valid_idx = y_full.notna()
            
            # Filter both X and y using the same valid indices
            X_valid = X.loc[y_full.index[valid_idx]]
            y_valid = y_full[valid_idx]
            
            # Reset index to ensure alignment
            X_valid = X_valid.reset_index(drop=True)
            y_valid = y_valid.reset_index(drop=True)
            
            if len(X_valid) < 20:
                print(f"  ⚠ Insufficient data for {horizon}h horizon: only {len(X_valid)} samples. Skipping.")
                continue
            
            # Time-based split (80% train, 20% validation)
            split_idx = int(len(X_valid) * 0.8)
            
            X_train = X_valid.iloc[:split_idx]
            X_val = X_valid.iloc[split_idx:]
            y_train = y_valid.iloc[:split_idx]
            y_val = y_valid.iloc[split_idx:]
            
            print(f"  Training samples: {len(X_train)}, Validation samples: {len(X_val)}")
            
            models, results = self.train_models(
                X_train, y_train, X_val, y_val, horizon, target_name
            )
            
            self.models[f'{target_name}_{horizon}h'] = models
            all_results[f'{horizon}h'] = results
            
            # Print results
            print(f"\n  Results for {horizon}h horizon:")
            for model_name, metrics in results.items():
                print(f"    {model_name.upper()}: RMSE={metrics['rmse']:.2f}, MAE={metrics['mae']:.2f}")
        
        return all_results
    
    def calculate_aqi(self, pollutants):
        """
        Calculate AQI from pollutant concentrations (US EPA standard)
        """
        def calculate_individual_aqi(conc, breakpoints):
            for bp_low, bp_high, aqi_low, aqi_high in breakpoints:
                if bp_low <= conc <= bp_high:
                    return ((aqi_high - aqi_low) / (bp_high - bp_low)) * (conc - bp_low) + aqi_low
            return None
        
        # US EPA breakpoints
        pm25_bp = [
            (0.0, 12.0, 0, 50),
            (12.1, 35.4, 51, 100),
            (35.5, 55.4, 101, 150),
            (55.5, 150.4, 151, 200),
            (150.5, 250.4, 201, 300),
            (250.5, 500.0, 301, 500)
        ]
        
        pm10_bp = [
            (0, 54, 0, 50),
            (55, 154, 51, 100),
            (155, 254, 101, 150),
            (255, 354, 151, 200),
            (355, 424, 201, 300),
            (425, 604, 301, 500)
        ]
        
        aqi_values = []
        
        if 'pm25' in pollutants:
            pm25_aqi = calculate_individual_aqi(pollutants['pm25'], pm25_bp)
            if pm25_aqi: aqi_values.append(pm25_aqi)
        
        if 'pm10' in pollutants:
            pm10_aqi = calculate_individual_aqi(pollutants['pm10'], pm10_bp)
            if pm10_aqi: aqi_values.append(pm10_aqi)
        
        return max(aqi_values) if aqi_values else None
    
    def predict_with_uncertainty(self, X_new, target_name='pm25', horizon=24):
        """
        Generate predictions with uncertainty bands using ensemble
        """
        model_key = f'{target_name}_{horizon}h'
        
        if model_key not in self.models:
            raise ValueError(f"Model for {model_key} not trained")
        
        # Scale features
        scaler = self.scalers[model_key]
        X_scaled = scaler.transform(X_new)
        
        # Get predictions from all models
        predictions = []
        for model_name, model in self.models[model_key].items():
            pred = model.predict(X_new)
            predictions.append(pred)
        
        predictions = np.array(predictions)
        
        # Calculate mean and std
        mean_pred = predictions.mean(axis=0)
        std_pred = predictions.std(axis=0)
        
        # 95% confidence interval
        lower_bound = mean_pred - 1.96 * std_pred
        upper_bound = mean_pred + 1.96 * std_pred
        
        return {
            'mean': mean_pred,
            'lower_95': lower_bound,
            'upper_95': upper_bound,
            'std': std_pred
        }
    
    def get_city_aqi_forecast(self, city_name):
        """
        Get multi-day AQI forecast for a city
        Shows today, tomorrow, and day after tomorrow AQI
        """
        print(f"\nGenerating AQI forecast for {city_name}...")
        
        # Fetch recent data for the city
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        query = """
        SELECT 
            datetime_utc,
            latitude,
            longitude,
            city,
            parameter_name,
            value,
            location_name,
            location_id
        FROM air_quality_data
        WHERE city ILIKE %s
        AND datetime_utc >= %s
        ORDER BY datetime_utc DESC
        LIMIT 1000
        """
        
        station_df = pd.read_sql(query, self.conn, params=[f'%{city_name}%', start_date])
        
        if station_df.empty:
            print(f"No data found for city: {city_name}")
            return None
        
        # Get the most recent complete observation
        latest_time = station_df['datetime_utc'].max()
        latest_data = station_df[station_df['datetime_utc'] == latest_time]
        
        # Pivot to get all pollutants
        latest_pivot = latest_data.pivot_table(
            index=['datetime_utc', 'latitude', 'longitude', 'location_id', 'city'],
            columns='parameter_name',
            values='value',
            aggfunc='mean'
        ).reset_index()
        
        lat = float(latest_pivot['latitude'].iloc[0])
        lon = float(latest_pivot['longitude'].iloc[0])
        location = latest_pivot['location_id'].iloc[0] if 'location_id' in latest_pivot else 'N/A'
        
        # Get current AQI
        current_pollutants = {}
        for pol in ['PM10', 'PM2.5', 'PM25']:
            if pol in latest_pivot.columns:
                current_pollutants[pol.lower().replace('.', '')] = latest_pivot[pol].iloc[0]
        
        current_aqi = self.calculate_aqi(current_pollutants) if current_pollutants else None
        
        # Prepare features for forecasting
        latest_pivot['datetime_utc'] = pd.to_datetime(latest_pivot['datetime_utc'])
        latest_pivot['hour'] = latest_pivot['datetime_utc'].dt.hour
        latest_pivot['day_of_week'] = latest_pivot['datetime_utc'].dt.dayofweek
        latest_pivot['month'] = latest_pivot['datetime_utc'].dt.month
        latest_pivot['is_weekend'] = latest_pivot['day_of_week'].isin([5, 6]).astype(int)
        latest_pivot['hour_sin'] = np.sin(2 * np.pi * latest_pivot['hour'] / 24)
        latest_pivot['hour_cos'] = np.cos(2 * np.pi * latest_pivot['hour'] / 24)
        
        # Fetch and add meteorological features
        met_query = """
        SELECT 
            granule_time_start as datetime,
            variable_name,
            variable_value
        FROM merra2_slv_data
        WHERE latitude BETWEEN %s AND %s
        AND longitude BETWEEN %s AND %s
        AND granule_time_start >= %s
        ORDER BY granule_time_start DESC
        LIMIT 100
        """
        
        met_df = pd.read_sql(met_query, self.conn, 
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
        
        # Add PBLH
        pblh_query = """
        SELECT 
            timestamp as datetime,
            pbl_height_m
        FROM pblh_data
        WHERE latitude BETWEEN %s AND %s
        AND longitude BETWEEN %s AND %s
        AND timestamp >= %s
        ORDER BY timestamp DESC
        LIMIT 50
        """
        
        pblh_df = pd.read_sql(pblh_query, self.conn,
                             params=[lat-0.5, lat+0.5, lon-0.5, lon+0.5, start_date])
        
        if not pblh_df.empty:
            latest_pivot['pbl_height_m'] = pblh_df['pbl_height_m'].iloc[0]
        else:
            latest_pivot['pbl_height_m'] = 0
        
        latest_pivot['fire_count_50km'] = 0
        latest_pivot['fire_frp_sum_50km'] = 0
        latest_pivot = latest_pivot.fillna(0)
        
        # Generate forecasts for 24h, 48h, 72h
        forecast_horizons = [24, 48, 72]
        aqi_forecasts = {}
        
        print(f"  Available trained models: {list(self.models.keys())}")
        
        for target_horizon in forecast_horizons:
            pollutant_forecasts = {}
            
            # Try to forecast PM10 and PM2.5/PM25 for AQI calculation
            for pollutant in ['PM10', 'PM2.5', 'PM25']:
                if pollutant not in latest_pivot.columns:
                    continue
                
                # Find the best available trained horizon (prefer 24h, then 6h, then 1h)
                available_horizons = [24, 6, 1]
                use_horizon = None
                
                for h in available_horizons:
                    model_key = f'{pollutant}_{h}h'
                    if model_key in self.models:
                        use_horizon = h
                        break
                
                if use_horizon is None:
                    print(f"  ⚠ No trained model found for {pollutant}")
                    continue
                
                model_key = f'{pollutant}_{use_horizon}h'
                print(f"  Using model {model_key} for {target_horizon}h forecast of {pollutant}")
                
                try:
                    # Get the training features for this model
                    training_features = self.training_features.get(model_key, [])
                    
                    if not training_features:
                        print(f"  ⚠ No training features found for {model_key}")
                        continue
                    
                    # Align features with training features
                    X_new = pd.DataFrame()
                    for feat in training_features:
                        if feat in latest_pivot.columns:
                            X_new[feat] = latest_pivot[feat]
                        else:
                            X_new[feat] = 0  # Fill missing features with 0
                    
                    # Update temporal features for target horizon
                    forecast_time = latest_pivot['datetime_utc'].iloc[0] + timedelta(hours=target_horizon)
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
                    
                    # Get prediction
                    prediction = self.predict_with_uncertainty(X_new, pollutant, use_horizon)
                    
                    # Apply persistence decay for longer horizons
                    decay_factor = 1.0
                    if target_horizon > use_horizon:
                        # Gradually increase uncertainty for longer forecasts
                        decay_factor = 0.95 ** ((target_horizon - use_horizon) / use_horizon)
                    
                    predicted_value = prediction['mean'][0] * decay_factor
                    
                    # Store with normalized key
                    pol_key = pollutant.lower().replace('.', '').replace('25', '25')
                    if pol_key == 'pm25':
                        pol_key = 'pm25'  # Normalize PM2.5 to pm25
                    
                    pollutant_forecasts[pol_key] = predicted_value
                    print(f"  ✓ Predicted {pollutant} for {target_horizon}h: {predicted_value:.2f}")
                    
                except Exception as e:
                    print(f"  ✗ Error predicting {pollutant} for {target_horizon}h: {e}")
                    import traceback
                    traceback.print_exc()
            
            if pollutant_forecasts:
                aqi = self.calculate_aqi(pollutant_forecasts)
                if aqi:
                    aqi_forecasts[target_horizon] = {
                        'aqi': aqi,
                        'pollutants': pollutant_forecasts
                    }
                    print(f"  ✓ Calculated AQI for {target_horizon}h: {aqi:.0f}")
                else:
                    print(f"  ⚠ Could not calculate AQI for {target_horizon}h")
            else:
                print(f"  ⚠ No pollutant forecasts available for {target_horizon}h")
        
        # Display results
        print(f"\n{'='*60}")
        print(f"AQI Forecast for {city_name}")
        print(f"{'='*60}")
        print(f"Location: {location}")
        print(f"Coordinates: ({lat:.4f}, {lon:.4f})")
        print(f"Last updated: {latest_time.strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"\n{'='*60}")
        
        # Today's AQI
        if current_aqi:
            category = self._get_aqi_category(current_aqi)
            print(f"\nTODAY ({latest_time.strftime('%Y-%m-%d')})")
            print(f"  AQI: {current_aqi:.0f} - {category}")
            self._print_health_advisory(current_aqi)
        else:
            print(f"\nTODAY: No AQI data available")
        
        # Tomorrow (24h forecast)
        if 24 in aqi_forecasts:
            tomorrow = latest_time + timedelta(hours=24)
            aqi = aqi_forecasts[24]['aqi']
            category = self._get_aqi_category(aqi)
            print(f"\nTOMORROW ({tomorrow.strftime('%Y-%m-%d')})")
            print(f"  Forecasted AQI: {aqi:.0f} - {category}")
            pols = aqi_forecasts[24]['pollutants']
            print(f"  Pollutants: " + ", ".join([f"{k.upper()}={v:.1f}" for k, v in pols.items()]))
            self._print_health_advisory(aqi)
        else:
            print(f"\nTOMORROW: Forecast not available (no trained models or insufficient data)")
        
        # Day after tomorrow (48h forecast)
        if 48 in aqi_forecasts:
            day_after = latest_time + timedelta(hours=48)
            aqi = aqi_forecasts[48]['aqi']
            category = self._get_aqi_category(aqi)
            print(f"\nDAY AFTER TOMORROW ({day_after.strftime('%Y-%m-%d')})")
            print(f"  Forecasted AQI: {aqi:.0f} - {category}")
            pols = aqi_forecasts[48]['pollutants']
            print(f"  Pollutants: " + ", ".join([f"{k.upper()}={v:.1f}" for k, v in pols.items()]))
            self._print_health_advisory(aqi)
        else:
            print(f"\nDAY AFTER TOMORROW: Forecast not available")
        
        # 3 days ahead (72h forecast)
        if 72 in aqi_forecasts:
            three_days = latest_time + timedelta(hours=72)
            aqi = aqi_forecasts[72]['aqi']
            category = self._get_aqi_category(aqi)
            print(f"\n3 DAYS AHEAD ({three_days.strftime('%Y-%m-%d')})")
            print(f"  Forecasted AQI: {aqi:.0f} - {category}")
            pols = aqi_forecasts[72]['pollutants']
            print(f"  Pollutants: " + ", ".join([f"{k.upper()}={v:.1f}" for k, v in pols.items()]))
            self._print_health_advisory(aqi)
        else:
            print(f"\n3 DAYS AHEAD: Forecast not available")
        
        if not aqi_forecasts:
            print(f"\n⚠ No forecasts could be generated. This usually means:")
            print(f"  1. Models haven't been trained yet (run option 1 or 3 first)")
            print(f"  2. Required pollutants (PM10/PM2.5) are not available")
            print(f"  3. Feature alignment issues between training and prediction")
        
        print(f"\n{'='*60}\n")
        
        return aqi_forecasts
    
    def _get_aqi_category(self, aqi):
        """Get AQI category from value"""
        if aqi <= 50:
            return "Good"
        elif aqi <= 100:
            return "Moderate"
        elif aqi <= 150:
            return "Unhealthy for Sensitive Groups"
        elif aqi <= 200:
            return "Unhealthy"
        elif aqi <= 300:
            return "Very Unhealthy"
        else:
            return "Hazardous"
    
    def _print_health_advisory(self, aqi):
        """Print health advisory based on AQI"""
        if aqi <= 50:
            print("  Health: Air quality is satisfactory")
        elif aqi <= 100:
            print("  Health: Acceptable for most, sensitive individuals may experience minor effects")
        elif aqi <= 150:
            print("  Health: Sensitive groups should reduce prolonged outdoor exertion")
        elif aqi <= 200:
            print("  Health: Everyone may experience health effects; sensitive groups at greater risk")
        elif aqi <= 300:
            print("  Health: Health alert - everyone may experience serious health effects")
        else:
            print("  Health: Health warning - emergency conditions, entire population affected")
    
    def get_available_cities(self):
        """
        Get list of cities with available air quality data
        """
        query = """
        SELECT DISTINCT city, COUNT(*) as record_count
        FROM air_quality_data
        WHERE city IS NOT NULL AND city != ''
        GROUP BY city
        ORDER BY record_count DESC
        """
        
        cities_df = pd.read_sql(query, self.conn)
        return cities_df
    
    def get_all_cities_aqi(self):
        """
        Get current AQI for all available cities
        """
        print("\nFetching AQI for all cities...")
        
        # Get distinct cities
        cities_df = self.get_available_cities()
        
        if cities_df.empty:
            print("No cities found")
            return
        
        city_aqi_list = []
        
        for idx, row in cities_df.head(30).iterrows():  # Limit to top 30 cities
            city = row['city']
            
            # Get latest data for this city
            query = """
            SELECT 
                datetime_utc,
                parameter_name,
                value,
                latitude,
                longitude
            FROM air_quality_data
            WHERE city = %s
            ORDER BY datetime_utc DESC
            LIMIT 100
            """
            
            df = pd.read_sql(query, self.conn, params=[city])
            
            if df.empty:
                continue
            
            # Get most recent observation
            latest_time = df['datetime_utc'].max()
            latest = df[df['datetime_utc'] == latest_time]
            
            # Calculate AQI
            pollutants = {}
            for pol in ['PM10', 'PM2.5', 'PM25']:
                pol_data = latest[latest['parameter_name'] == pol]
                if not pol_data.empty:
                    pollutants[pol.lower().replace('.', '')] = pol_data['value'].mean()
            
            if pollutants:
                aqi = self.calculate_aqi(pollutants)
                if aqi:
                    city_aqi_list.append({
                        'city': city,
                        'aqi': aqi,
                        'category': self._get_aqi_category(aqi),
                        'time': latest_time,
                        'latitude': latest['latitude'].iloc[0],
                        'longitude': latest['longitude'].iloc[0]
                    })
        
        # Sort by AQI (worst first)
        city_aqi_list.sort(key=lambda x: x['aqi'], reverse=True)
        
        # Display results
        print(f"\n{'='*70}")
        print(f"Current AQI for All Cities")
        print(f"{'='*70}")
        print(f"{'City':<20} {'AQI':<8} {'Category':<25} {'Updated':<20}")
        print(f"{'-'*70}")
        
        for item in city_aqi_list:
            print(f"{item['city']:<20} {item['aqi']:<8.0f} {item['category']:<25} {item['time'].strftime('%Y-%m-%d %H:%M')}")
        
        print(f"{'='*70}\n")
        
        return city_aqi_list
    
    def interactive_forecast(self):
        """
        Interactive mode for city-based forecasts
        """
        print("\n" + "="*60)
        print("Air Quality Forecasting System - Interactive Mode")
        print("="*60)
        
        while True:
            print("\nOptions:")
            print("1. Get AQI forecast for a specific city (Today, Tomorrow, Day After)")
            print("2. View all cities current AQI")
            print("3. Detailed pollutant forecast for a city")
            print("4. Quit")
            
            choice = input("\nEnter your choice (1/2/3/4): ").strip()
            
            if choice == '4' or choice.lower() in ['quit', 'exit', 'q']:
                print("\nGoodbye!")
                break
            
            if choice == '1':
                # Multi-day AQI forecast
                cities_df = self.get_available_cities()
                
                if not cities_df.empty:
                    print(f"\nAvailable cities ({len(cities_df)} total):")
                    print("-" * 60)
                    for idx, row in cities_df.head(20).iterrows():
                        print(f"  {row['city']} ({row['record_count']} records)")
                    if len(cities_df) > 20:
                        print(f"  ... and {len(cities_df) - 20} more cities")
                
                city = input("\nEnter city name: ").strip()
                
                if not city:
                    print("Please enter a valid city name.")
                    continue
                
                self.get_city_aqi_forecast(city)
            
            elif choice == '2':
                # All cities AQI
                self.get_all_cities_aqi()
            
            elif choice == '3':
                # Detailed pollutant forecast - placeholder
                print("\nDetailed pollutant forecast feature coming soon!")
            
            else:
                print("Invalid choice. Please enter 1, 2, 3, or 4.")
            
            print("\n" + "-"*60)
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("✓ Database connection closed")


# Example usage
if __name__ == "__main__":
    # Database configuration - UPDATE THESE VALUES
    db_config = {
        'host': 'localhost',
        'port': 5000,
        'dbname': 'db',
        'user': 'db_user',
        'password': 'db_password'
    }
    
    # Initialize forecaster
    forecaster = AirQualityForecaster(db_config)
    
    if forecaster.connect_db():
        
        # Check if user wants to train or just forecast
        print("\n" + "="*60)
        print("Air Quality Forecasting System")
        print("="*60)
        print("\nOptions:")
        print("1. Train new models (required for first time)")
        print("2. City forecast (requires trained models)")
        print("3. Train and then forecast")
        
        choice = input("\nEnter your choice (1/2/3): ").strip()
        
        if choice in ['1', '3']:
            # Training mode
            end_date = datetime.now()
            start_date = end_date - timedelta(days=90)  # 3 months of data
            
            print(f"\n📊 Fetching data from {start_date.date()} to {end_date.date()}")
            
            # Fetch all data sources
            station_df = forecaster.fetch_station_data(start_date, end_date)
            met_df = forecaster.fetch_meteorological_data(start_date, end_date)
            sat_data = forecaster.fetch_satellite_data(start_date, end_date)
            pblh_df = forecaster.fetch_pblh_data(start_date, end_date)
            fire_df = forecaster.fetch_fire_data(start_date, end_date)
            
            # Engineer features
            print("\n🔧 Engineering features...")
            feature_df = forecaster.engineer_features(
                station_df, met_df, sat_data, pblh_df, fire_df
            )
            
            # Get all available pollutants
            pollutant_cols = ['PM10', 'PM2.5', 'PM25', 'NO2', 'O3', 'CO', 'SO2']
            available_pollutants = [col for col in pollutant_cols if col in feature_df.columns]
            
            print(f"\n📋 Available pollutants for training: {', '.join(available_pollutants)}")
            
            # Train models for each pollutant
            for pollutant in available_pollutants:
                print(f"\n{'='*60}")
                print(f"Training models for {pollutant}")
                print(f"{'='*60}")
                
                try:
                    # Prepare training data for multiple horizons
                    print(f"\n📝 Preparing training data for {pollutant}...")
                    X, y_dict, metadata, feature_cols = forecaster.prepare_training_data(
                        feature_df.copy(), 
                        target_col=pollutant,
                        horizons=[1, 6, 24]
                    )
                    
                    # Train models
                    print(f"\n🚀 Training models for {pollutant}...")
                    results = forecaster.time_series_split_train(
                        X, y_dict, feature_cols,
                        target_name=pollutant,
                        horizons=[1, 6, 24]
                    )
                    
                except Exception as e:
                    print(f"⚠ Could not train models for {pollutant}: {e}")
                    continue
            
            print("\n✅ Model training complete!")
        
        if choice in ['2', '3']:
            # Interactive forecast mode
            if choice == '2' and not forecaster.models:
                print("\n⚠ No trained models found. Please train models first (option 1).")
            else:
                forecaster.interactive_forecast()
        
        forecaster.close()
        
        print("\n✅ Session complete!")
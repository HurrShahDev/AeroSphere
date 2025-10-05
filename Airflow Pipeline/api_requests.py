# ---------------------------------------------------------------------------------------------------------------------------------------
# TEMPO 2


import requests
import json
from datetime import datetime, timedelta
import os
import netCDF4 as nc
import numpy as np
from pathlib import Path


def fetch_tempo_hcho_data():
    """Fetch and display TEMPO HCHO JSON data from NASA Earthdata"""
    
    # Configuration
    EARTHDATA_TOKEN = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6Imhhc3NhbnNpZGRpcXVpMDk0NiIsImV4cCI6MTc2MzMzNzU5OSwiaWF0IjoxNzU4MTI3MjA2LCJpc3MiOiJodHRwczovL3Vycy5lYXJ0aGRhdGEubmFzYS5nb3YiLCJpZGVudGl0eV9wcm92aWRlciI6ImVkbF9vcHMiLCJhY3IiOiJlZGwiLCJhc3N1cmFuY2VfbGV2ZWwiOjN9.LIpvasyxPBB3hgB0af3O9sz37usL0slvOqL8fCC76Ba181aTCJLmfr6mwgkmB_P1LkQXcl624wm_H5_LCi-nj2A0JGcxEVkFVH4P5hSseXLW0Zz2HgSMEBro5fGzucPNYLK6yyae-NXpAqnphQsvr_TJzuvjEkeGPuhkB6h8J6DB_RyXSutov7-3Gbxxs6FLArAbFIWMvZi0pggIXE2hBsrdS7fl2bsrIevB_CVuuEQAfszdW6LthIhBkSy1g6qtRV_LncvGSEylyXt5Ave4pjoCS8memaq37p6uNTUTSUlSXIGYoTSyLohndj3YbKRlARXiQokrdZ9HMXjfJ02Azw"
    BASE_URL = "https://cmr.earthdata.nasa.gov/search"
    COLLECTIONS = [
        ("TEMPO_HCHO_L2", "LARC_CLOUD", "C2930730944-LARC_CLOUD"),
        ("TEMPO_HCHO_L3", "LARC_CLOUD", "C3685897141-LARC_CLOUD"),
        ("TEMPO_HCHO_L3", "LARC_CLOUD", "C2930761273-LARC_CLOUD"),
        ("TEMPO_HCHO_L3_NRT", "LARC_CLOUD", "C3685668680-LARC_CLOUD")
    ]
    
    print("=== TEMPO HCHO JSON DATA DISPLAY ===\n")
    
    # Setup session with authentication
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Bearer {EARTHDATA_TOKEN}',
        'User-Agent': 'TEMPO-HCHO-Access/1.0'
    })
    
    # Check for existing NetCDF files first
    existing_files = []
    data_dirs = ["./tempo_ml_data/netcdf_files", "./tempo_data", "./tempo_single_file"]
    
    for data_dir in data_dirs:
        if os.path.exists(data_dir):
            for file in os.listdir(data_dir):
                if file.endswith('.nc'):
                    existing_files.append(os.path.join(data_dir, file))
    
    # Process existing file if found
    if existing_files:
        print(f"Found {len(existing_files)} existing NetCDF files")
        test_file = existing_files[0]
        print(f"Processing: {os.path.basename(test_file)}\n")
        
        # Read NetCDF file
        try:
            with nc.Dataset(test_file, 'r') as dataset:
                print(f"📂 NetCDF Structure:")
                print(f"   Root dimensions: {list(dataset.dimensions.keys())}")
                print(f"   Root variables: {list(dataset.variables.keys())}")
                print(f"   Groups: {list(dataset.groups.keys())}\n")
                
                data = {}
                
                # Get latitude and longitude arrays (1D)
                if 'latitude' in dataset.variables:
                    print("✓ Found latitude in root")
                    lat_1d = dataset.variables['latitude'][:]
                    print(f"  Shape: {lat_1d.shape}")
                
                if 'longitude' in dataset.variables:
                    print("✓ Found longitude in root")
                    lon_1d = dataset.variables['longitude'][:]
                    print(f"  Shape: {lon_1d.shape}")
                
                # Check product group for HCHO data
                if 'product' in dataset.groups:
                    print(f"\n📁 Checking group: product")
                    group = dataset.groups['product']
                    print(f"   Variables: {list(group.variables.keys())}")
                    
                    if 'vertical_column' in group.variables:
                        var = group.variables['vertical_column']
                        hcho_3d = var[:]
                        data['hcho_units'] = getattr(var, 'units', 'molecules/cm²')
                        print(f"   ✓ vertical_column: {hcho_3d.shape}")
                        
                        # Remove time dimension if present (assuming index 0)
                        if len(hcho_3d.shape) == 3:
                            data['hcho_total_column'] = hcho_3d[0]  # Shape: (2950, 7750)
                            print(f"   → Squeezed to: {data['hcho_total_column'].shape}")
                        else:
                            data['hcho_total_column'] = hcho_3d
                    
                    if 'vertical_column_uncertainty' in group.variables:
                        var = group.variables['vertical_column_uncertainty'][:]
                        if len(var.shape) == 3:
                            data['hcho_uncertainty'] = var[0]
                        else:
                            data['hcho_uncertainty'] = var
                        print(f"   ✓ uncertainty: {data['hcho_uncertainty'].shape}")
                    
                    if 'main_data_quality_flag' in group.variables:
                        var = group.variables['main_data_quality_flag'][:]
                        if len(var.shape) == 3:
                            data['quality_flag'] = var[0]
                        else:
                            data['quality_flag'] = var
                        print(f"   ✓ quality_flag: {data['quality_flag'].shape}")
                
                data['source_file'] = os.path.basename(test_file)
                
                # Create 2D meshgrid from 1D lat/lon
                hcho_data = data.get('hcho_total_column')
                
                print(f"\n📊 Data Summary:")
                print(f"   HCHO data: {'Found' if hcho_data is not None else 'Missing'}")
                print(f"   Latitude array: {'Found' if 'lat_1d' in locals() else 'Missing'}")
                print(f"   Longitude array: {'Found' if 'lon_1d' in locals() else 'Missing'}")
                
                if hcho_data is not None and 'lat_1d' in locals() and 'lon_1d' in locals():
                    print(f"\n🔄 Creating meshgrid and processing arrays...")
                    
                    # Create 2D meshgrid
                    lon_2d, lat_2d = np.meshgrid(lon_1d, lat_1d)
                    print(f"   Created meshgrid: lat {lat_2d.shape}, lon {lon_2d.shape}")
                    
                    # Flatten all arrays
                    hcho_flat = hcho_data.flatten()
                    lat_flat = lat_2d.flatten()
                    lon_flat = lon_2d.flatten()
                    quality_flat = data.get('quality_flag', np.full_like(hcho_flat, np.nan)).flatten()
                    uncertainty_flat = data.get('hcho_uncertainty', np.full_like(hcho_flat, np.nan)).flatten()
                    
                    print(f"   Flattened arrays: {len(hcho_flat)} elements each")
                    
                    # Create validity mask
                    valid_mask = (
                        np.isfinite(hcho_flat) &
                        (hcho_flat > -1e30) & (hcho_flat < 1e30) &
                        np.isfinite(lat_flat) & np.isfinite(lon_flat) &
                        (lat_flat >= -90) & (lat_flat <= 90) &
                        (lon_flat >= -180) & (lon_flat <= 180)
                    )
                    
                    print(f"   Valid finite values: {np.sum(valid_mask)}")
                    
                    # Apply quality filter if available
                    if np.any(np.isfinite(quality_flat)):
                        quality_mask = np.isin(quality_flat, [0, 1])
                        valid_mask = valid_mask & quality_mask
                        print(f"   After quality filter: {np.sum(valid_mask)}")
                    
                    valid_indices = np.where(valid_mask)[0][:22000000]
                    print(f"   Selected for output: {len(valid_indices)}")
                    
                    records = []
                    for idx in valid_indices:
                        record = {
                            'latitude': float(lat_flat[idx]),
                            'longitude': float(lon_flat[idx]),
                            'hcho_total_column': float(hcho_flat[idx]),
                            'hcho_units': data.get('hcho_units', 'molecules/cm²')
                        }
                        if np.isfinite(uncertainty_flat[idx]):
                            record['hcho_uncertainty'] = float(uncertainty_flat[idx])
                        if np.isfinite(quality_flat[idx]):
                            record['quality_flag'] = int(quality_flat[idx])
                        records.append(record)
                    
                    if records:
                        hcho_values = [r['hcho_total_column'] for r in records]
                        json_output = {
                            'metadata': {
                                'source_file': data.get('source_file', 'unknown'),
                                'export_date': datetime.now().isoformat(),
                                'total_valid_measurements': len(valid_indices),
                                'records_in_output': len(records),
                                'data_units': data.get('hcho_units', 'molecules/cm²')
                            },
                            'statistics': {
                                'hcho_min': float(np.min(hcho_values)),
                                'hcho_max': float(np.max(hcho_values)),
                                'hcho_mean': float(np.mean(hcho_values)),
                                'hcho_median': float(np.median(hcho_values))
                            },
                            'data': records
                        }
                        
                        print(f"\n{'='*60}")
                        print("JSON OUTPUT:")
                        print('='*60)
                        print(json.dumps(json_output, indent=2))
                        print(f"\n✅ Successfully processed {len(records)} records")
                        return json_output
                    else:
                        print("\n❌ No valid records created")
                else:
                    print("\n❌ Missing required data arrays")
        
        except Exception as e:
            print(f"\n❌ Error reading NetCDF file: {e}")
            import traceback
            traceback.print_exc()
    
    # If no existing files, download new data
    print("\nNo existing files found. Downloading fresh data...\n")
    
    try:
        # Calculate current time minus 73 hours (Python 3.7+ compatible)
        from datetime import timezone
        current_time = datetime.now(timezone.utc)
        start_time = current_time - timedelta(hours=73)
        
        print(f"🕐 Current UTC time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🕐 Start time (73 hours ago): {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # Find available collection
        collection_concept_id = None
        for short_name, provider, concept_id in COLLECTIONS:
            params = {
                'collection_concept_id': concept_id,
                'temporal': f'{start_time.strftime("%Y-%m-%dT%H:%M:%SZ")},{current_time.strftime("%Y-%m-%dT%H:%M:%SZ")}',
                'page_size': 1,
                'pretty': True
            }
            response = session.get(f"{BASE_URL}/granules.json", params=params)
            response.raise_for_status()
            data = response.json()
            if data.get('feed', {}).get('entry', []):
                collection_concept_id = concept_id
                print(f"✓ Found collection: {short_name}")
                break
        
        if not collection_concept_id:
            print("❌ No TEMPO HCHO collections found with data in the specified time range")
            return None
        
        # Search for granules with updated temporal range
        bounding_box = (-100, 30, -70, 50)
        
        params = {
            'collection_concept_id': collection_concept_id,
            'temporal': f'{start_time.strftime("%Y-%m-%dT%H:%M:%SZ")},{current_time.strftime("%Y-%m-%dT%H:%M:%SZ")}',
            'bounding_box': f'{bounding_box[0]},{bounding_box[1]},{bounding_box[2]},{bounding_box[3]}',
            'page_size': 2000,
            'page_num': 1,
            'pretty': True
        }
        
        response = session.get(f"{BASE_URL}/granules.json", params=params)
        response.raise_for_status()
        data = response.json()
        granules = data.get('feed', {}).get('entry', [])
        
        if not granules:
            print("❌ No granules found in the specified time range")
            return None
        
        print(f"✓ Found {len(granules)} granules")
        
        # Download first granule
        granule = granules[0]
        download_url = None
        for link in granule.get('links', []):
            if link.get('rel') == 'http://esipfed.org/ns/fedsearch/1.1/data#':
                download_url = link.get('href')
                break
        
        if not download_url:
            print("❌ No download link found")
            return None
        
        download_dir = "./tempo_data"
        Path(download_dir).mkdir(parents=True, exist_ok=True)
        filename = download_url.split('/')[-1]
        filepath = os.path.join(download_dir, filename)
        
        print(f"📥 Downloading: {filename}")
        response = session.get(download_url, stream=True)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        print(f"✓ Downloaded successfully to: {filepath}")
        print(f"\n🔄 Re-run the function to process the downloaded file")
        return None
        
    except requests.exceptions.RequestException as e:
        print(f"❌ An error occurred: {e}")
        return None


# ------------------------------------------------------------------------------------------------------------------------------------------------
# TEMPO 4


import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import netCDF4 as nc
import logging
from pathlib import Path
import os
import gc
import warnings
import tempfile
import shutil
import json

warnings.filterwarnings('ignore')


def collect_tempo_no2_data(hours_back=1, max_files=5):
    """
    Single function to collect and process recent TEMPO NO2 data.
    Returns JSON response only - no file downloads.
    Includes embedded authentication token.
    
    Parameters:
    -----------
    hours_back : int
        Number of hours back to search for data (default: 1)
    max_files : int
        Maximum number of files to download and process (default: 5)
    
    Returns:
    --------
    dict : JSON response with 'data', 'summary', and 'metadata'
           Returns None if collection fails
    """
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # Embedded NASA Earthdata authentication token
    token = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6Imhhc2hhYW0iLCJleHAiOjE3NjQ2MTc1NzgsImlhdCI6MTc1OTQzMzU3OCwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292IiwiaWRlbnRpdHlfcHJvdmlkZXIiOiJlZGxfb3BzIiwiYWNyIjoiZWRsIiwiYXNzdXJhbmNlX2xldmVsIjozfQ.o8_4Oc7jgv3T1ULzIp2vOico-e021Aye08tZv3zlii7iHf6PBFHRVW7H89Q2JsdbvlWsyov3TsolZwFxM9nQLTvsWwImjU17zfzFOO9mvPc-DRBNZMizSMbrntgD5lqEcNvSVT9uBhfbftAa60vhhXF9vLRS6sBEQPheUz6LZ7ulixNsYVUyfvNOVqXgNJsbT0TDwRt37N1o34touIdvth1927yDdMF5thuSifM9yBPjKUmxYocqnCCGV4o0mOvLGI-KC1QjQY3f-9LjWroF7f3RmBvQ_ngOLUKw26_Yu9Xes-loOcDTyEPhzN6iwGJG35CF7sjX3nGqKmjIstrN6w"
    
    # Use temporary directory for downloads
    output_dir = tempfile.mkdtemp(prefix="tempo_")
    
    # Setup directories
    base_dir = Path(output_dir)
    raw_dir = base_dir / 'raw'
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting TEMPO NO2 data collection (last {hours_back} hours)")
    
    # Calculate time window (accounting for processing delay)
    # TEMPO data typically has 24-48 hour delay, so search further back
    now_utc = datetime.utcnow()
    end_time = now_utc - timedelta(hours=24)  # Look 24 hours back
    start_time = end_time - timedelta(hours=hours_back * 24)  # Expand search window
    logger.info(f"Search window: {start_time} to {end_time} UTC")
    logger.info(f"Note: TEMPO data typically has 24-48h processing delay")
    
    # Search for files using NASA CMR API
    try:
        base_url = "https://cmr.earthdata.nasa.gov/search/granules.json"
        params = {
            'short_name': 'TEMPO_NO2_L2',
            'temporal': f"{start_time.strftime('%Y-%m-%dT%H:%M:%SZ')},{end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}",
            'page_size': max_files,
            'pretty': True
        }
        headers = {
            'Authorization': f'Bearer {token}',
            'User-Agent': 'TEMPO-Collector/1.0'
        }
        
        logger.info("Searching for TEMPO files...")
        response = requests.get(base_url, params=params, headers=headers, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"Search failed: HTTP {response.status_code}")
            return None
        
        entries = response.json().get('feed', {}).get('entry', [])
        logger.info(f"Found {len(entries)} files")
        
        if not entries:
            logger.warning("No files found in time range")
            logger.info("TEMPO data may not be available yet. Try:")
            logger.info("  1. Increase hours_back (try 2-7 days)")
            logger.info("  2. Check https://www.earthdata.nasa.gov/eosdis/science-system-description/eosdis-components/tempo")
            logger.info("  3. TEMPO is still in early operations - data availability varies")
            
            print(json.dumps({
                'status': 'failed',
                'message': 'No data available',
                'troubleshooting': [
                    'TEMPO is a new mission with limited data availability',
                    'Try hours_back=7 to search the last week',
                    'Check if your token is still valid',
                    'Data may only be available for certain dates'
                ]
            }, indent=2))
            return None
        
        # Extract download URLs
        download_info = []
        for i, entry in enumerate(entries[:max_files]):
            title = entry.get('title', f'tempo_{i}')
            for link in entry.get('links', []):
                if link.get('rel') == 'http://esipfed.org/ns/fedsearch/1.1/data#':
                    download_info.append({
                        'filename': title,
                        'url': link.get('href'),
                        'index': i
                    })
                    break
        
        if not download_info:
            logger.error("No download URLs found")
            return None
        
    except Exception as e:
        logger.error(f"Search failed: {e}")
        return None
    
    # Download files
    downloaded_files = []
    for info in download_info:
        try:
            filename = f"tempo_{info['index']}.nc"
            file_path = raw_dir / filename
            
            logger.info(f"Downloading {filename}...")
            response = requests.get(info['url'], headers=headers, stream=True, timeout=120)
            
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                downloaded_files.append(str(file_path))
                logger.info(f"Downloaded: {filename}")
            else:
                logger.warning(f"Download failed for {filename}: HTTP {response.status_code}")
        
        except Exception as e:
            logger.error(f"Download error for {info['filename']}: {e}")
            continue
    
    if not downloaded_files:
        logger.error("No files downloaded successfully")
        return None
    
    # Process NetCDF files
    logger.info("Processing NetCDF files...")
    all_records = []
    
    for file_path in downloaded_files:
        try:
            with nc.Dataset(file_path, 'r') as ds:
                # Find data variables
                lon_data, lat_data, no2_data = None, None, None
                groups = [ds] + list(ds.groups.values())
                
                for group in groups:
                    for lon_name in ['longitude', 'lon']:
                        if lon_name in group.variables:
                            lon_data = group.variables[lon_name][:]
                            break
                    for lat_name in ['latitude', 'lat']:
                        if lat_name in group.variables:
                            lat_data = group.variables[lat_name][:]
                            break
                    for no2_name in ['vertical_column_troposphere', 'nitrogen_dioxide_tropospheric_column']:
                        if no2_name in group.variables:
                            no2_data = group.variables[no2_name][:]
                            break
                    if lon_data is not None and lat_data is not None and no2_data is not None:
                        break
                
                if lon_data is None or lat_data is None or no2_data is None:
                    logger.warning(f"Missing variables in {file_path}")
                    continue
                
                # Reduce dimensions
                while no2_data.ndim > 2:
                    no2_data = no2_data[0]
                while lon_data.ndim > 2:
                    lon_data = lon_data[0]
                while lat_data.ndim > 2:
                    lat_data = lat_data[0]
                
                # Sample if too large
                if no2_data.size > 1000000:
                    logger.info(f"Sampling large dataset ({no2_data.size} points)")
                    if no2_data.ndim == 2:
                        h, w = no2_data.shape
                        step = max(1, int(np.sqrt(no2_data.size / 50000)))
                        no2_data = no2_data[::step, ::step]
                        lon_data = lon_data[::step, ::step]
                        lat_data = lat_data[::step, ::step]
                
                # Flatten and validate
                no2_flat = no2_data.flatten()
                lon_flat = lon_data.flatten()
                lat_flat = lat_data.flatten()
                
                valid_mask = (
                    (no2_flat > 0) & (no2_flat < 1e16) & 
                    np.isfinite(no2_flat) & np.isfinite(lon_flat) & np.isfinite(lat_flat) &
                    (lon_flat > -180) & (lon_flat < 180) & (lat_flat > -90) & (lat_flat < 90)
                )
                
                if np.any(valid_mask):
                    valid_indices = np.where(valid_mask)[0][:10000]  # Max 10k per file
                    obs_time = datetime.utcnow() - timedelta(hours=12)
                    
                    for idx in valid_indices:
                        all_records.append({
                            'no2_tropospheric_column': float(no2_flat[idx]),
                            'longitude': float(lon_flat[idx]),
                            'latitude': float(lat_flat[idx]),
                            'observation_datetime': obs_time,
                            'file_name': os.path.basename(file_path),
                            'hours_old': 12.0
                        })
                    
                    logger.info(f"Extracted {len(valid_indices)} records from {os.path.basename(file_path)}")
            
            gc.collect()
        
        except Exception as e:
            logger.error(f"Processing error for {file_path}: {e}")
            continue
    
    if not all_records:
        logger.error("No valid data extracted")
        return None
    
    # Create DataFrame
    df = pd.DataFrame(all_records)
    df['log_no2'] = np.log10(df['no2_tropospheric_column'].clip(lower=1e10))
    logger.info(f"Created DataFrame with {len(df):,} observations")
    
    # Convert DataFrame to JSON-serializable format
    # Convert datetime objects to strings
    df['observation_datetime'] = df['observation_datetime'].astype(str)
    data_list = df.to_dict('records')
    
    # Limit to reasonable size for JSON output
    if len(data_list) > 1000000:
        logger.info(f"Limiting output to first 100000 records (from {len(data_list)} total)")
        data_list = data_list[:1000000]
    
    # Create summary
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    summary = {
        'collection_timestamp': timestamp,
        'hours_back': hours_back,
        'total_observations': len(df),
        'returned_observations': len(data_list),
        'unique_files': int(df['file_name'].nunique()),
        'no2_statistics': {
            'mean': float(df['no2_tropospheric_column'].mean()),
            'median': float(df['no2_tropospheric_column'].median()),
            'std': float(df['no2_tropospheric_column'].std()),
            'min': float(df['no2_tropospheric_column'].min()),
            'max': float(df['no2_tropospheric_column'].max()),
            'unit': 'molecules/cm²'
        },
        'spatial_coverage': {
            'longitude_range': [float(df['longitude'].min()), float(df['longitude'].max())],
            'latitude_range': [float(df['latitude'].min()), float(df['latitude'].max())]
        }
    }
    
    # Cleanup temporary files
    try:
        shutil.rmtree(output_dir)
        logger.info("Cleaned up temporary files")
    except:
        pass
    
    logger.info(f"Collection complete: {len(df):,} observations from {df['file_name'].nunique()} files")
    
    result = {
        'status': 'success',
        'data': data_list,
        'summary': summary,
        'metadata': {
            'source': 'NASA TEMPO NO2 L2',
            'time_window': {
                'start': start_time.strftime('%Y-%m-%d %H:%M:%S UTC'),
                'end': end_time.strftime('%Y-%m-%d %H:%M:%S UTC')
            }
        }
    }
    
    # Print JSON response
    if result:
        print("\n" + "="*80)
        print("TEMPO NO2 DATA - JSON RESPONSE")
        print("="*80)
        print(json.dumps(result, indent=2))
    
    return result


# ----------------------------------------------------------------------------------------------------------------------------------------

import os
import json
import logging
import requests
import xarray as xr
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_merra2_met_data():
    """
    Main function that fetches MERRA-2 meteorological variables for North America
    and returns as JSON. All logic is contained within this single function.
    """
    # Configuration
    token = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6Imhhc2hhYW0iLCJleHAiOjE3NjQ2MTc1NzgsImlhdCI6MTc1OTQzMzU3OCwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292IiwiaWRlbnRpdHlfcHJvdmlkZXIiOiJlZGxfb3BzIiwiYWNyIjoiZWRsIiwiYXNzdXJhbmNlX2xldmVsIjozfQ.o8_4Oc7jgv3T1ULzIp2vOico-e021Aye08tZv3zlii7iHf6PBFHRVW7H89Q2JsdbvlWsyov3TsolZwFxM9nQLTvsWwImjU17zfzFOO9mvPc-DRBNZMizSMbrntgD5lqEcNvSVT9uBhfbftAa60vhhXF9vLRS6sBEQPheUz6LZ7ulixNsYVUyfvNOVqXgNJsbT0TDwRt37N1o34touIdvth1927yDdMF5thuSifM9yBPjKUmxYocqnCCGV4o0mOvLGI-KC1QjQY3f-9LjWroF7f3RmBvQ_ngOLUKw26_Yu9Xes-loOcDTyEPhzN6iwGJG35CF7sjX3nGqKmjIstrN6w"
    days_back = 120
    output_dir = "./data_downloads"
    collection_type = "hourly"
    max_granules = 100
    print_json = True
    max_print_records = 5000000
    
    logger.info("=" * 60)
    logger.info("STARTING MERRA-2 METEOROLOGICAL DATA COLLECTION (JSON MODE)")
    logger.info("=" * 60)

    NORTH_AMERICA_BBOX = {'min_lat': 15.0, 'max_lat': 72.0, 'min_lon': -168.0, 'max_lon': -52.0}
    MERRA2_COLLECTIONS = {
        'hourly': {
            'id': 'C1276812863-GES_DISC',
            'name': 'M2T1NXSLV: MERRA-2 Single-Level Diagnostics (Hourly)',
            'short_name': 'M2T1NXSLV',
            'version': '5.12.4',
            'vars': ['T2M', 'QV2M', 'U10M', 'V10M', 'PS', 'SLP']
        }
    }

    # Setup directories
    base_dir = Path(output_dir) / "merra2_met"
    raw_dir = base_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Search for newest granule(s)
    logger.info(f"Step 1: Searching for MERRA-2 {collection_type} data...")
    collection = MERRA2_COLLECTIONS.get(collection_type)
    if not collection:
        logger.error(f"Collection type '{collection_type}' not supported")
        return None

    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)
    
    cmr_url = "https://cmr.earthdata.nasa.gov/search/granules.json"
    params = {
        'collection_concept_id': collection['id'],
        'temporal': f"{start_date.strftime('%Y-%m-%dT%H:%M:%SZ')},{end_date.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        'page_size': max_granules,
        'sort_key': '-start_date'
    }
    
    try:
        r = requests.get(cmr_url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        granules = data.get('feed', {}).get('entry', []) or []
    except Exception as e:
        logger.error(f"Search error: {e}")
        return None

    if not granules:
        logger.error("No granules found")
        return None

    granule = granules[0]
    logger.info(f"Found granule: {granule.get('title', 'Unknown')}")

    # Extract download URL
    download_url = None
    links = granule.get('links', []) or []
    for link in links:
        if link.get('rel') == 'http://esipfed.org/ns/fedsearch/1.1/data#':
            href = link.get('href', '')
            if '/data/' in href and 'search.earthdata.nasa.gov' not in href:
                download_url = href
                break
    
    if not download_url:
        for link in links:
            href = (link.get('href') or '').lower()
            if 'opendap' in href and href.endswith('.nc4.html'):
                download_url = link['href'].replace('/opendap/', '/data/').replace('.html', '')
                break
    
    if not download_url:
        logger.error("No download URL found")
        return None

    # Step 2: Download NetCDF file
    logger.info("Step 2: Downloading NetCDF file...")
    title = granule.get('title', '') or download_url.split('/')[-1]
    nc_name = title.split(':')[-1].strip() if '.nc4' in title else download_url.split('/')[-1]
    nc_path = raw_dir / nc_name

    if not nc_path.exists():
        sess = requests.Session()
        sess.headers.update({'Authorization': f'Bearer {token}', 'User-Agent': 'MERRA2-Fetcher/1.0'})
        try:
            with sess.get(download_url, stream=True, timeout=300) as resp:
                resp.raise_for_status()
                if 'text/html' in resp.headers.get('content-type', ''):
                    logger.error("Received HTML instead of NetCDF file")
                    return None

                total_size = int(resp.headers.get('content-length', 0))
                logger.info(f"Downloading {total_size / (1024*1024):.1f} MB...")

                downloaded = 0
                with open(nc_path, 'wb') as f:
                    for chunk in resp.iter_content(8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0 and downloaded % (50*1024*1024) < 8192:
                                logger.info(f"Progress: {downloaded/(1024*1024):.0f}/{total_size/(1024*1024):.0f} MB")

            logger.info(f"Download complete: {os.path.getsize(nc_path)/(1024*1024):.1f} MB")
            
            if os.path.getsize(nc_path) < 100_000:
                logger.error("Downloaded file is too small")
                return None
                
        except Exception as e:
            logger.error(f"Download error: {e}")
            return None
            
        logger.info(f"Downloaded: {nc_name}")
    else:
        logger.info(f"Using cached file: {nc_name}")

    # Step 3: Process NetCDF to JSON records
    logger.info("Step 3: Processing NetCDF file (this may take time)...")
    try:
        ds = xr.open_dataset(nc_path, engine='h5netcdf')
        wanted = [v for v in collection['vars'] if v in ds.data_vars]
        if not wanted:
            logger.error("No requested variables found in dataset")
            ds.close()
            return None

        logger.info(f"Found variables: {', '.join(wanted)}")
        logger.info("Step 4: Subsetting to North America...")

        lats = ds['lat'].values
        lons = ds['lon'].values
        lat_mask = (lats >= NORTH_AMERICA_BBOX['min_lat']) & (lats <= NORTH_AMERICA_BBOX['max_lat'])
        lon_mask = (lons >= NORTH_AMERICA_BBOX['min_lon']) & (lons <= NORTH_AMERICA_BBOX['max_lon'])
        ds_na = ds.sel(lat=lats[lat_mask], lon=lons[lon_mask])

        # Convert to records (with sampling for speed)
        logger.info("Step 5: Converting to JSON records (sampling for speed)...")
        records = []
        max_records_per_var = 1000  # Limit records per variable

        for var in wanted:
            dfv = ds_na[var].to_dataframe().reset_index()
            dfv = dfv.dropna(subset=[var])

            # Sample if too large
            if len(dfv) > max_records_per_var:
                logger.info(f"  {var}: sampling {max_records_per_var} from {len(dfv)} points")
                dfv = dfv.sample(n=max_records_per_var, random_state=42)
            else:
                logger.info(f"  {var}: using all {len(dfv)} points")

            for _, row in dfv.iterrows():
                records.append({
                    'timestamp': row['time'].isoformat() if hasattr(row['time'], 'isoformat') else str(row['time']),
                    'latitude': float(row['lat']),
                    'longitude': float(row['lon']),
                    'variable': var,
                    'value': float(row[var]),
                    'data_source': 'MERRA-2',
                    'collection': collection['short_name'],
                    'version': collection['version']
                })

        ds.close()

        if not records:
            logger.error("No valid records after processing")
            return None

        # Build summary
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        var_counts = {}
        for r in records:
            v = r['variable']
            var_counts[v] = var_counts.get(v, 0) + 1

        summary = {
            'collection': collection['name'],
            'short_name': collection['short_name'],
            'version': collection['version'],
            'variables': wanted,
            'bbox': NORTH_AMERICA_BBOX,
            'granule_time_start': granule.get('time_start'),
            'granule_time_end': granule.get('time_end'),
            'total_records': len(records),
            'records_per_variable': var_counts,
            'collection_timestamp': timestamp
        }

        payload = {"summary": summary, "records": records}

        # Optional: pretty-print JSON to terminal
        if print_json:
            preview = {
                "summary": summary,
                "records_preview_count": min(len(records), max_print_records),
                "records_preview": records[:max_print_records],
                "note": f"Showing first {min(len(records), max_print_records)} of {len(records)} records"
                        if len(records) > max_print_records else "Showing all records"
            }
            print(json.dumps(preview, indent=2))

        logger.info("=" * 60)
        logger.info(f"SUCCESS! Collected {len(records):,} meteorological records")
        logger.info(f"Variables: {', '.join(wanted)}")
        logger.info("=" * 60)

        if payload:
            logger.info(f"✓ Data collection complete! Total records: {payload['summary']['total_records']}")
        
        return payload

    except Exception as e:
        logger.error(f"Processing error: {e}")
        return None


# --------------------------------------------------------------------------------------------------------


import os
import json
import time
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path


def fetch_all_air_quality_data():
    """
    Complete unified function to fetch North American air quality data from 
    PurpleAir, EPA AQS, and OpenWeatherMap and return as JSON.
    
    This function contains all logic including configuration, data fetching,
    and result display.
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("STARTING AIR QUALITY DATA COLLECTION (JSON MODE)")
    logger.info("=" * 60)
    
    # API Configuration
    epa_email = "studyhashaam@gmail.com"
    epa_key = "sandhare35"
    purpleair_key = "2FAB1995-A207-11F0-BDE5-4201AC1DC121"
    owm_key = "fea5f36d1c3d7e577704ff566939e070"
    
    # Configuration parameters
    output_dir = "./data_downloads"
    purpleair_limit = None
    epa_states = [
    '01','02','04','05','06','08','09','10','11','12','13','15','16','17','18','19',
    '20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35',
    '36','37','38','39','40','41','42','44','45','46','47','48','49','50','51','53',
    '54','55','56'
    ]  # CA, NY, TX, FL, IL
    epa_parameters = ['pm25', 'pm10', 'no2', 'o3']
    owm_cities = [
    # ---------------- United States (state capitals + key cities) ----------------
    {'name': 'Montgomery', 'lat': 32.3792, 'lon': -86.3077, 'country': 'US'},  # Alabama
    {'name': 'Juneau', 'lat': 58.3019, 'lon': -134.4197, 'country': 'US'},     # Alaska (cap)
    {'name': 'Anchorage', 'lat': 61.2181, 'lon': -149.9003, 'country': 'US'},  # Alaska (largest city)
    {'name': 'Phoenix', 'lat': 33.4484, 'lon': -112.0740, 'country': 'US'},    # Arizona
    {'name': 'Little Rock', 'lat': 34.7465, 'lon': -92.2896, 'country': 'US'}, # Arkansas
    {'name': 'Sacramento', 'lat': 38.5816, 'lon': -121.4944, 'country': 'US'}, # California (cap)
    {'name': 'Los Angeles', 'lat': 34.0522, 'lon': -118.2437, 'country': 'US'},# California (largest)
    {'name': 'Denver', 'lat': 39.7392, 'lon': -104.9903, 'country': 'US'},     # Colorado
    {'name': 'Hartford', 'lat': 41.7658, 'lon': -72.6734, 'country': 'US'},    # Connecticut
    {'name': 'Dover', 'lat': 39.1582, 'lon': -75.5244, 'country': 'US'},       # Delaware (cap)
    {'name': 'Miami', 'lat': 25.7617, 'lon': -80.1918, 'country': 'US'},       # Florida (largest metro)
    {'name': 'Tallahassee', 'lat': 30.4383, 'lon': -84.2807, 'country': 'US'}, # Florida (cap)
    {'name': 'Atlanta', 'lat': 33.7490, 'lon': -84.3880, 'country': 'US'},     # Georgia
    {'name': 'Honolulu', 'lat': 21.3069, 'lon': -157.8583, 'country': 'US'},   # Hawaii
    {'name': 'Boise', 'lat': 43.6150, 'lon': -116.2023, 'country': 'US'},      # Idaho
    {'name': 'Springfield', 'lat': 39.7980, 'lon': -89.6440, 'country': 'US'}, # Illinois (cap)
    {'name': 'Chicago', 'lat': 41.8781, 'lon': -87.6298, 'country': 'US'},     # Illinois (largest)
    {'name': 'Indianapolis', 'lat': 39.7684, 'lon': -86.1581, 'country': 'US'},# Indiana
    {'name': 'Des Moines', 'lat': 41.5868, 'lon': -93.6250, 'country': 'US'},  # Iowa
    {'name': 'Topeka', 'lat': 39.0558, 'lon': -95.6890, 'country': 'US'},      # Kansas (cap)
    {'name': 'Louisville', 'lat': 38.2527, 'lon': -85.7585, 'country': 'US'},  # Kentucky
    {'name': 'Baton Rouge', 'lat': 30.4515, 'lon': -91.1871, 'country': 'US'}, # Louisiana (cap)
    {'name': 'New Orleans', 'lat': 29.9511, 'lon': -90.0715, 'country': 'US'}, # Louisiana (largest)
    {'name': 'Augusta', 'lat': 44.3106, 'lon': -69.7795, 'country': 'US'},     # Maine (cap)
    {'name': 'Portland', 'lat': 43.6591, 'lon': -70.2568, 'country': 'US'},    # Maine (largest)
    {'name': 'Baltimore', 'lat': 39.2904, 'lon': -76.6122, 'country': 'US'},   # Maryland
    {'name': 'Boston', 'lat': 42.3601, 'lon': -71.0589, 'country': 'US'},      # Massachusetts
    {'name': 'Lansing', 'lat': 42.7325, 'lon': -84.5555, 'country': 'US'},     # Michigan (cap)
    {'name': 'Detroit', 'lat': 42.3314, 'lon': -83.0458, 'country': 'US'},     # Michigan (largest)
    {'name': 'St. Paul', 'lat': 44.9537, 'lon': -93.0900, 'country': 'US'},    # Minnesota (cap)
    {'name': 'Minneapolis', 'lat': 44.9778, 'lon': -93.2650, 'country': 'US'}, # Minnesota (twin city)
    {'name': 'Jackson', 'lat': 32.2988, 'lon': -90.1848, 'country': 'US'},     # Mississippi
    {'name': 'Jefferson City', 'lat': 38.5767, 'lon': -92.1735, 'country': 'US'}, # Missouri (cap)
    {'name': 'Kansas City', 'lat': 39.0997, 'lon': -94.5786, 'country': 'US'}, # Missouri (largest metro)
    {'name': 'Helena', 'lat': 46.5891, 'lon': -112.0391, 'country': 'US'},     # Montana (cap)
    {'name': 'Billings', 'lat': 45.7833, 'lon': -108.5007, 'country': 'US'},   # Montana (largest)
    # (…similar for the rest of U.S. states)

    # ---------------- Canada (provincial/territorial capitals) ----------------
    {'name': 'Toronto', 'lat': 43.6511, 'lon': -79.3470, 'country': 'CA'},     # Ontario
    {'name': 'Ottawa', 'lat': 45.4215, 'lon': -75.6972, 'country': 'CA'},      # Federal capital
    {'name': 'Vancouver', 'lat': 49.2827, 'lon': -123.1207, 'country': 'CA'},  # BC (largest)
    {'name': 'Victoria', 'lat': 48.4284, 'lon': -123.3656, 'country': 'CA'},   # BC (cap)
    {'name': 'Montreal', 'lat': 45.5017, 'lon': -73.5673, 'country': 'CA'},    # Quebec (largest)
    {'name': 'Quebec City', 'lat': 46.8139, 'lon': -71.2080, 'country': 'CA'}, # Quebec (cap)
    {'name': 'Calgary', 'lat': 51.0447, 'lon': -114.0719, 'country': 'CA'},    # Alberta
    {'name': 'Edmonton', 'lat': 53.5461, 'lon': -113.4938, 'country': 'CA'},   # Alberta (cap)
    {'name': 'Winnipeg', 'lat': 49.8951, 'lon': -97.1384, 'country': 'CA'},    # Manitoba
    {'name': 'Regina', 'lat': 50.4452, 'lon': -104.6189, 'country': 'CA'},     # Saskatchewan
    {'name': 'Saskatoon', 'lat': 52.1332, 'lon': -106.6700, 'country': 'CA'},  # Saskatchewan (largest)
    {'name': 'Halifax', 'lat': 44.6488, 'lon': -63.5752, 'country': 'CA'},     # Nova Scotia
    {'name': 'St. John\'s', 'lat': 47.5615, 'lon': -52.7126, 'country': 'CA'}, # Newfoundland
    {'name': 'Charlottetown', 'lat': 46.2382, 'lon': -63.1311, 'country': 'CA'}, # PEI
    {'name': 'Fredericton', 'lat': 45.9636, 'lon': -66.6431, 'country': 'CA'}, # New Brunswick
    {'name': 'Iqaluit', 'lat': 63.7467, 'lon': -68.5169, 'country': 'CA'},     # Nunavut
    {'name': 'Whitehorse', 'lat': 60.7212, 'lon': -135.0568, 'country': 'CA'}, # Yukon
    {'name': 'Yellowknife', 'lat': 62.4540, 'lon': -114.3718, 'country': 'CA'},# NWT

    # ---------------- Mexico (major cities & state capitals) ----------------
    {'name': 'Mexico City', 'lat': 19.4326, 'lon': -99.1332, 'country': 'MX'},
    {'name': 'Guadalajara', 'lat': 20.6597, 'lon': -103.3496, 'country': 'MX'},
    {'name': 'Monterrey', 'lat': 25.6866, 'lon': -100.3161, 'country': 'MX'},
    {'name': 'Cancún', 'lat': 21.1619, 'lon': -86.8515, 'country': 'MX'},
    {'name': 'Tijuana', 'lat': 32.5149, 'lon': -117.0382, 'country': 'MX'},
    {'name': 'Mérida', 'lat': 20.9674, 'lon': -89.5926, 'country': 'MX'},      # Yucatán
    {'name': 'Puebla', 'lat': 19.0414, 'lon': -98.2063, 'country': 'MX'},
    {'name': 'Oaxaca', 'lat': 17.0732, 'lon': -96.7266, 'country': 'MX'},
    {'name': 'Chihuahua', 'lat': 28.6320, 'lon': -106.0691, 'country': 'MX'},
    {'name': 'León', 'lat': 21.1250, 'lon': -101.6850, 'country': 'MX'},

    # ---------------- Central America & Caribbean ----------------
    {'name': 'Havana', 'lat': 23.1136, 'lon': -82.3666, 'country': 'CU'},      # Cuba
    {'name': 'San Juan', 'lat': 18.4655, 'lon': -66.1057, 'country': 'PR'},    # Puerto Rico
    {'name': 'Port-au-Prince', 'lat': 18.5944, 'lon': -72.3074, 'country': 'HT'}, # Haiti
    {'name': 'Santo Domingo', 'lat': 18.4861, 'lon': -69.9312, 'country': 'DO'},# Dominican Republic
    {'name': 'Kingston', 'lat': 17.9712, 'lon': -76.7920, 'country': 'JM'},    # Jamaica
    {'name': 'San José', 'lat': 9.9281, 'lon': -84.0907, 'country': 'CR'},     # Costa Rica
    {'name': 'Panama City', 'lat': 8.9824, 'lon': -79.5199, 'country': 'PA'},  # Panama
    {'name': 'Guatemala City', 'lat': 14.6349, 'lon': -90.5069, 'country': 'GT'},
    {'name': 'San Salvador', 'lat': 13.6929, 'lon': -89.2182, 'country': 'SV'},
    {'name': 'Tegucigalpa', 'lat': 14.0723, 'lon': -87.1921, 'country': 'HN'},
    {'name': 'Managua', 'lat': 12.11499, 'lon': -86.2362, 'country': 'NI'},
    {'name': 'Belmopan', 'lat': 17.2510, 'lon': -88.7590, 'country': 'BZ'},
]

    print_json = True
    max_print_records = 5000000

    # Setup
    session = requests.Session()
    session.headers.update({'User-Agent': 'AQ-Composite-Fetcher/1.0'})
    base_dir = Path(output_dir) / "air_quality"
    base_dir.mkdir(parents=True, exist_ok=True)

    all_records = []

    # 1) PurpleAir (Real-time)
    logger.info("Step 1: Fetching PurpleAir data...")
    try:
        url = "https://api.purpleair.com/v1/sensors"
        headers = {'X-API-Key': purpleair_key}
        params = {
            'nwlng': -168.0, 'nwlat': 72.0, 'selng': -52.0, 'selat': 15.0,
            'max_age': 3600,
            'fields': 'sensor_index,name,latitude,longitude,pm1.0,pm2.5,pm10.0,last_seen,location_type'
        }
        r = session.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 200:
            payload = r.json()
            sensors, fields = payload.get('data', []), payload.get('fields', [])
            idx_map = {f: i for i, f in enumerate(fields)}
            def get(row, key):
                i = idx_map.get(key)
                return row[i] if (i is not None and i < len(row)) else None

            for row in sensors:
                last_seen = get(row, 'last_seen') or time.time()
                base = {
                    'datetime_utc': datetime.utcfromtimestamp(last_seen).isoformat() + 'Z',
                    'datetime_local': '',
                    'latitude': get(row, 'latitude'),
                    'longitude': get(row, 'longitude'),
                    'location_id': f"PA_{get(row,'sensor_index')}",
                    'location_name': get(row, 'name') or 'PurpleAir Sensor',
                    'city': '', 'state': '', 'country': '',
                    'sensor_id': str(get(row, 'sensor_index')),
                    'provider': 'PurpleAir',
                    'data_source': 'PurpleAir'
                }
                for pa_key, pname in [('pm2.5', 'PM2.5'), ('pm10.0', 'PM10'), ('pm1.0', 'PM1.0')]:
                    val = get(row, pa_key)
                    if val is not None:
                        rec = base.copy()
                        rec.update({
                            'value': float(val),
                            'parameter_name': pname,
                            'parameter_display_name': {'PM2.5':'PM2.5 (Fine PM)','PM10':'PM10 (Coarse PM)','PM1.0':'PM1.0 (Ultra-fine PM)'}[pname],
                            'units': 'µg/m³'
                        })
                        all_records.append(rec)
            if purpleair_limit and len(all_records) > purpleair_limit:
                all_records = all_records[:purpleair_limit]
            logger.info(f"  Collected {len(all_records)} PurpleAir records")
    except Exception as e:
        logger.warning(f"  PurpleAir error: {e}")

    # 2) OpenWeatherMap (Cities)
    logger.info("Step 2: Fetching OpenWeatherMap data...")
    owm_count = 0
    try:
        base_url = "http://api.openweathermap.org/data/2.5/air_pollution"
        country_map = {'US': 'United States', 'CA': 'Canada', 'MX': 'Mexico'}
        for city in owm_cities:
            params = {'lat': city['lat'], 'lon': city['lon'], 'appid': owm_key}
            r = session.get(base_url, params=params, timeout=20)
            if r.status_code == 200:
                data = r.json()
                for reading in data.get('list', []):
                    comps = reading.get('components', {})
                    ts = reading.get('dt', time.time())
                    for pollutant, value in comps.items():
                        if value is None:
                            continue
                        pname = pollutant.upper()
                        if pollutant == 'pm2_5': pname = 'PM2.5'
                        elif pollutant == 'pm10': pname = 'PM10'
                        rec = {
                            'datetime_utc': datetime.utcfromtimestamp(ts).isoformat() + 'Z',
                            'datetime_local': '',
                            'value': float(value),
                            'latitude': city['lat'],
                            'longitude': city['lon'],
                            'location_id': f"OWM_{city['name'].replace(' ','_')}",
                            'location_name': city['name'],
                            'city': city['name'],
                            'state': '',
                            'country': country_map.get(city['country'], ''),
                            'parameter_name': pname,
                            'parameter_display_name': pname,
                            'units': 'µg/m³',
                            'sensor_id': f"OWM_{city['name']}_{pollutant}",
                            'provider': 'OpenWeatherMap',
                            'data_source': 'OpenWeatherMap'
                        }
                        all_records.append(rec)
                        owm_count += 1
            time.sleep(0.1)
        logger.info(f"  Collected {owm_count} OpenWeatherMap records")
    except Exception as e:
        logger.warning(f"  OpenWeatherMap error: {e}")

    # 3) EPA AQS (By State/Param)
    logger.info("Step 3: Fetching EPA AQS data...")
    epa_count = 0
    try:
        aqs_url = "https://aqs.epa.gov/data/api/sampleData/byState"
        today = datetime.utcnow()
        yesterday = today - timedelta(days=1)
        param_codes = {
            'pm25':'88101','pm10':'81102','no2':'42602',
            'o3':'44201','so2':'42401','co':'42101'
        }
        for p in epa_parameters:
            code = param_codes.get(p, '88101')
            for st in epa_states:
                params = {
                    'email': epa_email, 'key': epa_key, 'param': code,
                    'bdate': yesterday.strftime('%Y%m%d'),
                    'edate': today.strftime('%Y%m%d'),
                    'state': st
                }
                r = session.get(aqs_url, params=params, timeout=90)
                if r.status_code == 200:
                    data = r.json()
                    for rec in data.get('Data', []):
                        out = {
                            'datetime_utc': rec.get('date_gmt',''),
                            'datetime_local': f"{rec.get('date_local','')} {rec.get('time_local','')}".strip(),
                            'value': rec.get('sample_measurement'),
                            'latitude': rec.get('latitude'),
                            'longitude': rec.get('longitude'),
                            'location_id': f"AQS_{rec.get('site_number','')}",
                            'location_name': rec.get('local_site_name',''),
                            'city': rec.get('city_name',''),
                            'state': rec.get('state_name',''),
                            'country': 'United States',
                            'parameter_name': p.upper(),
                            'parameter_display_name': rec.get('parameter_name',''),
                            'units': rec.get('units_of_measure',''),
                            'sensor_id': f"AQS_{rec.get('monitor','')}",
                            'provider': 'EPA AQS',
                            'data_source': 'EPA AQS'
                        }
                        all_records.append(out)
                        epa_count += 1
                time.sleep(0.4)
        logger.info(f"  Collected {epa_count} EPA AQS records")
    except Exception as e:
        logger.warning(f"  EPA AQS error: {e}")

    if not all_records:
        logger.error("No data collected from any sources")
        return None

    # Build summary
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    sources = {}
    parameters = {}
    for rec in all_records:
        src = rec.get('data_source', 'Unknown')
        sources[src] = sources.get(src, 0) + 1
        param = rec.get('parameter_name', 'Unknown')
        parameters[param] = parameters.get(param, 0) + 1

    summary = {
        'collection_timestamp': timestamp,
        'total_records': len(all_records),
        'sources': sources,
        'parameters': parameters,
        'data_sources': list(sources.keys())
    }

    payload = {"summary": summary, "records": all_records}

    # Optional: pretty-print JSON to terminal
    if print_json:
        preview = {
            "summary": summary,
            "records_preview_count": min(len(all_records), max_print_records),
            "records_preview": all_records[:max_print_records],
            "note": f"Showing first {min(len(all_records), max_print_records)} of {len(all_records)} records"
                    if len(all_records) > max_print_records else "Showing all records"
        }
        print(json.dumps(preview, indent=2))

    logger.info("=" * 60)
    logger.info(f"SUCCESS! Collected {len(all_records):,} air quality records")
    logger.info(f"Sources: {', '.join(sources.keys())}")
    logger.info("=" * 60)

    # Display final results
    if payload:
        print()
        print(f"✓ Data collection complete!")
        print(f"✓ Total records: {payload['summary']['total_records']}")
        print(f"✓ Parameters: {', '.join(payload['summary']['parameters'].keys())}")
    else:
        print("✗ Failed to fetch air quality data")

    return payload






# --------------------------------------------------------------------------------------------------

import os
import json
import re
import time
import bz2
import gzip
import logging
import requests
from pathlib import Path
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_and_process_pandora_hcho_data(max_sites=5, max_files_per_site=2, print_json=True, max_print_records=50):
    """
    Complete function to fetch Pandora HCHO data, process it, and return as JSON.
    This single function handles all operations from fetching to formatting.

    Args:
        max_sites (int): Maximum number of sites to check
        max_files_per_site (int): Maximum files to download per site
        print_json (bool): If True, pretty-prints JSON to terminal
        max_print_records (int): Max records to print to terminal

    Returns:
        dict: {'summary': {...}, 'records': [...]} or None if failed
    """
    logger.info("=" * 60)
    logger.info("STARTING PANDORA HCHO DATA COLLECTION (JSON MODE)")
    logger.info("=" * 60)
    logger.info("Pandora HCHO Data Fetcher")
    logger.info("Fetching formaldehyde measurements from Pandonia network")
    print()

    # Define North American Pandora sites
    sites = [
        {'site': 'GreenbeltMD', 'location': 'Greenbelt, MD', 'lat': 38.9922, 'lon': -76.8405},
        {'site': 'BeltsvilleMD', 'location': 'Beltsville, MD', 'lat': 39.0347, 'lon': -76.8778},
        {'site': 'BoulderCO', 'location': 'Boulder, CO', 'lat': 40.0394, 'lon': -105.2469},
        {'site': 'LosAngelesCA', 'location': 'Los Angeles, CA', 'lat': 34.0522, 'lon': -118.2437},
        {'site': 'Toronto-CNTower', 'location': 'Toronto CN Tower, Canada', 'lat': 43.7806, 'lon': -79.4680},
        {'site': 'ChicagoIL', 'location': 'Chicago, IL', 'lat': 41.8781, 'lon': -87.6298},
        {'site': 'HoustonTX', 'location': 'Houston, TX', 'lat': 29.7604, 'lon': -95.3698},
        {'site': 'NewYorkNY', 'location': 'New York, NY', 'lat': 40.7128, 'lon': -74.0060},
        {'site': 'SeattleWA', 'location': 'Seattle, WA', 'lat': 47.6062, 'lon': -122.3321},
        {'site': 'MexicoCityMX', 'location': 'Mexico City, Mexico', 'lat': 19.4326, 'lon': -99.1332}
    ]

    base_url = "https://data.pandonia-global-network.org"
    headers = {'User-Agent': 'NASA-Pandonia-Fetcher/1.0'}

    # Fetch data from all sites
    logger.info(f"Fetching data from {min(max_sites, len(sites))} Pandora sites...")
    all_records = []
    successful_sites = 0

    for idx, site_info in enumerate(sites[:max_sites]):
        site = site_info['site']
        logger.info(f"[{idx+1}/{min(max_sites, len(sites))}] Processing: {site_info['location']}")

        try:
            # Get site files
            site_url = f"{base_url}/{site}/"
            response = requests.get(site_url, headers=headers, timeout=30)

            if response.status_code != 200:
                logger.info(f"  No data available")
                continue

            # Find Pandora directories
            href_pattern = r'href="([^"]*)"'
            all_hrefs = re.findall(href_pattern, response.text)
            pandora_dirs = [h for h in all_hrefs if 'pandora' in h.lower() and h.endswith('/')]

            site_records = []

            for pandora_dir in pandora_dirs[:2]:  # Check first 2 instruments
                clean_pandora = pandora_dir.strip('./').rstrip('/')
                pandora_url = f"{site_url}{clean_pandora}/"

                try:
                    pandora_response = requests.get(pandora_url, headers=headers, timeout=30)
                    if pandora_response.status_code != 200:
                        continue

                    # Find L2 data directories
                    pandora_hrefs = re.findall(href_pattern, pandora_response.text)
                    level_dirs = [h for h in pandora_hrefs if h.startswith('./L2')]

                    for level_dir in level_dirs[:1]:  # Check top level
                        clean_level = level_dir.strip('./').rstrip('/')
                        level_url = f"{pandora_url}{clean_level}/"

                        try:
                            level_response = requests.get(level_url, headers=headers, timeout=30)
                            if level_response.status_code != 200:
                                continue

                            # Find data files
                            level_hrefs = re.findall(href_pattern, level_response.text)
                            data_files = [h for h in level_hrefs if h.startswith('./Pandora') and len(h) > 15]

                            # Sort by date
                            data_files.sort(key=lambda x: re.search(r'(\d{8})', x).group(1) if re.search(r'(\d{8})', x) else '0', reverse=True)

                            # Download and parse files
                            for file_href in data_files[:max_files_per_site]:
                                file_url = f"{level_url}{file_href.strip('./')}"
                                filename = file_href.strip('./')

                                try:
                                    file_response = requests.get(file_url, headers=headers, timeout=90)
                                    if file_response.status_code != 200:
                                        continue

                                    # Decompress if needed
                                    content = file_response.content
                                    if filename.endswith('.bz2'):
                                        content = bz2.decompress(content).decode('utf-8', errors='ignore')
                                    elif filename.endswith('.gz'):
                                        content = gzip.decompress(content).decode('utf-8', errors='ignore')
                                    else:
                                        content = content.decode('utf-8', errors='ignore')

                                    # Parse content
                                    lines = content.strip().split('\n')
                                    data_start = 0
                                    columns = []

                                    # Find data section
                                    for i, line in enumerate(lines):
                                        if line.startswith('-') and len(line) > 10:
                                            # Generate column names
                                            columns = [
                                                'utc_datetime', 'fractional_day', 'solar_zenith_angle',
                                                'solar_azimuth_angle', 'elevation_angle', 'azimuth_angle',
                                                'hcho_slant_column', 'hcho_slant_column_error', 'hcho_vertical_column',
                                                'hcho_vertical_column_error', 'air_mass_factor', 'fitting_rms',
                                                'temperature', 'quality_flag', 'processing_code'
                                            ]

                                            # Find actual data start
                                            j = i + 1
                                            while j < len(lines) and (lines[j].strip().startswith('Column') or not lines[j].strip()):
                                                j += 1
                                            data_start = j
                                            break

                                    if not columns:
                                        continue

                                    # Parse data rows
                                    parsed = 0
                                    for line in lines[data_start:]:
                                        line = line.strip()
                                        if not line or line.startswith('#') or line.startswith('*'):
                                            continue

                                        values = line.split()
                                        if len(values) < 3:
                                            continue

                                        # Check if looks like data
                                        numeric_count = sum(1 for v in values if v.replace('.','').replace('-','').replace('e','').replace('E','').isdigit())
                                        if numeric_count < len(values) * 0.6:
                                            continue

                                        row = {}
                                        for j, col in enumerate(columns):
                                            if j < len(values):
                                                try:
                                                    val = values[j]
                                                    if '.' in val or 'e' in val.lower():
                                                        row[col] = float(val)
                                                    else:
                                                        row[col] = int(val)
                                                except:
                                                    row[col] = values[j]
                                            else:
                                                row[col] = None

                                        row['site'] = site
                                        row['location'] = site_info['location']
                                        row['latitude'] = site_info['lat']
                                        row['longitude'] = site_info['lon']
                                        row['instrument'] = clean_pandora
                                        row['data_level'] = clean_level
                                        row['source_file'] = filename

                                        site_records.append(row)
                                        parsed += 1

                                        if parsed >= 50000:  # Limit records per file
                                            break

                                    if parsed > 0:
                                        logger.info(f"  Parsed {parsed} records from {filename}")

                                    time.sleep(0.2)

                                except Exception:
                                    continue

                            time.sleep(0.3)
                        except Exception:
                            continue

                    time.sleep(0.3)
                except Exception:
                    continue

            if site_records:
                all_records.extend(site_records)
                successful_sites += 1
                logger.info(f"  SUCCESS: {len(site_records)} records collected")
            else:
                logger.info(f"  No parseable data found")

            time.sleep(1)  # Rate limiting

        except Exception as e:
            logger.warning(f"  Error: {e}")

    if not all_records:
        logger.error("No data collected from any sites")
        return None

    # Build summary
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    unique_sites = len(set(r['site'] for r in all_records))

    # Calculate statistics on HCHO vertical column if available
    hcho_values = [r.get('hcho_vertical_column') for r in all_records
                   if r.get('hcho_vertical_column') is not None and isinstance(r.get('hcho_vertical_column'), (int, float))]

    summary = {
        'variable': 'HCHO',
        'collection_timestamp': timestamp,
        'total_observations': len(all_records),
        'unique_sites': unique_sites,
        'sites_successful': successful_sites,
        'sites_attempted': min(max_sites, len(sites)),
        'data_source': 'Pandonia Global Network'
    }

    if hcho_values:
        import statistics
        summary['statistics'] = {
            'mean': statistics.mean(hcho_values),
            'median': statistics.median(hcho_values),
            'min': min(hcho_values),
            'max': max(hcho_values)
        }

    payload = {"summary": summary, "records": all_records}

    # Optional: pretty-print JSON to terminal
    if print_json:
        preview = {
            "summary": summary,
            "records_preview_count": min(len(all_records), max_print_records),
            "records_preview": all_records[:max_print_records],
            "note": f"Showing first {min(len(all_records), max_print_records)} of {len(all_records)} records"
                    if len(all_records) > max_print_records else "Showing all records"
        }
        print(json.dumps(preview, indent=2))

    logger.info("=" * 60)
    logger.info(f"SUCCESS! Collected {len(all_records):,} HCHO observations")
    logger.info(f"Sites with data: {unique_sites}")
    logger.info(f"Success rate: {(successful_sites/min(max_sites, len(sites))*100):.1f}%")
    logger.info("=" * 60)

    # Final output
    print()
    logger.info("Data collection complete!")
    logger.info(f"Total observations: {payload['summary']['total_observations']}")
    logger.info(f"Unique sites: {payload['summary']['unique_sites']}")

    def main():
        """
        Main entry point - calls the fetch_and_process_pandora_hcho_data function.
        """
        fetch_and_process_pandora_hcho_data()

    return payload






# -------------------------------------------------------------------------------------------------------------------------------------------


import os
import json
import logging
import requests
import xarray as xr
from pathlib import Path
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_pblh_data(days_back=120, output_dir="./data_downloads",
                    print_json=True, max_print_records=10):
    """
    Fetch latest MERRA-2 Planetary Boundary Layer Height (PBLH) for North America
    and return as JSON.

    Args:
        days_back (int): How many days back to search (MERRA-2 is delayed ~2–3 months)
        output_dir (str): Base directory for cache files
        print_json (bool): If True, pretty-prints JSON to terminal
        max_print_records (int): Max records to print to terminal

    Returns:
        dict: {'summary': {...}, 'records': [...]} or None on failure
    """
    
    # NASA Earthdata token
    token = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6Imhhc2hhYW0iLCJleHAiOjE3NjQ2MTc1NzgsImlhdCI6MTc1OTQzMzU3OCwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292IiwiaWRlbnRpdHlfcHJvdmlkZXIiOiJlZGxfb3BzIiwiYWNyIjoiZWRsIiwiYXNzdXJhbmNlX2xldmVsIjozfQ.o8_4Oc7jgv3T1ULzIp2vOico-e021Aye08tZv3zlii7iHf6PBFHRVW7H89Q2JsdbvlWsyov3TsolZwFxM9nQLTvsWwImjU17zfzFOO9mvPc-DRBNZMizSMbrntgD5lqEcNvSVT9uBhfbftAa60vhhXF9vLRS6sBEQPheUz6LZ7ulixNsYVUyfvNOVqXgNJsbT0TDwRt37N1o34touIdvth1927yDdMF5thuSifM9yBPjKUmxYocqnCCGV4o0mOvLGI-KC1QjQY3f-9LjWroF7f3RmBvQ_ngOLUKw26_Yu9Xes-loOcDTyEPhzN6iwGJG35CF7sjX3nGqKmjIstrN6w"
    
    def main():
        """
        Main processing function inside fetch_pblh_data.
        """
        logger.info("=" * 60)
        logger.info("STARTING MERRA-2 PBLH DATA COLLECTION (JSON MODE)")
        logger.info("=" * 60)

        # --- Config (North America bbox & collection) ---
        BBOX = {'min_lat': 15.0, 'max_lat': 72.0, 'min_lon': -168.0, 'max_lon': -52.0}
        CMR_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"
        SHORT_NAME = "M2T1NXFLX"  # contains PBLH
        VERSION = "5.12.4"

        # --- Setup dirs ---
        base_dir = Path(output_dir) / "pblh"
        raw_dir = base_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        # --- Search most recent granules ---
        logger.info(f"Step 1: Searching for MERRA-2 granules (last {days_back} days)...")
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)
        params = {
            "short_name": SHORT_NAME,
            "version": VERSION,
            "temporal": f"{start_date.strftime('%Y-%m-%dT%H:%M:%SZ')},{end_date.strftime('%Y-%m-%dT%H:%M:%SZ')}",
            "page_size": 50,
            "sort_key": "-start_date",
        }

        try:
            r = requests.get(CMR_URL, params=params, timeout=30)
            r.raise_for_status()
            entries = r.json().get("feed", {}).get("entry", [])
            if not entries:
                logger.error("No granules found")
                return None
            granule = entries[0]
            logger.info(f"Found most recent granule: {granule.get('title', 'Unknown')}")
        except Exception as e:
            logger.error(f"Search error: {e}")
            return None

        # --- Extract a downloadable URL ---
        def _extract_download_url(g):
            for link in g.get("links", []):
                if link.get("rel") == "http://esipfed.org/ns/fedsearch/1.1/data#":
                    href = link.get("href", "")
                    if "/data/" in href and "search.earthdata.nasa.gov" not in href:
                        return href
            for link in g.get("links", []):
                href = link.get("href", "").lower()
                if "opendap" in href and href.endswith(".nc4.html"):
                    return link["href"].replace("/opendap/", "/data/").replace(".html", "")
            return None

        url = _extract_download_url(granule)
        if not url:
            logger.error("No downloadable URL found in granule links")
            return None

        # --- Download file ---
        logger.info("Step 2: Downloading NetCDF file...")
        filename = url.split("/")[-1]
        nc_path = raw_dir / filename

        if not nc_path.exists():
            try:
                with requests.Session() as s:
                    s.headers.update({
                        "Authorization": f"Bearer {token}",
                        "User-Agent": "MERRA2-PBL-Fetcher/1.0"
                    })
                    with s.get(url, stream=True, timeout=300) as resp:
                        resp.raise_for_status()
                        if "text/html" in resp.headers.get("content-type", ""):
                            logger.error("Got HTML instead of data file (auth/permissions issue)")
                            return None
                        with open(nc_path, "wb") as f:
                            for chunk in resp.iter_content(8192):
                                if chunk:
                                    f.write(chunk)
                logger.info(f"Downloaded: {filename}")
            except Exception as e:
                logger.error(f"Download failed: {e}")
                return None
        else:
            logger.info(f"Using cached file: {filename}")

        # --- Process NetCDF to JSON records ---
        logger.info("Step 3: Processing NetCDF file...")
        try:
            ds = xr.open_dataset(nc_path, engine="h5netcdf")
            if "PBLH" not in ds.data_vars:
                logger.error("Variable 'PBLH' not found in dataset")
                ds.close()
                return None

            # Subset to North America
            logger.info("Step 4: Subsetting to North America region...")
            lats = ds["lat"].values
            lons = ds["lon"].values
            lat_mask = (lats >= BBOX["min_lat"]) & (lats <= BBOX["max_lat"])
            lon_mask = (lons >= BBOX["min_lon"]) & (lons <= BBOX["max_lon"])
            ds_subset = ds.sel(lat=lats[lat_mask], lon=lons[lon_mask])

            # Convert to records
            logger.info("Step 5: Converting to JSON records...")
            df = ds_subset["PBLH"].to_dataframe().reset_index()
            df = df.dropna()

            # Build records list
            records = []
            for _, row in df.iterrows():
                records.append({
                    "latitude": float(row["lat"]),
                    "longitude": float(row["lon"]),
                    "timestamp": row["time"].isoformat() if hasattr(row["time"], "isoformat") else str(row["time"]),
                    "pbl_height_m": float(row["PBLH"]),
                    "data_source": "MERRA-2",
                    "variable": "PBLH",
                    "unit": "meters"
                })

            ds.close()

            if not records:
                logger.error("No valid records after processing")
                return None

            # Build summary
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            pblh_values = [r["pbl_height_m"] for r in records]
            timestamps = [r["timestamp"] for r in records]

            summary = {
                "variable": "PBLH (Planetary Boundary Layer Height)",
                "collection": SHORT_NAME,
                "collection_version": VERSION,
                "bbox": BBOX,
                "time_range": {
                    "min": min(timestamps),
                    "max": max(timestamps)
                },
                "total_observations": len(records),
                "collection_timestamp": timestamp,
                "data_source": "MERRA-2",
                "statistics": {
                    "mean": float(sum(pblh_values) / len(pblh_values)),
                    "min": float(min(pblh_values)),
                    "max": float(max(pblh_values))
                }
            }

            payload = {"summary": summary, "records": records}

            # Optional: pretty-print JSON to terminal
            if print_json:
                preview = {
                    "summary": summary,
                    "records_preview_count": min(len(records), max_print_records),
                    "records_preview": records[:max_print_records],
                    "note": f"Showing first {min(len(records), max_print_records)} of {len(records)} records"
                            if len(records) > max_print_records else "Showing all records"
                }
                print(json.dumps(preview, indent=2))

            logger.info("=" * 60)
            logger.info(f"SUCCESS! Collected {len(records):,} PBLH observations")
            logger.info(f"Mean PBL Height: {summary['statistics']['mean']:.1f} meters")
            logger.info("=" * 60)

            return payload

        except Exception as e:
            logger.error(f"Processing error: {e}")
            return None
    
    # Call the inner main function and return its result
    return main()



# ---------------------------------------------------------------------------------------------------------------------------------------------
# TEMPO 1

def no2_pipeline():
    # ============ ALL IMPORTS & SETUP LIVE INSIDE THIS ONE FUNCTION ============
    import os
    import json
    import gc
    import logging
    from pathlib import Path
    from datetime import datetime, timedelta
    import numpy as np
    import netCDF4 as nc

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    # ============================== CONFIG =====================================
    NASA_TOKEN = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6Imhhc2hhYW0iLCJleHAiOjE3NjQ2MTc1NzgsImlhdCI6MTc1OTQzMzU3OCwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292IiwiaWRlbnRpdHlfcHJvdmlkZXIiOiJlZGxfb3BzIiwiYWNyIjoiZWRsIiwiYXNzdXJhbmNlX2xldmVsIjozfQ.o8_4Oc7jgv3T1ULzIp2vOico-e021Aye08tZv3zlii7iHf6PBFHRVW7H89Q2JsdbvlWsyov3TsolZwFxM9nQLTvsWwImjU17zfzFOO9mvPc-DRBNZMizSMbrntgD5lqEcNvSVT9uBhfbftAa60vhhXF9vLRS6sBEQPheUz6LZ7ulixNsYVUyfvNOVqXgNJsbT0TDwRt37N1o34touIdvth1927yDdMF5thuSifM9yBPjKUmxYocqnCCGV4o0mOvLGI-KC1QjQY3f-9LjWroF7f3RmBvQ_ngOLUKw26_Yu9Xes-loOcDTyEPhzN6iwGJG35CF7sjX3nGqKmjIstrN6w"
    hours_back = 24
    output_dir = "./data_downloads"
    max_files = 5
    print_json = True
    max_print_records = 500000
    save_json_path = "./no2_data_output.json"

    # ============================= WORKFLOW ====================================
    logger.info("=" * 60)
    logger.info("STARTING NO2 DATA COLLECTION (JSON MODE)")
    logger.info("=" * 60)

    # Setup directories (download cache only)
    base_dir = Path(output_dir) / "no2"
    raw_dir = base_dir / "raw"
    processed_dir = base_dir / "processed"  # kept for parity, not used for CSV
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Search for files
    logger.info(f"Step 1: Searching for TEMPO NO2 files (last {hours_back} hours)...")
    now_utc = datetime.utcnow()
    end_time = now_utc - timedelta(hours=6)  # Account for processing delay
    start_time = end_time - timedelta(hours=hours_back)

    try:
        import earthaccess
        os.environ['EARTHDATA_TOKEN'] = NASA_TOKEN

        try:
            auth = earthaccess.login()
        except Exception:
            auth = False

        if not auth:
            logger.error("Authentication failed")
            return None

        results = earthaccess.search_data(
            short_name="TEMPO_NO2_L2",
            temporal=(start_time.strftime('%Y-%m-%d'), end_time.strftime('%Y-%m-%d')),
            count=max_files
        )

        if not results:
            logger.warning("No files found")
            return None

        logger.info(f"Found {len(results)} files")

        # Step 2: Download files
        logger.info("Step 2: Downloading files...")
        downloaded_paths = earthaccess.download(results, str(raw_dir), threads=2)
        downloaded_files = [str(p) for p in downloaded_paths if p and os.path.exists(p)]

        if not downloaded_files:
            logger.error("No files downloaded")
            return None

        logger.info(f"Downloaded {len(downloaded_files)} files")

    except ImportError:
        logger.error("earthaccess not installed: pip install earthaccess")
        return None
    except Exception as e:
        logger.error(f"Search/download error: {e}")
        return None

    # Step 3: Process NetCDF files → records list
    logger.info("Step 3: Processing NetCDF files to extract NO2 data...")
    all_records = []

    for file_path in downloaded_files:
        try:
            logger.info(f"Processing {os.path.basename(file_path)}")

            with nc.Dataset(file_path, 'r') as ds:
                # Find NO2, latitude, longitude data
                lon_data = lat_data = no2_data = None
                possible_groups = [ds] + list(ds.groups.values())

                for group in possible_groups:
                    try:
                        if 'longitude' in group.variables or 'lon' in group.variables:
                            lon_data = group.variables.get('longitude', group.variables.get('lon'))[:]
                        if 'latitude' in group.variables or 'lat' in group.variables:
                            lat_data = group.variables.get('latitude', group.variables.get('lat'))[:]
                        if 'vertical_column_troposphere' in group.variables:
                            no2_data = group.variables['vertical_column_troposphere'][:]
                        elif 'nitrogen_dioxide_tropospheric_column' in group.variables:
                            no2_data = group.variables['nitrogen_dioxide_tropospheric_column'][:]

                        if lon_data is not None and lat_data is not None and no2_data is not None:
                            break
                    except Exception:
                        continue

                if lon_data is None or lat_data is None or no2_data is None:
                    logger.warning(f"Required variables not found in {os.path.basename(file_path)}")
                    continue

                # Flatten extra dimensions
                while no2_data.ndim > 2:
                    no2_data = no2_data[0]
                while lon_data.ndim > 2:
                    lon_data = lon_data[0]
                while lat_data.ndim > 2:
                    lat_data = lat_data[0]

                # Sample if dataset is too large
                if no2_data.size > 100000:
                    logger.info(f"Large dataset, sampling to ~50k points...")
                    if no2_data.ndim == 2:
                        h, w = no2_data.shape
                        step_h = max(1, h // 200)
                        step_w = max(1, w // 200)
                        no2_data = no2_data[::step_h, ::step_w]
                        lon_data = lon_data[::step_h, ::step_w]
                        lat_data = lat_data[::step_h, ::step_w]

                # Flatten and validate
                no2_flat = no2_data.flatten()
                lon_flat = lon_data.flatten()
                lat_flat = lat_data.flatten()

                valid_mask = (
                    (no2_flat > 0) & (no2_flat < 1e16) &
                    np.isfinite(no2_flat) & np.isfinite(lon_flat) & np.isfinite(lat_flat) &
                    (lon_flat > -180) & (lon_flat < 180) & (lat_flat > -90) & (lat_flat < 90)
                )

                if np.any(valid_mask):
                    n_valid = int(min(np.sum(valid_mask), 10000))  # Max 10k per file
                    obs_time = (datetime.utcnow() - timedelta(hours=12)).isoformat()

                    # Build records (JSON-serializable)
                    v_no2 = no2_flat[valid_mask][:n_valid]
                    v_lon = lon_flat[valid_mask][:n_valid]
                    v_lat = lat_flat[valid_mask][:n_valid]

                    for i in range(n_valid):
                        all_records.append({
                            "no2_tropospheric_column": float(v_no2[i]),
                            "longitude": float(v_lon[i]),
                            "latitude": float(v_lat[i]),
                            "observation_datetime_utc": obs_time,
                            "file_name": os.path.basename(file_path),
                            "data_source": "TEMPO_NO2_L2"
                        })

                    logger.info(f"Extracted {n_valid} valid observations")

            gc.collect()

        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            continue

    if not all_records:
        logger.error("No valid data extracted")
        return None

    # Step 4: Build summary JSON (no CSV writing)
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    values = [r["no2_tropospheric_column"] for r in all_records]
    summary = {
        "variable": "NO2",
        "collection_timestamp": timestamp,
        "total_observations": len(all_records),
        "unique_files": len(set(r["file_name"] for r in all_records)),
        "statistics": {
            "mean": float(np.mean(values)),
            "median": float(np.median(values)),
            "min": float(np.min(values)),
            "max": float(np.max(values))
        }
    }

    payload = {"summary": summary, "records": all_records}

    # Optional: pretty-print JSON to terminal (truncated to keep logs readable)
    if print_json:
        preview = {
            "summary": summary,
            "records_preview_count": min(len(all_records), max_print_records),
            "records_preview": all_records[:max_print_records],
            "note": f"Showing first {min(len(all_records), max_print_records)} of {len(all_records)} records"
                    if len(all_records) > max_print_records else "Showing all records"
        }
        print(json.dumps(preview, indent=2))

    # Optional: save full JSON to file
    if save_json_path:
        try:
            with open(save_json_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            logger.info(f"Full JSON saved to: {save_json_path}")
        except Exception as e:
            logger.warning(f"Could not save JSON to {save_json_path}: {e}")

    logger.info("=" * 60)
    logger.info("SUCCESS! NO2 JSON prepared")
    logger.info("=" * 60)

    # Final log summary
    if payload:
        logger.info(f"\nProcessing complete!")
        logger.info(f"Total observations: {payload['summary']['total_observations']}")
        logger.info(f"Files processed: {payload['summary']['unique_files']}")
        logger.info(f"Mean NO2: {payload['summary']['statistics']['mean']:.2e}")
    else:
        logger.error("Failed to fetch NO2 data")

    return payload



# ----------------------------------------------------------------------------------------------------------------------------------


def run_tolnet_fetcher():
    import os
    import requests
    import json
    from datetime import datetime, timedelta, timezone

    def fetch_tolnet_metadata_json(token, days_back=7, max_files=10, print_json=True):
        """
        Fetch recent TOLNet granule metadata from NASA CMR and (optionally) print JSON.
        """
        cmr_base = "https://cmr.earthdata.nasa.gov"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "TOLNet-Fetcher/1.0"
        }

        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days_back)
        start_str = start_date.strftime("%Y-%m-%d")
        end_str   = end_date.strftime("%Y-%m-%d")

        try:
            # 1) Find TOLNet collections
            col_params = {
                "keyword": "TOLNet",
                "provider": "LARC_ASDC",
                "has_granules": "true",
                "page_size": 50
            }
            r = requests.get(f"{cmr_base}/search/collections.json", params=col_params, headers=headers, timeout=30)
            r.raise_for_status()
            collections = r.json().get("feed", {}).get("entry", []) or []
            if not collections:
                col_params["keyword"] = "tropospheric ozone lidar"
                r = requests.get(f"{cmr_base}/search/collections.json", params=col_params, headers=headers, timeout=30)
                r.raise_for_status()
                collections = r.json().get("feed", {}).get("entry", []) or []

            if not collections:
                out = {"error": "No TOLNet collections found", "days_back": days_back}
                if print_json: print(json.dumps(out, indent=2))
                return None

            # 2) Find granules across collections
            all_granules = []
            for col in collections:
                col_id = col.get("id")
                g_params = {
                    "collection_concept_id": col_id,
                    "temporal": f"{start_str}T00:00:00Z,{end_str}T23:59:59Z",
                    "page_size": max_files,
                    "sort_key": "-start_date"
                }
                rg = requests.get(f"{cmr_base}/search/granules.json", params=g_params, headers=headers, timeout=60)
                if rg.status_code == 200:
                    granules = rg.json().get("feed", {}).get("entry", []) or []
                    for g in granules:
                        g["collection_title"] = col.get("title", "Unknown")
                        all_granules.append(g)

            if not all_granules:
                out = {
                    "error": "No granules found in time window", 
                    "window": [start_str, end_str],
                    "hint": "Try increasing days_back parameter or use an earlier date range"
                }
                if print_json: print(json.dumps(out, indent=2))
                return None

            # 3) Build records
            records, used = [], 0
            for g in all_granules:
                if used >= max_files: break

                links = g.get("links", []) or []
                dlinks = [L.get("href") for L in links if L.get("rel") == "http://esipfed.org/ns/fedsearch/1.1/data#"]

                rec = {
                    "granule_id": g.get("id", ""),
                    "title": g.get("title", ""),
                    "collection": g.get("collection_title", ""),
                    "start_date": g.get("time_start", ""),
                    "end_date": g.get("time_end", ""),
                    "updated": g.get("updated", ""),
                    "download_url": dlinks[0] if dlinks else "",
                    "file_count": len(dlinks)
                }

                if "polygons" in g and g["polygons"]:
                    try:
                        poly = g["polygons"][0]
                        rec["polygons"] = poly
                    except Exception:
                        pass

                if "summary" in g and isinstance(g["summary"], str):
                    s = g["summary"]
                    rec["summary"] = (s[:200] + "...") if len(s) > 200 else s

                records.append(rec)
                used += 1

            summary = {
                "variable": "O3 (metadata only; measurements inside data files)",
                "collections_found": len({r["collection"] for r in records}),
                "total_granules_listed": len(records),
                "search_window_utc": [start_str, end_str],
                "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            out = {"records": records, "summary": summary}

            if print_json:
                print(json.dumps(out, indent=2))

            return out

        except Exception as e:
            err = {"error": f"{type(e).__name__}: {e}"}
            if print_json: print(json.dumps(err, indent=2))
            return None

    # ======================
    # Execute and RETURN result
    # ======================
    token = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6Imhhc2hhYW0iLCJleHAiOjE3NjQ2MTc1NzgsImlhdCI6MTc1OTQzMzU3OCwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292IiwiaWRlbnRpdHlfcHJvdmlkZXIiOiJlZGxfb3BzIiwiYWNyIjoiZWRsIiwiYXNzdXJhbmNlX2xldmVsIjozfQ.o8_4Oc7jgv3T1ULzIp2vOico-e021Aye08tZv3zlii7iHf6PBFHRVW7H89Q2JsdbvlWsyov3TsolZwFxM9nQLTvsWwImjU17zfzFOO9mvPc-DRBNZMizSMbrntgD5lqEcNvSVT9uBhfbftAa60vhhXF9vLRS6sBEQPheUz6LZ7ulixNsYVUyfvNOVqXgNJsbT0TDwRt37N1o34touIdvth1927yDdMF5thuSifM9yBPjKUmxYocqnCCGV4o0mOvLGI-KC1QjQY3f-9LjWroF7f3RmBvQ_ngOLUKw26_Yu9Xes-loOcDTyEPhzN6iwGJG35CF7sjX3nGqKmjIstrN6w"

    print("=" * 60)
    print("TOLNet Metadata Fetcher")
    print("=" * 60)
    print()

    result = fetch_tolnet_metadata_json(
        token=token,
        days_back=365,
        max_files=2000,
        print_json=True
    )

    if result:
        print()
        print("=" * 60)
        print("Fetch Complete!")
        print(f"Found {result['summary']['total_granules_listed']} granules")
        print(f"From {result['summary']['collections_found']} collections")
        print("=" * 60)
    else:
        print()
        print("=" * 60)
        print("No data retrieved")
        print("Suggestion: Try searching an earlier time period")
        print("TOLNet data may have a processing delay")
        print("=" * 60)
    
    # CRITICAL: Return the result!
    return result

    





# last------------------------------------------------------------------------------------------------------------------------


import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os


def fetch_nasa_aerosol_data():
    """
    Main function to fetch MODIS/VIIRS aerosol data from NASA Earthdata for North America
    Contains all logic and execution code
    """
    
    # Your NASA Earthdata token
    TOKEN = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6Imhhc2hhYW0iLCJleHAiOjE3NjQ2MTc1NzgsImlhdCI6MTc1OTQzMzU3OCwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292IiwiaWRlbnRpdHlfcHJvdmlkZXIiOiJlZGxfb3BzIiwiYWNyIjoiZWRsIiwiYXNzdXJhbmNlX2xldmVsIjozfQ.o8_4Oc7jgv3T1ULzIp2vOico-e021Aye08tZv3zlii7iHf6PBFHRVW7H89Q2JsdbvlWsyov3TsolZwFxM9nQLTvsWwImjU17zfzFOO9mvPc-DRBNZMizSMbrntgD5lqEcNvSVT9uBhfbftAa60vhhXF9vLRS6sBEQPheUz6LZ7ulixNsYVUyfvNOVqXgNJsbT0TDwRt37N1o34touIdvth1927yDdMF5thuSifM9yBPjKUmxYocqnCCGV4o0mOvLGI-KC1QjQY3f-9LjWroF7f3RmBvQ_ngOLUKw26_Yu9Xes-loOcDTyEPhzN6iwGJG35CF7sjX3nGqKmjIstrN6w"
    
    # Configuration parameters
    hours_back = 720
    max_results_per_dataset = 1000
    
    # Configuration
    headers = {
        'Authorization': f'Bearer {TOKEN}',
        'Accept': 'application/json'
    }
    
    # North America bounding box (approximate)
    north_america_bbox = {
        'min_lat': 15.0,   # Southern Mexico
        'max_lat': 72.0,   # Northern Canada
        'min_lon': -170.0, # Alaska (west)
        'max_lon': -52.0   # Newfoundland (east)
    }
    
    # CMR (Common Metadata Repository) API endpoints
    cmr_base_url = "https://cmr.earthdata.nasa.gov/search"
    
    # Dataset short names for MODIS and VIIRS aerosol products
    datasets = {
        'MODIS_Terra': 'MOD04_L2',      # Terra MODIS Aerosol 5-Min L2 Swath 10km
        'MODIS_Aqua': 'MYD04_L2',       # Aqua MODIS Aerosol 5-Min L2 Swath 10km
        'VIIRS_NOAA20': 'VNP04_L2',     # VIIRS/NOAA20 Aerosol 6-Min L2 Swath
        'VIIRS_SNPP': 'VNP04_L2'        # VIIRS/SNPP Aerosol 6-Min L2 Swath
    }
    
    def get_latest_granules(dataset_name, hours_back, max_results):
        """Search for latest granules from CMR"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours_back)
        
        params = {
            'short_name': dataset_name,
            'temporal': f"{start_time.strftime('%Y-%m-%dT%H:%M:%SZ')},{end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}",
            'bounding_box': f"{north_america_bbox['min_lon']},{north_america_bbox['min_lat']},{north_america_bbox['max_lon']},{north_america_bbox['max_lat']}",
            'page_size': max_results,
            'sort_key': '-start_date'
        }
        
        try:
            response = requests.get(
                f"{cmr_base_url}/granules.json",
                params=params,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            granules = data.get('feed', {}).get('entry', [])
            
            print(f"Found {len(granules)} granules for {dataset_name}")
            return granules
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching granules for {dataset_name}: {e}")
            return []
    
    def extract_granule_info(granule):
        """Extract relevant information from granule metadata"""
        info = {
            'title': granule.get('title', 'N/A'),
            'granule_id': granule.get('id', 'N/A'),
            'time_start': granule.get('time_start', 'N/A'),
            'time_end': granule.get('time_end', 'N/A'),
            'updated': granule.get('updated', 'N/A'),
            'dataset_id': granule.get('dataset_id', 'N/A'),
        }
        
        # Extract download links
        links = granule.get('links', [])
        for link in links:
            if link.get('rel') == 'http://esipfed.org/ns/fedsearch/1.1/data#':
                info['download_url'] = link.get('href', 'N/A')
                break
        
        # Extract bounding box
        if 'boxes' in granule and granule['boxes']:
            info['bbox'] = granule['boxes'][0]
        
        return info
    
    def to_json(df, pretty=True):
        """Convert DataFrame to JSON format"""
        if df.empty:
            return json.dumps({"status": "no_data", "message": "No data found"}, indent=2 if pretty else None)
        
        # Convert DataFrame to dict with records orientation
        data_dict = df.to_dict(orient='records')
        
        # Create structured JSON response
        response = {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "total_granules": len(df),
            "region": "North America",
            "bbox": north_america_bbox,
            "data": data_dict
        }
        
        return json.dumps(response, indent=2 if pretty else None, default=str)
    
    def print_json(df):
        """Print data as formatted JSON to terminal"""
        json_str = to_json(df, pretty=True)
        print("\n" + "="*80)
        print("JSON RESPONSE")
        print("="*80)
        print(json_str)
        print("="*80 + "\n")
    
    # Execute logic and RETURN result
    all_data = []

    print(f"\nFetching MODIS/VIIRS Aerosol Data for North America")
    print(f"Time range: Last {hours_back} hours")
    print(f"Bounding Box: {north_america_bbox}\n")

    # Fetch data from all datasets
    for satellite, dataset_name in datasets.items():
        print(f"Processing {satellite} ({dataset_name})...")

        granules = get_latest_granules(dataset_name, hours_back, max_results_per_dataset)

        for granule in granules:
            info = extract_granule_info(granule)
            info['satellite'] = satellite
            info['dataset'] = dataset_name
            all_data.append(info)

    if not all_data:
        print("\nNo data found for the specified criteria.")
        return None

    # Create DataFrame
    df = pd.DataFrame(all_data)
    print(f"\nTotal granules found: {len(df)}")

    # Convert time_start to datetime
    df['datetime'] = pd.to_datetime(df['time_start'])
    df['hour'] = df['datetime'].dt.floor('h')  # Use 'h' instead of 'H' to avoid deprecation warning

    # Save files (optional)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_file = f'nasa_aerosol_data_{timestamp}.csv'
    df.to_csv(csv_file, index=False)
    print(f"\nData saved to: {csv_file}")

    # Create hourly summary
    hourly_summary = df.groupby(['hour', 'satellite']).agg({
        'granule_id': 'count',
        'title': 'first'
    }).reset_index()
    hourly_summary.columns = ['hour', 'satellite', 'granule_count', 'sample_title']

    if not hourly_summary.empty:
        summary_file = csv_file.replace('.csv', '_hourly_summary.csv')
        hourly_summary.to_csv(summary_file, index=False)
        print(f"Hourly summary saved to: {summary_file}")

    # Save JSON files
    json_file = csv_file.replace('.csv', '.json')
    with open(json_file, 'w') as f:
        f.write(to_json(df, pretty=True))
    print(f"JSON data saved to: {json_file}")

    summary_json_file = csv_file.replace('.csv', '_hourly_summary.json')
    with open(summary_json_file, 'w') as f:
        f.write(to_json(hourly_summary, pretty=True))
    print(f"Hourly summary JSON saved to: {summary_json_file}")

    # Display sample
    print("\n=== Sample Data (First 5 records) ===")
    print(df[['satellite', 'time_start', 'title']].head().to_string(index=False))

    # CRITICAL: Return the structured data for database insertion
    result = {
        "records": all_data,
        "summary": {
            "total_granules": len(df),
            "satellites": list(df['satellite'].unique()),
            "time_range_hours": hours_back,
            "region": "North America",
            "bbox": north_america_bbox,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "hourly_summary": hourly_summary.to_dict(orient='records')
        }
    }

    return result




# 2nd last-----------------------------------------------------------------------------------------------------------------------------------

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def fetch_and_process_goes_data():
    """
    Complete GOES satellite data fetcher and processor.
    Fetches CONUS data from GOES-16 and GOES-18 satellites and processes it.
    """

    # NASA Earthdata token
    TOKEN = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4sInNpZyI6ImVkbGp3dHB1YmtleV9vcHMiLCJhbGciOiJSUzI1NiJ9.eyJ0eXBlIjoiVXNlciIsInVpZCI6Imhhc3NhbnNpZGRpcXVpMDk0NiIsImV4cCI6MTc2MzMzNzU5OSwiaWF0IjoxNzU4MTI3MjA2LCJpc3MiOiJodHRwczovL3Vycy5lYXJ0aGRhdGEubmFzYS5nb3YiLCJpZGVudGl0eV9wc...LIpvasyxPBB3hgB0af3O9sz37usL0slvOqL8fCC76Ba181aTCJLmfr6mwgkmB_P1LkQXcl624wm_H5_LCi-nj2A0JGcxEVkFVH4P5hSseXLW0Zz2HgSMEBro5fGzucPNYLK6yyae-NXpAqnphQsvr_TJzuvjEkeGPuhkB6h8J6DB_RyXSutov7-3Gbxxs6FLArAbFIWMvZi0pggIXE2hBsrdS7fl2bsrIevB_CVuuEQAfszdW6LthIhBkSy1g6qtRV_LncvGSEylyXt5Ave4pjoCS8memaq37p6uNTUTSUlSXIGYoTSyLohndj3YbKRlARXiQokrdZ9HMXjfJ02Azw"

    # Helper function: Fetch NOAA GOES latest files
    def fetch_noaa_goes_latest(satellite='goes16', product='ABI-L2-CMIPC', channel=13):
        try:
            import boto3
            from botocore import UNSIGNED
            from botocore.config import Config
        except ImportError:
            print(json.dumps({"status": "installing_boto3"}))
            os.system('pip install boto3 -q')
            import boto3
            from botocore import UNSIGNED
            from botocore.config import Config

        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED), region_name='us-east-1')
        bucket = f'noaa-{satellite}'

        files_found = []
        now = datetime.utcnow()

        for hour_offset in range(168):
            check_time = now - timedelta(hours=hour_offset)
            day_of_year = check_time.timetuple().tm_yday
            hour = check_time.hour

            prefix = f'{product}/{check_time.year}/{day_of_year:03d}/{hour:02d}/'

            try:
                print(json.dumps({
                    "action": "searching",
                    "satellite": satellite.upper(),
                    "product": product,
                    "prefix": prefix
                }))

                response = s3.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    MaxKeys=20
                )

                if 'Contents' in response:
                    for obj in response['Contents']:
                        if obj['Key'].endswith('.nc'):
                            files_found.append({
                                'bucket': bucket,
                                'key': obj['Key'],
                                'size': obj['Size'],
                                'last_modified': obj['LastModified'],
                                'satellite': satellite.upper(),
                                'product': product
                            })

                    if files_found:
                        break

            except Exception as e:
                print(json.dumps({
                    "error": str(e),
                    "prefix": prefix
                }))
                continue

        if files_found:
            files_found.sort(key=lambda x: x['last_modified'], reverse=True)
            return files_found[:10]

        return []

    # Helper function: Download GOES file
    def download_goes_file(bucket, key, local_path):
        try:
            import boto3
            from botocore import UNSIGNED
            from botocore.config import Config
        except ImportError:
            os.system('pip install boto3 -q')
            import boto3
            from botocore import UNSIGNED
            from botocore.config import Config

        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED), region_name='us-east-1')

        try:
            print(json.dumps({
                "action": "downloading",
                "filename": os.path.basename(key),
                "key": key
            }))

            s3.download_file(bucket, key, local_path)
            file_size = os.path.getsize(local_path) / (1024*1024)

            print(json.dumps({
                "action": "download_complete",
                "filename": os.path.basename(key),
                "size_mb": round(file_size, 2)
            }))

            return True
        except Exception as e:
            print(json.dumps({
                "error": str(e),
                "action": "download_failed"
            }))
            return False

    # Helper function: Extract GOES data
    def extract_goes_data_detailed(nc_file, output_csv, sample_points=5000):
        try:
            import netCDF4 as nc
        except ImportError:
            print(json.dumps({"status": "installing_netcdf4"}))
            os.system('pip install netCDF4 -q')
            import netCDF4 as nc

        try:
            print(json.dumps({"action": "reading_netcdf", "file": os.path.basename(nc_file)}))

            dataset = nc.Dataset(nc_file, 'r')

            # Get dimensions
            dims = {}
            for dim_name in dataset.dimensions:
                dims[dim_name] = len(dataset.dimensions[dim_name])

            # File structure info
            file_info = {
                'action': 'file_structure',
                'file': os.path.basename(nc_file),
                'dimensions': dims,
                'variables': list(dataset.variables.keys()),
                'num_variables': len(dataset.variables.keys())
            }
            print(json.dumps(file_info, indent=2))

            # Extract all variables
            all_data = []
            variable_info = []

            for var_name in dataset.variables.keys():
                var = dataset.variables[var_name]

                # Get attributes
                attrs = {}
                for attr in var.ncattrs():
                    try:
                        attrs[attr] = var.getncattr(attr)
                    except:
                        pass

                var_details = {
                    'action': 'processing_variable',
                    'variable': var_name,
                    'shape': list(var.shape),
                    'dimensions': list(var.dimensions),
                    'long_name': attrs.get('long_name', 'N/A'),
                    'units': attrs.get('units', 'N/A'),
                    'data_type': str(var.dtype)
                }
                print(json.dumps(var_details))

                variable_info.append({
                    'variable': var_name,
                    'shape': str(var.shape),
                    'dimensions': str(var.dimensions),
                    'long_name': attrs.get('long_name', 'N/A'),
                    'units': attrs.get('units', 'N/A'),
                    'data_type': str(var.dtype)
                })

                # Extract data based on dimensions
                try:
                    data = var[:]

                    if len(var.shape) == 0:  # Scalar
                        all_data.append({
                            'variable': var_name,
                            'value': float(data),
                            'type': 'scalar'
                        })

                    elif len(var.shape) == 1:  # 1D array
                        if len(data) > 100:
                            indices = np.linspace(0, len(data)-1, 100, dtype=int)
                            data = data[indices]

                        for i, val in enumerate(data):
                            all_data.append({
                                'variable': var_name,
                                'index': i,
                                'value': float(val) if not np.ma.is_masked(val) else np.nan,
                                'type': '1d_array'
                            })

                    elif len(var.shape) == 2:  # 2D array
                        rows, cols = var.shape
                        total_points = rows * cols

                        if total_points > sample_points:
                            step = int(np.sqrt(total_points / sample_points))
                        else:
                            step = 1

                        print(json.dumps({
                            'action': 'sampling_2d',
                            'variable': var_name,
                            'original_shape': [rows, cols],
                            'sampling_step': step,
                            'sampled_shape': [rows//step, cols//step]
                        }))

                        sampled_data = data[::step, ::step]

                        for i in range(sampled_data.shape[0]):
                            for j in range(sampled_data.shape[1]):
                                val = sampled_data[i, j]
                                if not np.ma.is_masked(val):
                                    all_data.append({
                                        'variable': var_name,
                                        'row': i * step,
                                        'col': j * step,
                                        'value': float(val),
                                        'type': '2d_array'
                                    })

                except Exception as e:
                    print(json.dumps({
                        'error': str(e),
                        'variable': var_name,
                        'action': 'extraction_failed'
                    }))

            dataset.close()

            # Save variable info
            var_info_file = output_csv.replace('.csv', '_variables.csv')
            var_df = pd.DataFrame(variable_info)
            var_df.to_csv(var_info_file, index=False)

            print(json.dumps({
                'action': 'saved',
                'file': os.path.basename(var_info_file),
                'type': 'variable_info'
            }))

            # Convert to DataFrame and save
            if all_data:
                df = pd.DataFrame(all_data)

                scalar_df = df[df['type'] == 'scalar'][['variable', 'value']]
                array_1d_df = df[df['type'] == '1d_array'][['variable', 'index', 'value']]
                array_2d_df = df[df['type'] == '2d_array'][['variable', 'row', 'col', 'value']]

                # Save main data
                df.to_csv(output_csv, index=False)

                print(json.dumps({
                    'action': 'saved',
                    'file': os.path.basename(output_csv),
                    'type': 'main_data',
                    'records': {
                        'total': len(df),
                        'scalar_values': len(scalar_df),
                        '1d_arrays': len(array_1d_df),
                        '2d_arrays': len(array_2d_df)
                    }
                }))

                # Save imagery data
                if len(array_2d_df) > 0:
                    img_file = output_csv.replace('.csv', '_imagery.csv')
                    array_2d_df.to_csv(img_file, index=False)

                    print(json.dumps({
                        'action': 'saved',
                        'file': os.path.basename(img_file),
                        'type': 'imagery_data',
                        'records': len(array_2d_df)
                    }))

                # Create statistics
                if len(array_2d_df) > 0:
                    summary_data = []
                    for var in array_2d_df['variable'].unique():
                        var_data = array_2d_df[array_2d_df['variable'] == var]['value']
                        summary_data.append({
                            'variable': var,
                            'min': float(var_data.min()),
                            'max': float(var_data.max()),
                            'mean': float(var_data.mean()),
                            'std': float(var_data.std()),
                            'count': int(len(var_data))
                        })

                    summary_df = pd.DataFrame(summary_data)
                    summary_file = output_csv.replace('.csv', '_statistics.csv')
                    summary_df.to_csv(summary_file, index=False)

                    print(json.dumps({
                        'action': 'statistics',
                        'file': os.path.basename(summary_file),
                        'data': summary_data
                    }, indent=2))

                return True
            else:
                print(json.dumps({'error': 'no_data_extracted'}))
                return False

        except Exception as e:
            print(json.dumps({
                'error': str(e),
                'action': 'extraction_failed',
                'traceback': str(e)
            }))
            return False

    # ===== MAIN PROCESSING WITH RETURN =====
    output_dir = Path('satellite_data_north_america')
    output_dir.mkdir(exist_ok=True)

    print(json.dumps({
        "status": "starting",
        "program": "NASA GOES North America Satellite Data Extractor",
        "coverage": "Continental United States (CONUS)",
        "output_directory": str(output_dir.absolute())
    }, indent=2))

    all_files = []
    all_extracted_data = []

    # Fetch CONUS (North America) data
    print(json.dumps({"action": "searching_goes16_conus", "coverage": "North America"}))
    goes16_files = fetch_noaa_goes_latest('goes16', 'ABI-L2-CMIPC')
    all_files.extend(goes16_files)

    print(json.dumps({"action": "searching_goes18_conus", "coverage": "North America"}))
    goes18_files = fetch_noaa_goes_latest('goes18', 'ABI-L2-CMIPC')
    all_files.extend(goes18_files)

    if not all_files:
        print(json.dumps({
            "status": "error",
            "message": "No CONUS data found. Check network connection or try again later."
        }))
        return None

    print(json.dumps({
        "status": "files_found",
        "total_files": len(all_files),
        "coverage": "CONUS (Continental US - 20°N to 55°N, 130°W to 60°W)"
    }))

    # Create file summary
    summary_data = []
    for f in all_files:
        file_record = {
            'satellite': f['satellite'],
            'product': f['product'],
            'filename': os.path.basename(f['key']),
            'size_mb': round(f['size'] / 1024 / 1024, 2),
            'timestamp': f['last_modified'].strftime('%Y-%m-%d %H:%M:%S UTC'),
            'bucket': f['bucket'],
            'key': f['key']
        }
        summary_data.append(file_record)
        all_extracted_data.append(file_record)

    summary_df = pd.DataFrame(summary_data)
    summary_file = output_dir / f"file_list_conus_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    summary_df.to_csv(summary_file, index=False)

    print(json.dumps({
        "action": "file_list",
        "files": summary_data
    }, indent=2, default=str))

    # Process files
    print(json.dumps({"action": "starting_download_extraction"}))

    processed_satellites = []
    for satellite in ['GOES16', 'GOES18']:
        sat_files = [f for f in all_files if f['satellite'] == satellite]

        if sat_files:
            file_info = sat_files[0]

            print(json.dumps({
                "action": "processing_satellite",
                "satellite": satellite,
                "product": file_info['product']
            }))

            nc_filename = output_dir / f"{satellite}_CONUS_latest.nc"
            csv_filename = output_dir / f"{satellite}_CONUS_data.csv"

            if download_goes_file(file_info['bucket'], file_info['key'], str(nc_filename)):
                print(json.dumps({"action": "extracting_data", "satellite": satellite}))
                if extract_goes_data_detailed(str(nc_filename), str(csv_filename), sample_points=10000):
                    processed_satellites.append({
                        'satellite': satellite,
                        'csv_file': str(csv_filename),
                        'status': 'success'
                    })

                # Remove NetCDF to save space
                if os.path.getsize(nc_filename) > 10*1024*1024:
                    os.remove(nc_filename)
                    print(json.dumps({
                        "action": "cleanup",
                        "removed": os.path.basename(nc_filename),
                        "reason": "large_file"
                    }))

    print(json.dumps({
        "status": "complete",
        "output_directory": str(output_dir.absolute()),
        "coverage": "North America CONUS region"
    }))

    # CRITICAL: Return structured data for database
    result = {
    "records": all_extracted_data,
    "csv_files": {
        "GOES18_data": str(output_dir / "GOES18_CONUS_data.csv") if (output_dir / "GOES18_CONUS_data.csv").exists() else None,
        "GOES18_imagery": str(output_dir / "GOES18_CONUS_data_imagery.csv") if (output_dir / "GOES18_CONUS_data_imagery.csv").exists() else None,
        "GOES16_data": str(output_dir / "GOES16_CONUS_data.csv") if (output_dir / "GOES16_CONUS_data.csv").exists() else None,
        "GOES16_imagery": str(output_dir / "GOES16_CONUS_data_imagery.csv") if (output_dir / "GOES16_CONUS_data_imagery.csv").exists() else None,
                },
    "summary": {
        "total_files": len(all_files),
        "satellites": list(set([f['satellite'] for f in all_files])),
        "coverage": "CONUS (Continental US)",
        "processed_satellites": processed_satellites,
        "output_directory": str(output_dir.absolute()),
        "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    }

    return result







# -----------------------------------------------------------------------------------------------------------------------------------------------
# data of last 96 hours
# 4th last 


import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json


def fetch_cygnss_data(output_dir='cygnss_data', hours_back=96, json_output=True):
    """
    Complete CYGNSS data fetcher that creates synthetic wind data with temporal coverage.
    
    Args:
        output_dir (str): Directory to save output files
        hours_back (int): Number of hours of historical data to generate
        json_output (bool): Whether to print JSON output
    
    Returns:
        dict: Results dictionary with data statistics and file paths
    """
    
    def main():
        """Internal main function that executes the data fetching logic"""
        # Initialize results
        results = {
            'status': 'starting',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'mode': 'synthetic',
            'output_directory': os.path.abspath(output_dir),
            'hours_requested': hours_back
        }
        
        os.makedirs(output_dir, exist_ok=True)
        
        print("="*80)
        print(f"CYGNSS Level-3 Wind Data Fetcher (Past {hours_back} Hours)")
        print("="*80)
        print("\n⚠  NOTE: Real CYGNSS data is DAILY, not hourly!")
        print("    This synthetic data demonstrates hourly temporal resolution.")
        
        # Create synthetic sample data
        print(f"\nCreating synthetic CYGNSS sample data for past {hours_back} hours...")
        
        # Create sample data grid (reduced for performance)
        lat_min, lat_max = 10, 38
        lon_min, lon_max = -125, -60
        
        # Use coarser grid for better performance with many hours
        grid_spacing = 1.0 if hours_back > 48 else 0.5
        lats = np.arange(lat_min, lat_max, grid_spacing)
        lons = np.arange(lon_min, lon_max, grid_spacing)
        
        lon_grid, lat_grid = np.meshgrid(lons, lats)
        
        all_data = []
        current_time = datetime.now()
        
        # Generate data for each hour (going backwards in time)
        print(f"Generating {hours_back} hours of data...")
        for hour_offset in range(hours_back):
            timestamp = current_time - timedelta(hours=hour_offset)
            
            # Simulate wind speed with temporal and spatial variation
            base_wind = 8 + 5 * np.sin(2 * np.pi * hour_offset / 24)  # Daily cycle
            wind_speed = base_wind + 5 * np.random.random(lon_grid.shape)
            wind_speed += 2 * np.sin(np.radians(lat_grid)) * np.cos(np.radians(lon_grid))
            
            # Create DataFrame for this hour
            df_hour = pd.DataFrame({
                'timestamp': timestamp,
                'hours_ago': hour_offset,
                'latitude': lat_grid.flatten(),
                'longitude': lon_grid.flatten(),
                'wind_speed_ms': wind_speed.flatten(),
                'data_source': 'Synthetic_Sample'
            })
            
            all_data.append(df_hour)
            
            if (hour_offset + 1) % 50 == 0:
                print(f"  Generated {hour_offset + 1}/{hours_back} hours...")
        
        # Combine all hours
        df = pd.concat(all_data, ignore_index=True)
        
        # Save to CSV
        csv_file = os.path.join(output_dir, f'cygnss_synthetic_last_{hours_back}hours.csv')
        df.to_csv(csv_file, index=False)
        
        print(f"\n✓ Created synthetic sample: {csv_file}")
        print(f"  Total records: {len(df):,}")
        print(f"  Grid points per hour: {len(lats) * len(lons):,}")
        print(f"  Grid spacing: {grid_spacing}° ({len(lats)} x {len(lons)} grid)")
        
        # Show temporal coverage
        print(f"\n📅 TEMPORAL COVERAGE:")
        print(f"  Start: {df['timestamp'].min()}")
        print(f"  End:   {df['timestamp'].max()}")
        print(f"  Span:  {hours_back} hours")
        
        # Show samples from different time periods
        print(f"\n📊 SAMPLE DATA FROM DIFFERENT TIME PERIODS:")
        print("=" * 80)
        
        # Sample from start, middle, and end
        time_samples = [0, hours_back // 2, hours_back - 1]
        for hours_ago in time_samples:
            df_sample = df[df['hours_ago'] == hours_ago].head(3)
            print(f"\n🕐 {hours_ago} hours ago ({df_sample['timestamp'].iloc[0]}):")
            print(df_sample[['latitude', 'longitude', 'wind_speed_ms', 'hours_ago']].to_string(index=False))
        
        # Statistics by time period
        print(f"\n📈 WIND SPEED STATISTICS BY TIME PERIOD:")
        print("=" * 80)
        
        time_periods = [
            (0, 24, "Last 24 hours"),
            (24, 48, "24-48 hours ago"),
            (max(48, hours_back - 24), hours_back, f"Oldest data")
        ]
        
        for start_h, end_h, label in time_periods:
            if end_h <= hours_back:
                df_period = df[(df['hours_ago'] >= start_h) & (df['hours_ago'] < end_h)]
                if len(df_period) > 0:
                    print(f"\n{label}:")
                    print(f"  Records: {len(df_period):,}")
                    print(f"  Wind speed: {df_period['wind_speed_ms'].min():.2f} - {df_period['wind_speed_ms'].max():.2f} m/s")
                    print(f"  Mean: {df_period['wind_speed_ms'].mean():.2f} m/s")
        
        # Store results
        results['synthetic_data'] = {
            'csv_file': csv_file,
            'total_points': len(df),
            'hours_of_data': hours_back,
            'time_range': {
                'start': str(df['timestamp'].min()),
                'end': str(df['timestamp'].max()),
                'current_time': current_time.strftime('%Y-%m-%d %H:%M:%S')
            },
            'grid_size': {
                'latitude': len(lats),
                'longitude': len(lons),
                'spacing_degrees': grid_spacing
            },
            'points_per_hour': len(lats) * len(lons),
            'wind_speed_stats': {
                'min': float(df['wind_speed_ms'].min()),
                'max': float(df['wind_speed_ms'].max()),
                'mean': float(df['wind_speed_ms'].mean()),
                'std': float(df['wind_speed_ms'].std())
            },
            'spatial_extent': {
                'lat_min': lat_min,
                'lat_max': lat_max,
                'lon_min': lon_min,
                'lon_max': lon_max
            }
        }
        
        # Analyze temporal coverage
        print(f"\n{'='*80}")
        print("DETAILED TEMPORAL ANALYSIS")
        print('='*80)
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Group by hour
        hourly_summary = df.groupby('hours_ago').agg({
            'wind_speed_ms': ['count', 'mean', 'min', 'max'],
            'timestamp': 'first'
        }).reset_index()
        
        hourly_summary.columns = ['hours_ago', 'point_count', 'mean_wind', 'min_wind', 'max_wind', 'timestamp']
        
        print(f"\nShowing first 10 and last 10 hours:")
        print("\nFIRST 10 HOURS (Most Recent):")
        print(hourly_summary.head(10).to_string(index=False))
        
        print(f"\nLAST 10 HOURS (Oldest):")
        print(hourly_summary.tail(10).to_string(index=False))
        
        print(f"\n{'='*80}")
        print(f"Total unique hours: {len(hourly_summary)}")
        print(f"Data points per hour: {hourly_summary['point_count'].iloc[0]:,}")
        print('='*80)
        
        results['status'] = 'success'
        
        print(f"\n{'='*80}")
        print(f"✅ Output saved to: {os.path.abspath(output_dir)}")
        print('='*80)
        
        if json_output:
            print(f"\n{'='*80}")
            print("JSON OUTPUT")
            print('='*80)
            print(json.dumps(results, indent=2))
            print('='*80)
        
        print("\n" + "="*80)
        print("💡 IMPORTANT NOTES:")
        print("="*80)
        print("• This synthetic data covers the FULL 96-hour period")
        print("• Each hour has ~1,800 spatial points (28x65 grid)")
        print("• CYGNSS real data covers tropical regions only (±38° latitude)")
        print("• Real CYGNSS data is DAILY resolution, not hourly")
        print("• NASA data requires Earthdata authentication")
        print("="*80)
        print("\n📚 For REAL hourly wind data, consider:")
        print("  1. ERA5 (Copernicus Climate Data Store - free)")
        print("  2. NOAA GFS (Publicly accessible)")
        print("  3. OpenWeather API (Free tier available)")
        print("="*80)
        
        result1 = {
            "records": df.to_dict('records') if not df.empty else [],
            "temporal_analysis": hourly_summary.to_dict('records') if not hourly_summary.empty else [],
            "summary": {
                "status": "success",
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "mode": "synthetic",
                "csv_file": csv_file,  # Use csv_file instead of output_file
                "total_points": len(df) if not df.empty else 0,
                "hours_of_data": hours_back,  # Use hours_back instead of hours
                "time_range": {
                    "start": df['timestamp'].min().strftime('%Y-%m-%d %H:%M:%S') if not df.empty else None,
                    "end": df['timestamp'].max().strftime('%Y-%m-%d %H:%M:%S') if not df.empty else None,
                    "current_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                },
                "grid_size": {
                    "latitude": len(lats),  # Use len(lats)
                    "longitude": len(lons),  # Use len(lons)
                    "spacing_degrees": grid_spacing
                },
                "points_per_hour": len(lats) * len(lons) if not df.empty else 0,
                "wind_speed_stats": {
                    "min": float(df['wind_speed_ms'].min()) if not df.empty else None,
                    "max": float(df['wind_speed_ms'].max()) if not df.empty else None,
                    "mean": float(df['wind_speed_ms'].mean()) if not df.empty else None,
                    "std": float(df['wind_speed_ms'].std()) if not df.empty else None
                },
                "spatial_extent": {
                    "lat_min": lat_min,  # Use lat_min
                    "lat_max": lat_max,  # Use lat_max
                    "lon_min": lon_min,  # Use lon_min
                    "lon_max": lon_max   # Use lon_max
                }
            }
        }
        
        return result1

    
    # Call the internal main function
    return main()






# ----------------------------------------------------------------------------------------------------------------------------------------
# TEMPO 3




import os
import earthaccess, json
from datetime import datetime, timedelta, timezone



def run_fetch_tempo_o3():
    import re
    
    # Hardcoded credentials
    os.environ["EARTHDATA_USERNAME"] = "hassansiddiqui0946"
    os.environ["EARTHDATA_PASSWORD"] = "KiaAMQ840tends(0378)"
    os.environ["EARTHDATA_TOKEN"] = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEg..."

    # Login
    auth = earthaccess.login(strategy="environment")

    def parse_raw_granule(raw_str):
        """Parse the raw string format into structured data"""
        data = {}
        
        # Extract Collection info
        collection_match = re.search(r"'ShortName':\s*'([^']+)'.*'Version':\s*'([^']+)'", raw_str)
        if collection_match:
            data['short_name'] = collection_match.group(1)
            data['version'] = collection_match.group(2)
        
        # Extract temporal coverage
        time_match = re.search(r"'BeginningDateTime':\s*'([^']+)'.*'EndingDateTime':\s*'([^']+)'", raw_str)
        if time_match:
            data['time_start'] = time_match.group(1)
            data['time_end'] = time_match.group(2)
        
        # Extract size
        size_match = re.search(r"Size\(MB\):\s*(\d+\.?\d*)", raw_str)
        if size_match:
            data['size_mb'] = float(size_match.group(1))
        else:
            data['size_mb'] = 0.0
        
        # Extract data URL
        url_match = re.search(r"'(https://[^']+\.nc)'", raw_str)
        if url_match:
            data['data_url'] = url_match.group(1)
        
        data['raw_data'] = raw_str
        
        return data

    def fetch_tempo_o3(max_weeks=4, hours_back=72):
        now = datetime.now(timezone.utc)

        # Search time windows
        windows = [(now - timedelta(hours=hours_back), now)]
        for i in range(max_weeks):
            end = now - timedelta(days=7 * i)
            start = end - timedelta(days=7)
            windows.append((start, end))

        all_granules = []
        for start, end in windows:
            print(f"⏳ Searching TEMPO O₃TOT L2 between {start} → {end} ...")
            try:
                results = earthaccess.search_data(
                    short_name="TEMPO_O3TOT_L2",
                    temporal=(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")),
                    count=10
                )

                if results:
                    print(f"✅ Found {len(results)} granules in {start.date()} → {end.date()}")

                    for g in results:
                        raw_str = None
                        if hasattr(g, "data"):
                            raw_str = str(g.data)
                        elif hasattr(g, "_dict_"):
                            raw_str = str(g._dict_)
                        else:
                            raw_str = str(g)
                        
                        # Parse the raw string into structured data
                        parsed = parse_raw_granule(raw_str)
                        all_granules.append(parsed)

                    # Show first 2
                    print(json.dumps([{'raw': g['raw_data'][:500] + '...'} for g in all_granules[:2]], indent=2))

            except Exception as e:
                print(f"⚠ Error: {e}")
                continue

        if not all_granules:
            print("❌ No TEMPO O₃TOT L2 data found in tested windows")
            return None
        
        return {
            "records": all_granules,
            "summary": {
                "total_granules": len(all_granules),
                "product": "TEMPO_O3TOT_L2",
                "search_windows": len(windows),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        }

    # Execute and return
    return fetch_tempo_o3()



# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MISSING O3 OZONE DATA (latest 3-10-25  9:22)



def O3_OZONE_WAQI_DATA():
    import requests
    import pandas as pd
    from datetime import datetime, timedelta
    import time
    import json

    # ============================================================================
    # OPTION 1: WAQI (World Air Quality Index) - EASIEST & MOST RELIABLE
    # ============================================================================

    class WAQIOzoneFetcher:
        """
        Fetches O3 data using World Air Quality Index API
        FREE API - Get token at: https://aqicn.org/data-platform/token/
        """

        def __init__(self, api_token):
            self.api_token = api_token
            self.base_url = "https://api.waqi.info"

            # Major North American cities to query
            self.cities = [
                # USA Major Cities
                "losangeles", "newyork", "chicago", "houston", "phoenix",
                "philadelphia", "sanantonio", "sandiego", "dallas", "sanjose",
                "austin", "jacksonville", "fortworth", "columbus", "charlotte",
                "sanfrancisco", "indianapolis", "seattle", "denver", "washington",
                "boston", "elpaso", "detroit", "nashville", "portland",
                "lasvegas", "oklahomacity", "albuquerque", "tucson", "fresno",
                "sacramento", "kansas", "atlanta", "miami", "cleveland",

                # Canada Major Cities
                "toronto", "montreal", "vancouver", "calgary", "edmonton",
                "ottawa", "winnipeg", "quebec", "hamilton", "london",
                "victoria", "halifax", "saskatoon", "regina",

                # Mexico Major Cities
                "mexicocity", "guadalajara", "monterrey", "puebla", "tijuana",
                "leon", "juarez", "zapopan", "monterrey", "chihuahua"
            ]

        def fetch_city_data(self, city):
            """Fetch O3 data for a specific city"""
            url = f"{self.base_url}/feed/{city}/"
            params = {'token': self.api_token}

            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()

                if data.get('status') == 'ok':
                    return data.get('data')
                else:
                    return None

            except Exception as e:
                print(f"Error fetching {city}: {e}")
                return None

        def extract_o3_data(self, city_data, city_name):
            """Extract O3 specific data from city response"""
            if not city_data:
                return None

            # Get O3 data from iaqi (individual air quality index)
            iaqi = city_data.get('iaqi', {})
            o3_data = iaqi.get('o3', {})

            if not o3_data:
                return None

            record = {
                'city': city_name,
                'station_name': city_data.get('city', {}).get('name', ''),
                'latitude': city_data.get('city', {}).get('geo', [None, None])[0],
                'longitude': city_data.get('city', {}).get('geo', [None, None])[1],
                'datetime_utc': city_data.get('time', {}).get('iso'),
                'o3_aqi': o3_data.get('v'),
                'dominant_pollutant': city_data.get('dominentpol'),
                'overall_aqi': city_data.get('aqi'),
                'temperature': iaqi.get('t', {}).get('v'),
                'humidity': iaqi.get('h', {}).get('v'),
                'pressure': iaqi.get('p', {}).get('v'),
                'station_url': city_data.get('city', {}).get('url')
            }

            return record

        def fetch_all_data(self):
            """Fetch O3 data for all North American cities"""
            print("=" * 60)
            print("Fetching O3 Data from WAQI (World Air Quality Index)")
            print("=" * 60)

            all_records = []

            for idx, city in enumerate(self.cities, 1):
                print(f"[{idx}/{len(self.cities)}] Fetching {city}...")

                city_data = self.fetch_city_data(city)

                if city_data:
                    o3_record = self.extract_o3_data(city_data, city)
                    if o3_record and o3_record['o3_aqi'] is not None:
                        all_records.append(o3_record)
                        print(f"  ✓ O3 AQI: {o3_record['o3_aqi']}")
                    else:
                        print(f"  ⚠ No O3 data available")

                time.sleep(0.5)  # Respect API rate limits

            print(f"\n✓ Total records collected: {len(all_records)}")
            return all_records

        def save_to_csv(self, records, filename=None):
            """Save records to CSV"""
            if not records:
                print("No data to save")
                return None

            df = pd.DataFrame(records)

            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"north_america_o3_waqi_{timestamp}.csv"

            df.to_csv(filename, index=False)
            print(f"\n✓ Data saved to: {filename}")

            # Summary
            print("\n" + "=" * 60)
            print("Data Summary:")
            print("=" * 60)
            print(f"Total Stations: {len(df)}")
            print(f"Average O3 AQI: {df['o3_aqi'].mean():.2f}")
            print(f"Max O3 AQI: {df['o3_aqi'].max():.2f} at {df.loc[df['o3_aqi'].idxmax(), 'station_name']}")
            print(f"Min O3 AQI: {df['o3_aqi'].min():.2f} at {df.loc[df['o3_aqi'].idxmin(), 'station_name']}")

            return df

    # ============================================================================
    # MAIN EXECUTION
    # ============================================================================
    
    print("=" * 60)
    print("North America O3 (Ozone) Data Fetcher")
    print("Multiple Data Sources Available")
    print("=" * 60)

    WAQI_TOKEN = "50745193dad3cedc401993df279b3f5d645ec181"

    print("\n>>> Using WAQI (World Air Quality Index) <<<\n")
    fetcher = WAQIOzoneFetcher(api_token=WAQI_TOKEN)
    records = fetcher.fetch_all_data()
    df = fetcher.save_to_csv(records)

    if df is not None:
        print("\nFirst 5 records:")
        print(df.head())

        # Print full records as JSON response in terminal
        print("\nJSON Response:")
        print(json.dumps(records, indent=4))

    # CRITICAL: Return structured data for database insertion
    if records:
        result = {
            "records": records,
            "summary": {
                "total_stations": len(records),
                "average_o3_aqi": df['o3_aqi'].mean() if df is not None else None,
                "max_o3_aqi": df['o3_aqi'].max() if df is not None else None,
                "min_o3_aqi": df['o3_aqi'].min() if df is not None else None,
                "data_source": "WAQI",
                "timestamp": datetime.now().isoformat()
            }
        }
        return result
    else:
        return None





# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MISSING FIRE/SMOKE DETECTION DATA (latest 3-10-25  9:31)


def FIRE_SMOKE_DETECTION_DATA():
    import requests
    import pandas as pd
    from datetime import datetime, timedelta
    import csv
    import json
    from io import StringIO

    class FireDataFetcher:
        """
        Fetch fire/smoke detection data for North America from NASA FIRMS
        Requires a free MAP_KEY from https://firms.modaps.eosdis.nasa.gov/api/map_key/
        """

        def __init__(self, map_key):
            """
            Initialize with NASA FIRMS MAP_KEY

            Args:
                map_key (str): Your NASA FIRMS MAP_KEY
            """
            self.map_key = map_key
            self.base_url = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

        def fetch_fire_data(self, region="north_america", days=1, source="VIIRS_NOAA20_NRT"):
            """
            Fetch fire detection data for specified region and time period

            Args:
                region (str): Region name or coordinates (default: north_america)
                days (int): Number of days to fetch (1-10, default: 1 for last 24h)
                source (str): Data source - options:
                    - VIIRS_NOAA20_NRT (375m resolution, recommended)
                    - VIIRS_SNPP_NRT (375m resolution)
                    - MODIS_NRT (1km resolution)

            Returns:
                pandas.DataFrame: Fire detection data
            """

            # North America bounding box coordinates
            # Format: west,south,east,north
            north_america_bbox = "-170,15,-50,75"  # Covers USA, Canada, Mexico

            # Construct API URL
            url = f"{self.base_url}/{self.map_key}/{source}/{north_america_bbox}/{days}"

            print(f"Fetching data from: {url}")
            print(f"Time range: Last {days} day(s)")
            print(f"Source: {source}")
            print(f"Region: North America")
            print("-" * 50)

            try:
                # Fetch data
                response = requests.get(url, timeout=30)
                response.raise_for_status()

                # Parse CSV data
                lines = response.text.strip().split('\n')

                if len(lines) <= 1:
                    print("No fire detections found in the specified period.")
                    return pd.DataFrame()

                # Read into pandas DataFrame
                df = pd.read_csv(StringIO(response.text))

                print(f"\n✓ Successfully fetched {len(df)} fire detections")

                return df

            except requests.exceptions.RequestException as e:
                print(f"Error fetching data: {e}")
                return pd.DataFrame()

        def save_to_csv(self, df, filename=None):
            """
            Save DataFrame to CSV file

            Args:
                df (pandas.DataFrame): Data to save
                filename (str): Output filename (optional)
            """
            if df.empty:
                print("No data to save.")
                return

            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"fire_detections_north_america_{timestamp}.csv"

            df.to_csv(filename, index=False)
            print(f"\n✓ Data saved to: {filename}")

        def save_to_json(self, df, filename=None):
            """
            Save DataFrame to JSON file

            Args:
                df (pandas.DataFrame): Data to save
                filename (str): Output filename (optional)
            """
            if df.empty:
                print("No data to save.")
                return

            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"fire_detections_north_america_{timestamp}.json"

            df.to_json(filename, orient='records', indent=2)
            print(f"✓ Data saved to: {filename}")

        def display_json(self, df, limit=10):
            """
            Display data in JSON format on terminal

            Args:
                df (pandas.DataFrame): Data to display
                limit (int): Number of records to display (default: 10, use None for all)
            """
            if df.empty:
                print("\n❌ No data to display")
                return

            print("\n" + "="*50)
            print("JSON RESPONSE")
            print("="*50)

            # Convert to JSON
            if limit is not None and len(df) > limit:
                display_df = df.head(limit)
                json_data = display_df.to_dict(orient='records')
                print(f"(Showing first {limit} of {len(df)} records)\n")
            else:
                json_data = df.to_dict(orient='records')
                print(f"(Showing all {len(df)} records)\n")

            # Pretty print JSON
            print(json.dumps(json_data, indent=2, default=str))
            print("\n" + "="*50 + "\n")

        def display_summary(self, df):
            """Display summary statistics of the fire detection data"""
            if df.empty:
                return

            print("\n" + "="*50)
            print("FIRE DETECTION SUMMARY")
            print("="*50)
            print(f"Total detections: {len(df)}")
            print(f"Date range: {df['acq_date'].min()} to {df['acq_date'].max()}")

            if 'confidence' in df.columns:
                print(f"\nConfidence levels:")
                print(df['confidence'].value_counts())

            if 'bright_ti4' in df.columns:
                print(f"\nBrightness temperature (K):")
                print(f"  Mean: {df['bright_ti4'].mean():.2f}")
                print(f"  Max: {df['bright_ti4'].max():.2f}")
                print(f"  Min: {df['bright_ti4'].min():.2f}")

            if 'frp' in df.columns:
                print(f"\nFire Radiative Power (MW):")
                print(f"  Mean: {df['frp'].mean():.2f}")
                print(f"  Max: {df['frp'].max():.2f}")

            print("="*50 + "\n")

    # ==================== MAIN EXECUTION ====================

    MAP_KEY = "f53d9581c35b964e4bb5fa4ef85ced6f"

    # Initialize fetcher
    fetcher = FireDataFetcher(MAP_KEY)

    # Fetch fire data for North America (last 24 hours)
    fire_data = fetcher.fetch_fire_data(
        region="north_america",
        days=1,
        source="VIIRS_NOAA20_NRT"
    )

    # Display summary
    fetcher.display_summary(fire_data)

    # Display JSON response on terminal (first 10 records)
    fetcher.display_json(fire_data, limit=10)

    # Save to CSV
    fetcher.save_to_csv(fire_data, "north_america_fires_24h.csv")

    # Save to JSON file
    fetcher.save_to_json(fire_data, "north_america_fires_24h.json")

    # Optional: Display first few rows
    if not fire_data.empty:
        print("\nFirst 5 detections (Table view):")
        print(fire_data.head())

        print("\nColumn names:")
        print(fire_data.columns.tolist())

    # CRITICAL: Return structured data for database insertion
    if not fire_data.empty:
        result = {
            "records": fire_data.to_dict('records'),
            "summary": {
                "total_detections": len(fire_data),
                "date_range": {
                    "start": str(fire_data['acq_date'].min()),
                    "end": str(fire_data['acq_date'].max())
                },
                "data_source": "VIIRS_NOAA20_NRT",
                "region": "North America",
                "timestamp": datetime.now().isoformat(),
                "statistics": {
                    "mean_brightness": float(fire_data['bright_ti4'].mean()) if 'bright_ti4' in fire_data.columns else None,
                    "max_brightness": float(fire_data['bright_ti4'].max()) if 'bright_ti4' in fire_data.columns else None,
                    "mean_frp": float(fire_data['frp'].mean()) if 'frp' in fire_data.columns else None,
                    "max_frp": float(fire_data['frp'].max()) if 'frp' in fire_data.columns else None
                }
            }
        }
        return result
    else:
        return None





# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MISSING ENHANCE METEROLOGY DATA (latest 3-10-25  9:45)


def ENHANCE_METEROLOGY_DATA():
    import requests
    import pandas as pd
    from datetime import datetime, timedelta
    import numpy as np
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    import json
    from collections import deque

    class RateLimiter:
        """Smart rate limiter to stay under 600 requests per minute"""
        def __init__(self, max_per_minute=580):
            self.max_per_minute = max_per_minute
            self.requests = deque()
            self.lock = __import__('threading').Lock()

        def wait_if_needed(self):
            """Wait if we're about to exceed rate limit"""
            with self.lock:
                now = time.time()
                while self.requests and now - self.requests[0] > 60:
                    self.requests.popleft()

                if len(self.requests) >= self.max_per_minute:
                    sleep_time = 60 - (now - self.requests[0]) + 0.1
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                        now = time.time()
                        while self.requests and now - self.requests[0] > 60:
                            self.requests.popleft()

                self.requests.append(time.time())

    def generate_north_america_grid(lat_step=2, lon_step=2):
        """Generate a grid of coordinates covering North America"""
        latitudes = np.arange(15, 73, lat_step)
        longitudes = np.arange(-170, -49, lon_step)

        grid_points = []
        for lat in latitudes:
            for lon in longitudes:
                grid_points.append({
                    'latitude': float(lat),
                    'longitude': float(lon)
                })

        print(f"Generated {len(grid_points)} grid points covering North America")
        return grid_points

    def fetch_single_point(point, start_str, end_str, rate_limiter, max_retries=3):
        """Fetch weather data for a single point with rate limiting"""
        url = "https://api.open-meteo.com/v1/forecast"

        params = {
            'latitude': point['latitude'],
            'longitude': point['longitude'],
            'hourly': 'temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,pressure_msl,cloud_cover',
            'start_date': start_str,
            'end_date': end_str,
            'timezone': 'UTC'
        }

        for attempt in range(max_retries):
            try:
                rate_limiter.wait_if_needed()
                response = requests.get(url, params=params, timeout=15)

                if response.status_code == 429:
                    wait_time = (2 ** attempt) * 5
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()
                data = response.json()

                records = []
                if 'hourly' in data:
                    hourly = data['hourly']
                    times = hourly['time']

                    for i in range(len(times)):
                        record = {
                            'timestamp': times[i],
                            'latitude': point['latitude'],
                            'longitude': point['longitude'],
                            'temperature_c': hourly['temperature_2m'][i],
                            'humidity_percent': hourly['relative_humidity_2m'][i],
                            'precipitation_mm': hourly['precipitation'][i],
                            'wind_speed_kmh': hourly['wind_speed_10m'][i],
                            'pressure_hpa': hourly['pressure_msl'][i],
                            'cloud_cover_percent': hourly['cloud_cover'][i]
                        }
                        records.append(record)

                return records, None, 'success'

            except requests.exceptions.HTTPError as e:
                if response.status_code == 400:
                    return [], f"No data available (likely ocean)", 'no_data'
                elif attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    return [], f"HTTP Error: {str(e)}", 'error'
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    return [], f"Error: {str(e)}", 'error'

        return [], "Max retries exceeded", 'error'

    def fetch_weather_parallel(grid_points, output_file='north_america_weather_24h_optimized.csv', max_workers=10):
        """Fetch weather data with smart rate limiting"""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=24)
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        print(f"\n{'='*70}")
        print(f"RATE-LIMITED PARALLEL WEATHER DATA FETCHER")
        print(f"{'='*70}")
        print(f"Time Range: {start_date.strftime('%Y-%m-%d %H:%M')} to {end_date.strftime('%Y-%m-%d %H:%M')}")
        print(f"Grid Points: {len(grid_points)}")
        print(f"Parallel Workers: {max_workers}")
        print(f"Rate Limit: 580 requests/minute")
        print(f"{'='*70}\n")

        all_data = []
        errors = []
        no_data_points = []
        completed = 0
        total = len(grid_points)

        start_time = time.time()
        rate_limiter = RateLimiter(max_per_minute=580)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_point = {
                executor.submit(fetch_single_point, point, start_str, end_str, rate_limiter): point
                for point in grid_points
            }

            for future in as_completed(future_to_point):
                completed += 1
                point = future_to_point[future]

                try:
                    records, error, status = future.result()

                    if status == 'success':
                        all_data.extend(records)
                    elif status == 'no_data':
                        no_data_points.append(f"Lat {point['latitude']}, Lon {point['longitude']}")
                    else:
                        errors.append(f"Lat {point['latitude']}, Lon {point['longitude']}: {error}")

                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    eta = (total - completed) / rate if rate > 0 else 0

                    success_count = len(set([(r['latitude'], r['longitude']) for r in all_data]))
                    success_rate = (success_count / completed * 100) if completed > 0 else 0

                    print(f"Progress: {completed}/{total} ({completed/total*100:.1f}%) | "
                          f"Success: {success_rate:.1f}% | "
                          f"Rate: {rate:.1f} pts/sec | ETA: {eta/60:.1f}m | "
                          f"Records: {len(all_data):,}", end='\r')

                except Exception as e:
                    errors.append(f"Point {point}: {str(e)}")

        elapsed_time = time.time() - start_time
        df = pd.DataFrame(all_data)
        
        if not df.empty:
            df.to_csv(output_file, index=False)

        successful_points = len(set([(r['latitude'], r['longitude']) for r in all_data]))
        success_rate = (successful_points / total * 100) if total > 0 else 0

        print(f"\n\n{'='*70}")
        print(f"DATA COLLECTION COMPLETE")
        print(f"{'='*70}")
        print(f"Total Time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
        print(f"Total Records: {len(df):,}")
        print(f"Successful Points: {successful_points}/{total} ({success_rate:.1f}%)")
        print(f"Output File: {output_file}")

        json_summary = {
            "status": "success",
            "execution": {
                "total_time_seconds": round(elapsed_time, 2),
                "total_time_minutes": round(elapsed_time / 60, 2),
                "start_time": start_date.strftime('%Y-%m-%d %H:%M:%S'),
                "end_time": end_date.strftime('%Y-%m-%d %H:%M:%S')
            },
            "data_collection": {
                "total_records": len(df),
                "successful_points": successful_points,
                "no_data_points": len(no_data_points),
                "failed_points": len(errors),
                "total_grid_points": total,
                "success_rate_percent": round(success_rate, 2)
            }
        }

        if len(df) > 0:
            json_summary["weather_statistics"] = {
                "temperature_c": {
                    "min": round(float(df['temperature_c'].min()), 2),
                    "max": round(float(df['temperature_c'].max()), 2),
                    "mean": round(float(df['temperature_c'].mean()), 2)
                },
                "humidity_percent": {
                    "mean": round(float(df['humidity_percent'].mean()), 2)
                },
                "wind_speed_kmh": {
                    "mean": round(float(df['wind_speed_kmh'].mean()), 2)
                }
            }

            print(f"\nData Summary:")
            print(df[['temperature_c', 'humidity_percent', 'wind_speed_kmh']].describe().round(2))

        print(f"\nJSON SUMMARY:")
        print(json.dumps(json_summary, indent=2))

        return df, json_summary

    # MAIN EXECUTION
    print("RATE-LIMITED NORTH AMERICA WEATHER DATA FETCHER\n")
    
    grid = generate_north_america_grid(lat_step=2, lon_step=2)
    
    df, json_result = fetch_weather_parallel(
        grid_points=grid,
        output_file='north_america_weather_24h_optimized.csv',
        max_workers=10
    )

    # CRITICAL: Return structured data for database insertion
    if not df.empty:
        result = {
            "records": df.to_dict('records'),
            "summary": json_result
        }
        return result
    else:
        return None




# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MISSING SO2 DATA (latest 3-10-25  9:50)


def MISSING_SO2_DATA_WAQI():
    import requests
    import pandas as pd
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    import json

    # WAQI API Configuration
    API_TOKEN = "6ce9559b847280301baf94a0e946e403de1e6f75"

    def fetch_stations_by_bounds(token, bounds):
        url = f"https://api.waqi.info/v2/map/bounds"
        params = {
            'latlng': f"{bounds['lat_min']},{bounds['lng_min']},{bounds['lat_max']},{bounds['lng_max']}",
            'token': token
        }
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            print(f"\n{'='*80}")
            print(f"STATIONS BY BOUNDS - {bounds.get('name', 'Region')}")
            print(f"{'='*80}")
            print(json.dumps(data, indent=2))
            print(f"{'='*80}\n")

            return data.get('data', []) if data.get('status') == 'ok' else []
        except:
            return []

    def fetch_station_details(token, station_uid):
        url = f"https://api.waqi.info/feed/@{station_uid}/"
        try:
            response = requests.get(url, params={'token': token}, timeout=10)
            response.raise_for_status()
            data = response.json()

            print(f"\n{'='*80}")
            print(f"STATION DETAILS - UID: {station_uid}")
            print(f"{'='*80}")
            print(json.dumps(data, indent=2))
            print(f"{'='*80}\n")

            return data.get('data', {}) if data.get('status') == 'ok' else None
        except:
            return None

    def extract_pollutant_data(station_data, station_info, pollutant):
        if not station_data:
            return None
        iaqi = station_data.get('iaqi', {})
        if pollutant not in iaqi:
            return None
        pollutant_data = iaqi[pollutant]
        return {
            'station_uid': station_info.get('uid'),
            'station_name': station_data.get('city', {}).get('name', 'N/A'),
            'country': station_info.get('country', 'N/A'),
            'latitude': station_info.get('lat'),
            'longitude': station_info.get('lon'),
            'parameter': pollutant,
            'value': pollutant_data.get('v'),
            'aqi': station_info.get('aqi', 'N/A'),
            'timestamp_utc': station_data.get('time', {}).get('iso', 'N/A'),
            'timestamp_local': station_data.get('time', {}).get('s', 'N/A'),
            'dominant_pollutant': station_data.get('dominentpol', 'N/A'),
            'url': station_data.get('city', {}).get('url', 'N/A')
        }

    def fetch_station_with_pollutant(args):
        token, station, pollutant = args
        uid = station.get('uid')
        details = fetch_station_details(token, uid) if uid else None
        return extract_pollutant_data(details, station, pollutant) if details else None

    def fetch_north_america_data(token, pollutant, max_workers=20):
        print(f"\nFetching {pollutant.upper()} data for North America...")
        regions = [
            {'name': 'Northern USA & Canada', 'lat_min': 40, 'lat_max': 70, 'lng_min': -170, 'lng_max': -50},
            {'name': 'Southern USA', 'lat_min': 25, 'lat_max': 45, 'lng_min': -125, 'lng_max': -65},
            {'name': 'Mexico & Central America', 'lat_min': 10, 'lat_max': 30, 'lng_min': -120, 'lng_max': -80},
            {'name': 'Alaska', 'lat_min': 50, 'lat_max': 72, 'lng_min': -180, 'lng_max': -130},
        ]
        all_stations, station_uids = [], set()
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(fetch_stations_by_bounds, token, region): region for region in regions}
            for future in as_completed(futures):
                stations = future.result()
                for station in stations:
                    uid = station.get('uid')
                    if uid and uid not in station_uids:
                        station_uids.add(uid)
                        all_stations.append(station)

        pollutant_data = []
        station_args = [(token, station, pollutant) for station in all_stations]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for future in as_completed([executor.submit(fetch_station_with_pollutant, args) for args in station_args]):
                result = future.result()
                if result:
                    pollutant_data.append(result)
        return pollutant_data

    def save_to_csv(data, filename):
        if not data:
            return None
        df = pd.DataFrame(data).sort_values('timestamp_utc', ascending=False)
        df.to_csv(filename, index=False)
        print(f"Saved {len(df)} records to {filename}")
        return df

    # MAIN EXECUTION
    print("WAQI Air Quality Data Fetcher - North America (SO2 Only)")
    so2_data = fetch_north_america_data(API_TOKEN, 'so2')
    df = save_to_csv(so2_data, 'north_america_so2_latest.csv')

    # CRITICAL: Return structured data for database insertion
    if so2_data:
        result = {
            "records": so2_data,
            "summary": {
                "total_stations": len(so2_data),
                "parameter": "SO2",
                "region": "North America",
                "data_source": "WAQI",
                "timestamp": datetime.now().isoformat()
            }
        }
        return result
    else:
        return None





# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MISSING CO DATA (latest 3-10-25  9:56)



def MISSING_CO_DATA_WAQI():
    import requests
    import pandas as pd
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    import json

    # WAQI API Configuration
    API_TOKEN = "6ce9559b847280301baf94a0e946e403de1e6f75"

    def fetch_stations_by_bounds(token, bounds):
        url = f"https://api.waqi.info/v2/map/bounds"
        params = {
            'latlng': f"{bounds['lat_min']},{bounds['lng_min']},{bounds['lat_max']},{bounds['lng_max']}",
            'token': token
        }
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            print(f"\n{'='*80}")
            print(f"BOUNDS API RESPONSE - {bounds['name']}")
            print(f"{'='*80}")
            print(json.dumps(data, indent=2))
            print(f"{'='*80}\n")

            return data.get('data', []) if data.get('status') == 'ok' else []
        except Exception as e:
            print(f"Error fetching bounds: {e}")
            return []

    def fetch_station_details(token, station_uid):
        url = f"https://api.waqi.info/feed/@{station_uid}/"
        try:
            response = requests.get(url, params={'token': token}, timeout=10)
            response.raise_for_status()
            data = response.json()

            print(f"\n{'='*80}")
            print(f"STATION DETAILS API RESPONSE - UID: {station_uid}")
            print(f"{'='*80}")
            print(json.dumps(data, indent=2))
            print(f"{'='*80}\n")

            return data.get('data', {}) if data.get('status') == 'ok' else None
        except Exception as e:
            print(f"Error fetching station {station_uid}: {e}")
            return None

    def extract_pollutant_data(station_data, station_info, pollutant):
        if not station_data:
            return None
        iaqi = station_data.get('iaqi', {})
        if pollutant not in iaqi:
            return None
        pollutant_data = iaqi[pollutant]
        return {
            'station_uid': station_info.get('uid'),
            'station_name': station_data.get('city', {}).get('name', 'N/A'),
            'country': station_info.get('country', 'N/A'),
            'latitude': station_info.get('lat'),
            'longitude': station_info.get('lon'),
            'parameter': pollutant,
            'value': pollutant_data.get('v'),
            'aqi': station_info.get('aqi', 'N/A'),
            'timestamp_utc': station_data.get('time', {}).get('iso', 'N/A'),
            'timestamp_local': station_data.get('time', {}).get('s', 'N/A'),
            'dominant_pollutant': station_data.get('dominentpol', 'N/A'),
            'url': station_data.get('city', {}).get('url', 'N/A')
        }

    def fetch_station_with_pollutant(args):
        token, station, pollutant = args
        uid = station.get('uid')
        details = fetch_station_details(token, uid) if uid else None
        return extract_pollutant_data(details, station, pollutant) if details else None

    def fetch_north_america_data(token, pollutant, max_workers=20):
        print(f"\nFetching {pollutant.upper()} data for North America...")
        regions = [
            {'name': 'Northern USA & Canada', 'lat_min': 40, 'lat_max': 70, 'lng_min': -170, 'lng_max': -50},
            {'name': 'Southern USA', 'lat_min': 25, 'lat_max': 45, 'lng_min': -125, 'lng_max': -65},
            {'name': 'Mexico & Central America', 'lat_min': 10, 'lat_max': 30, 'lng_min': -120, 'lng_max': -80},
            {'name': 'Alaska', 'lat_min': 50, 'lat_max': 72, 'lng_min': -180, 'lng_max': -130},
        ]
        all_stations, station_uids = [], set()
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(fetch_stations_by_bounds, token, region): region for region in regions}
            for future in as_completed(futures):
                stations = future.result()
                for station in stations:
                    uid = station.get('uid')
                    if uid and uid not in station_uids:
                        station_uids.add(uid)
                        all_stations.append(station)

        pollutant_data = []
        station_args = [(token, station, pollutant) for station in all_stations]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for future in as_completed([executor.submit(fetch_station_with_pollutant, args) for args in station_args]):
                result = future.result()
                if result:
                    pollutant_data.append(result)
        return pollutant_data

    def save_to_csv(data, filename):
        if not data:
            return None
        df = pd.DataFrame(data).sort_values('timestamp_utc', ascending=False)
        df.to_csv(filename, index=False)
        print(f"Saved {len(df)} records to {filename}")
        return df

    # MAIN EXECUTION
    print("WAQI Air Quality Data Fetcher - North America (CO Only)")
    co_data = fetch_north_america_data(API_TOKEN, 'co')
    df = save_to_csv(co_data, 'north_america_co_latest.csv')

    # CRITICAL: Return structured data for database insertion
    if co_data:
        result = {
            "records": co_data,
            "summary": {
                "total_stations": len(co_data),
                "parameter": "CO",
                "region": "North America",
                "data_source": "WAQI",
                "timestamp": datetime.now().isoformat()
            }
        }
        return result
    else:
        return None




# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MISSING PM 2.5 DATA (latest 3-10-25  10:00)


def MISSING_PM_2_POINT_5_DATA():
    """
    PM2.5 Air Quality Data Fetcher for North America (WAQI version)
    Fetches real-time PM2.5 data for selected cities and exports to CSV
    """

    import requests
    import pandas as pd
    from datetime import datetime, timezone
    import time
    import json

    class PM25DataFetcher:
        def __init__(self):
            self.data = []

        def fetch_waqi_data(self, api_token, cities_coords):
            """Fetch PM2.5 data from WAQI API"""
            print("Fetching data from WAQI API...")

            all_data = []

            for city_name, lat, lon in cities_coords:
                url = f"https://api.waqi.info/feed/geo:{lat};{lon}/"
                params = {"token": api_token}

                try:
                    response = requests.get(url, params=params, timeout=10)

                    if response.status_code == 200:
                        data = response.json()

                        print(f"\n--- RAW JSON for {city_name} ---")
                        print(json.dumps(data, indent=2))
                        print("-----------------------------\n")

                        if data.get("status") == "ok":
                            iaqi = data["data"].get("iaqi", {})
                            pm25_value = iaqi.get("pm25", {}).get("v")

                            if pm25_value is not None:
                                ts = data["data"].get("time", {}).get("v")

                                if isinstance(ts, int):
                                    timestamp = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                                elif isinstance(ts, str):
                                    timestamp = ts
                                else:
                                    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

                                all_data.append({
                                    'timestamp': timestamp,
                                    'location': city_name,
                                    'city': city_name,
                                    'country': 'N/A',
                                    'pm25_value': pm25_value,
                                    'unit': 'μg/m³',
                                    'latitude': lat,
                                    'longitude': lon,
                                    'source': 'WAQI'
                                })

                    else:
                        print(f"Error {response.status_code} for {city_name}")

                except Exception as e:
                    print(f"Error fetching data for {city_name}: {str(e)}")

                time.sleep(0.3)

            self.data.extend(all_data)
            print(f"Total WAQI measurements fetched: {len(all_data)}")
            return all_data

        def save_to_csv(self, filename=None):
            """Save collected data to CSV file"""
            if not self.data:
                print("No data to save!")
                return None

            if filename is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'pm25_waqi_{timestamp}.csv'

            df = pd.DataFrame(self.data)
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors="coerce")
            df = df.sort_values('timestamp', ascending=False)
            df.to_csv(filename, index=False)
            
            print(f"\nData saved to: {filename}")
            print(f"Total records: {len(df)}")
            print("\n=== DATA SUMMARY ===")
            print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            print(f"Number of locations: {df['location'].nunique()}")
            print(f"\nPM2.5 Statistics (μg/m³):")
            print(f"  Mean: {df['pm25_value'].mean():.2f}")
            print(f"  Median: {df['pm25_value'].median():.2f}")
            print(f"  Min: {df['pm25_value'].min():.2f}")
            print(f"  Max: {df['pm25_value'].max():.2f}")

            return df

    # MAIN EXECUTION
    print("=" * 60)
    print("PM2.5 Data Fetcher for North America (WAQI)")
    print("Real-time Air Quality Data")
    print("=" * 60)
    print()

    fetcher = PM25DataFetcher()

    WAQI_API_KEY = "34f49677d64b59d165c5ed3354e8fedb5e6c302a"

    north_american_cities = [
        ("New York", 40.7128, -74.0060),
        ("Los Angeles", 34.0522, -118.2437),
        ("Chicago", 41.8781, -87.6298),
        ("Toronto", 43.6532, -79.3832),
        ("Mexico City", 19.4326, -99.1332),
        ("Vancouver", 49.2827, -123.1207),
        ("Houston", 29.7604, -95.3698),
        ("Phoenix", 33.4484, -112.0740),
        ("Montreal", 45.5017, -73.5673),
        ("San Francisco", 37.7749, -122.4194)
    ]

    fetcher.fetch_waqi_data(WAQI_API_KEY, north_american_cities)

    print("\n" + "=" * 60)
    df = fetcher.save_to_csv()
    print("=" * 60)

    # CRITICAL: Return structured data for database insertion
    if fetcher.data:
        result = {
            "records": fetcher.data,
            "summary": {
                "total_measurements": len(fetcher.data),
                "parameter": "PM2.5",
                "unit": "μg/m³",
                "cities_count": len(north_american_cities),
                "region": "North America",
                "data_source": "WAQI",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        }
        return result
    else:
        return None





# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MISSING VEHCILE EMISSION DATA WITH NOX AND VOCS (latest 3-10-25  10:41)


def MISSING_VEHICLE_EMISSION_DATA_WAQI_WITH_NOX_AND_VOCS():
    import requests
    import pandas as pd
    from datetime import datetime, timedelta
    import time
    import json
    from concurrent.futures import ThreadPoolExecutor, as_completed

    class WAQIVehicleEmissionDataFetcher:
        """Fetcher for 24-hour historical vehicle emission data from WAQI API"""

        def __init__(self, token, max_workers=10):
            self.token = token
            self.base_url = "https://api.waqi.info"
            self.stations_data = []
            self.max_workers = max_workers

        def get_stations_in_bounds(self, lat1, lng1, lat2, lng2):
            """Fetch all monitoring stations within specified geographical bounds"""
            url = f"{self.base_url}/map/bounds/?token={self.token}&latlng={lat1},{lng1},{lat2},{lng2}"

            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()

                if data['status'] == 'ok':
                    return data['data']
                else:
                    print(f"Error: {data.get('data', 'Unknown error')}")
                    return []
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                return []

        def get_station_historical_data(self, station_id):
            """Fetch 24-hour historical data for a specific station"""
            if isinstance(station_id, int) or str(station_id).isdigit():
                url = f"{self.base_url}/feed/@{station_id}/?token={self.token}"
            else:
                url = f"{self.base_url}/feed/{station_id}/?token={self.token}"

            try:
                response = requests.get(url, timeout=15)
                response.raise_for_status()
                data = response.json()

                if data['status'] == 'ok':
                    return data['data']
                else:
                    return None
            except requests.exceptions.RequestException:
                return None

        def extract_historical_vehicle_data(self, station_data, station_info):
            """Extract 24-hour historical data for vehicle emission pollutants"""
            if not station_data:
                return []

            records = []
            forecast = station_data.get('forecast', {})

            base_info = {
                'station_id': station_info.get('uid'),
                'station_name': station_data.get('city', {}).get('name', 'Unknown'),
                'latitude': station_info.get('lat'),
                'longitude': station_info.get('lon'),
                'source': station_data.get('attributions', [{}])[0].get('name', '') if station_data.get('attributions') else ''
            }

            pollutants = {
                'pm25': 'pm25_aqi',
                'pm10': 'pm10_aqi',
                'o3': 'o3_aqi',
                'no2': 'no2_aqi',
                'co': 'co_aqi',
                'so2': 'so2_aqi',
                'nox': 'nox_aqi',
                'benzene': 'benzene_aqi',
                'toluene': 'toluene_aqi',
                'xylene': 'xylene_aqi'
            }

            hourly_data = {}

            for pollutant, column_name in pollutants.items():
                if pollutant in forecast.get('daily', {}):
                    daily_data = forecast['daily'][pollutant]
                    for entry in daily_data:
                        timestamp = entry.get('day')
                        value = entry.get('avg')

                        if timestamp and value is not None:
                            if timestamp not in hourly_data:
                                hourly_data[timestamp] = base_info.copy()
                                hourly_data[timestamp]['timestamp'] = timestamp
                                hourly_data[timestamp]['aqi'] = station_info.get('aqi')

                            hourly_data[timestamp][column_name] = value

            now = datetime.now()
            cutoff_time = now - timedelta(hours=24)

            for timestamp, data in hourly_data.items():
                try:
                    data_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    if data_time >= cutoff_time:
                        records.append(data)
                except:
                    records.append(data)

            if not records:
                current_record = base_info.copy()
                current_record['timestamp'] = datetime.now().isoformat()
                current_record['aqi'] = station_info.get('aqi')

                iaqi = station_data.get('iaqi', {})
                for pollutant, column_name in pollutants.items():
                    value = iaqi.get(pollutant, {}).get('v')
                    if value is not None:
                        current_record[column_name] = value

                if any(current_record.get(col) is not None for col in pollutants.values()):
                    records.append(current_record)

            return records

        def process_station(self, uid, station, vehicle_only):
            """Process a single station for 24-hour historical data"""
            station_details = self.get_station_historical_data(uid)

            if station_details:
                records = self.extract_historical_vehicle_data(station_details, station)

                if records:
                    if vehicle_only:
                        vehicle_cols = [
                            'no2_aqi', 'co_aqi', 'pm25_aqi', 'pm10_aqi', 'nox_aqi',
                            'benzene_aqi', 'toluene_aqi', 'xylene_aqi'
                        ]
                        filtered_records = []
                        for record in records:
                            if any(record.get(col) is not None for col in vehicle_cols):
                                filtered_records.append(record)

                        if filtered_records:
                            return filtered_records, True
                        else:
                            return [], False
                    else:
                        return records, True
            return [], False

        def fetch_north_america_vehicle_emission_data(self, vehicle_only=True):
            """Fetch 24-hour historical vehicle emission data for North America"""
            print("Fetching stations in North America...")

            regions = [
                {'name': 'Continental USA', 'bounds': (25, -125, 49, -65)},
                {'name': 'Canada', 'bounds': (42, -141, 72, -52)},
                {'name': 'Mexico', 'bounds': (14, -118, 33, -86)},
                {'name': 'Alaska', 'bounds': (51, -170, 72, -130)},
                {'name': 'Central America', 'bounds': (7, -93, 18, -77)},
            ]

            all_stations = {}

            for region in regions:
                print(f"\nFetching stations in {region['name']}...")
                lat1, lng1, lat2, lng2 = region['bounds']
                stations = self.get_stations_in_bounds(lat1, lng1, lat2, lng2)

                for station in stations:
                    uid = station.get('uid')
                    if uid and uid not in all_stations:
                        all_stations[uid] = station

                print(f"Found {len(stations)} stations in {region['name']}")
                time.sleep(0.5)

            print(f"\nTotal unique stations found: {len(all_stations)}")
            print(f"Fetching 24-hour historical data using {self.max_workers} parallel workers...")

            all_records = []
            skipped = 0
            total = len(all_stations)
            processed = 0

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_station = {
                    executor.submit(self.process_station, uid, station, vehicle_only): uid
                    for uid, station in all_stations.items()
                }

                for future in as_completed(future_to_station):
                    processed += 1

                    try:
                        records, has_data = future.result()
                        if records:
                            all_records.extend(records)
                        elif not has_data and vehicle_only:
                            skipped += 1
                    except Exception as e:
                        print(f"Error processing station: {e}")

                    if processed % 50 == 0 or processed == total:
                        print(f"Progress: {processed}/{total} stations | Collected {len(all_records)} data points | Skipped {skipped}")

            self.stations_data = all_records
            print(f"\nProcessing complete!")
            print(f"Total data points collected: {len(all_records)}")
            return all_records

        def save_to_csv(self, filename='north_america_24h_vehicle_emission_data.csv'):
            """Save the collected 24-hour data to a CSV file"""
            if not self.stations_data:
                print("No data to save.")
                return None

            df = pd.DataFrame(self.stations_data)

            column_order = [
                'timestamp', 'station_id', 'station_name', 'latitude', 'longitude', 'aqi',
                'no2_aqi', 'co_aqi', 'pm25_aqi', 'pm10_aqi', 'nox_aqi',
                'benzene_aqi', 'toluene_aqi', 'xylene_aqi',
                'o3_aqi', 'so2_aqi', 'source'
            ]

            column_order = [col for col in column_order if col in df.columns]
            df = df[column_order]

            if 'timestamp' in df.columns:
                df['timestamp_parsed'] = pd.to_datetime(df['timestamp'], errors='coerce')
                df = df.sort_values(['station_id', 'timestamp_parsed'])
                df = df.drop('timestamp_parsed', axis=1)

            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"\nData saved to: {filename}")
            print(f"Total rows: {len(df)}")

            return df

    # MAIN EXECUTION
    TOKEN = "34f49677d64b59d165c5ed3354e8fedb5e6c302a"

    print("WAQI 24-Hour Historical Vehicle Emission Data Fetcher (with NOx & VOCs)")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    fetcher = WAQIVehicleEmissionDataFetcher(TOKEN, max_workers=10)
    data = fetcher.fetch_north_america_vehicle_emission_data(vehicle_only=True)

    if data:
        print(f"\nSample JSON Output (first 15 records):")
        print(json.dumps(data[:15], indent=2, ensure_ascii=False))

        df = fetcher.save_to_csv('north_america_24h_vehicle_emission_data.csv')

        # CRITICAL: Return structured data for database insertion
        result = {
            "records": data,
            "summary": {
                "total_data_points": len(data),
                "unique_stations": df['station_id'].nunique() if df is not None else 0,
                "parameters": ["NO2", "CO", "PM2.5", "PM10", "NOx", "Benzene", "Toluene", "Xylene"],
                "region": "North America",
                "time_range": "24 hours",
                "data_source": "WAQI",
                "timestamp": datetime.now().isoformat()
            }
        }
        return result
    else:
        return None







# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MISSING DATA NOx WAQI (latest 4-10-25  3:06)


def MISSING_DATA_NOx_WAQI():
  import requests
  import pandas as pd
  from datetime import datetime, timedelta
  import time
  import json
  from concurrent.futures import ThreadPoolExecutor, as_completed

  class WAQIVehicleEmissionDataFetcher:
    """
    Fetcher for 24-hour historical vehicle emission data from WAQI API
    Includes NOx
    """
    
    def __init__(self, token, max_workers=10):
        self.token = token
        self.base_url = "https://api.waqi.info"
        self.stations_data = []
        self.max_workers = max_workers
        
    def get_stations_in_bounds(self, lat1, lng1, lat2, lng2):
        """Fetch all monitoring stations within specified geographical bounds"""
        url = f"{self.base_url}/map/bounds/?token={self.token}&latlng={lat1},{lng1},{lat2},{lng2}"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] == 'ok':
                return data['data']
            else:
                print(f"Error: {data.get('data', 'Unknown error')}")
                return []
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return []
    
    def get_station_historical_data(self, station_id):
        """
        Fetch 24-hour historical data for a specific station
        Returns hourly/daily data for the past 24 hours
        """
        if isinstance(station_id, int) or str(station_id).isdigit():
            url = f"{self.base_url}/feed/@{station_id}/?token={self.token}"
        else:
            url = f"{self.base_url}/feed/{station_id}/?token={self.token}"
        
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] == 'ok':
                return data['data']
            else:
                return None
        except requests.exceptions.RequestException:
            return None
    
    def extract_historical_vehicle_data(self, station_data, station_info):
        """
        Extract 24-hour historical data for vehicle emission pollutants
        Returns a list of records (one per hour/day depending on availability)
        """
        if not station_data:
            return []
        
        records = []
        
        forecast = station_data.get('forecast', {})
        
        # Base station info
        base_info = {
            'station_id': station_info.get('uid'),
            'station_name': station_data.get('city', {}).get('name', 'Unknown'),
            'latitude': station_info.get('lat'),
            'longitude': station_info.get('lon'),
            'source': station_data.get('attributions', [{}])[0].get('name', '') if station_data.get('attributions') else ''
        }
        
        # Pollutants to extract (includes NOx)
        pollutants = {
            'pm25': 'pm25_aqi',
            'pm10': 'pm10_aqi',
            'o3': 'o3_aqi',
            'no2': 'no2_aqi',
            'co': 'co_aqi',
            'so2': 'so2_aqi',
            'nox': 'nox_aqi'   # ✅ NOx
        }
        
        # Collect all hourly/daily data points
        hourly_data = {}
        
        for pollutant, column_name in pollutants.items():
            if pollutant in forecast.get('daily', {}):
                daily_data = forecast['daily'][pollutant]
                for entry in daily_data:
                    timestamp = entry.get('day')
                    value = entry.get('avg')
                    
                    if timestamp and value is not None:
                        if timestamp not in hourly_data:
                            hourly_data[timestamp] = base_info.copy()
                            hourly_data[timestamp]['timestamp'] = timestamp
                            hourly_data[timestamp]['aqi'] = station_info.get('aqi')
                        
                        hourly_data[timestamp][column_name] = value
        
        # Convert to list and filter last 24 hours
        now = datetime.now()
        cutoff_time = now - timedelta(hours=24)
        
        for timestamp, data in hourly_data.items():
            try:
                data_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                if data_time >= cutoff_time:
                    records.append(data)
            except:
                records.append(data)
        
        # If no historical data available, fall back to current IAQI readings
        if not records:
            current_record = base_info.copy()
            current_record['timestamp'] = datetime.now().isoformat()
            current_record['aqi'] = station_info.get('aqi')
            
            iaqi = station_data.get('iaqi', {})
            for pollutant, column_name in pollutants.items():
                value = iaqi.get(pollutant, {}).get('v')
                if value is not None:
                    current_record[column_name] = value
            
            if any(current_record.get(col) is not None for col in pollutants.values()):
                records.append(current_record)
        
        return records
    
    def process_station(self, uid, station, vehicle_only):
        """Process a single station for 24-hour historical data"""
        station_details = self.get_station_historical_data(uid)
        
        if station_details:
            records = self.extract_historical_vehicle_data(station_details, station)
            
            if records:
                if vehicle_only:
                    vehicle_cols = ['no2_aqi', 'co_aqi', 'pm25_aqi', 'pm10_aqi', 'nox_aqi']  # ✅ include NOx
                    filtered_records = []
                    for record in records:
                        if any(record.get(col) is not None for col in vehicle_cols):
                            filtered_records.append(record)
                    
                    if filtered_records:
                        return filtered_records, True
                    else:
                        return [], False
                else:
                    return records, True
        return [], False
    
    def fetch_north_america_vehicle_emission_data(self, vehicle_only=True):
        """
        Fetch 24-hour historical vehicle emission data for North America (includes NOx)
        """
        print("Fetching stations in North America...")
        print("This will collect 24-hour historical data for each station (including NOx)...")
        
        regions = [
            {'name': 'Continental USA', 'bounds': (25, -125, 49, -65)},
            {'name': 'Canada', 'bounds': (42, -141, 72, -52)},
            {'name': 'Mexico', 'bounds': (14, -118, 33, -86)},
            {'name': 'Alaska', 'bounds': (51, -170, 72, -130)},
            {'name': 'Central America', 'bounds': (7, -93, 18, -77)},
        ]
        
        all_stations = {}
        
        for region in regions:
            print(f"\nFetching stations in {region['name']}...")
            lat1, lng1, lat2, lng2 = region['bounds']
            stations = self.get_stations_in_bounds(lat1, lng1, lat2, lng2)
            
            for station in stations:
                uid = station.get('uid')
                if uid and uid not in all_stations:
                    all_stations[uid] = station
            
            print(f"Found {len(stations)} stations in {region['name']}")
            time.sleep(0.5)
        
        print(f"\nTotal unique stations found: {len(all_stations)}")
        
        if vehicle_only:
            print("Filtering mode: Will only save stations WITH vehicle emission data (NOx included)")
        
        print(f"Fetching 24-hour historical data using {self.max_workers} parallel workers...")
        
        all_records = []
        skipped = 0
        total = len(all_stations)
        processed = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_station = {
                executor.submit(self.process_station, uid, station, vehicle_only): uid 
                for uid, station in all_stations.items()
            }
            
            for future in as_completed(future_to_station):
                processed += 1
                
                try:
                    records, has_data = future.result()
                    if records:
                        all_records.extend(records)
                    elif not has_data and vehicle_only:
                        skipped += 1
                except Exception as e:
                    print(f"Error processing station: {e}")
                
                if processed % 50 == 0 or processed == total:
                    print(f"Progress: {processed}/{total} stations | Collected {len(all_records)} data points | Skipped {skipped}")
        
        self.stations_data = all_records
        print(f"\n{'='*60}")
        print(f"Processing complete!")
        print(f"Total stations processed: {total}")
        print(f"Total data points collected: {len(all_records)}")
        if vehicle_only:
            print(f"Stations WITHOUT vehicle emission data (skipped): {skipped}")
        print(f"{'='*60}")
        return all_records
    
    def save_to_csv(self, filename='north_america_24h_vehicle_emission_data.csv'):
        """Save the collected 24-hour data to a CSV file"""
        if not self.stations_data:
            print("No data to save. Please fetch data first.")
            return
        
        df = pd.DataFrame(self.stations_data)
        
        column_order = [
            'timestamp', 'station_id', 'station_name', 'latitude', 'longitude', 'aqi',
            'no2_aqi', 'co_aqi', 'pm25_aqi', 'pm10_aqi', 'nox_aqi',
            'o3_aqi', 'so2_aqi', 'source'
        ]
        
        column_order = [col for col in column_order if col in df.columns]
        df = df[column_order]
        
        if 'timestamp' in df.columns:
            df['timestamp_parsed'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.sort_values(['station_id', 'timestamp_parsed'])
            df = df.drop('timestamp_parsed', axis=1)
        
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\nData saved to: {filename}")
        print(f"Total rows (hourly data points): {len(df)}")
        print(f"Unique stations: {df['station_id'].nunique()}")
        
        print("\n" + "="*60)
        print("24-HOUR DATA SUMMARY - VEHICLE EMISSION INDICATORS (includes NOx)")
        print("="*60)
        
        vehicle_pollutants = {
            'NO2 (Nitrogen Dioxide)': 'no2_aqi',
            'CO (Carbon Monoxide)': 'co_aqi',
            'PM2.5 (Fine Particulates)': 'pm25_aqi',
            'PM10 (Coarse Particulates)': 'pm10_aqi',
            'NOx (Nitrogen Oxides)': 'nox_aqi'
        }
        
        print("\n🚗 Vehicle Emission Pollutants (24h data):")
        for pollutant_name, pollutant_col in vehicle_pollutants.items():
            if pollutant_col in df.columns:
                count = df[pollutant_col].notna().sum()
                if count > 0:
                    print(f"\n  {pollutant_name}: {count} data points")
                    print(f"    Mean: {df[pollutant_col].mean():.2f} | "
                          f"Median: {df[pollutant_col].median():.2f} | "
                          f"Min: {df[pollutant_col].min():.2f} | "
                          f"Max: {df[pollutant_col].max():.2f}")
        
        print("="*60)
        
        return df


  def main():
    """Main execution function"""
    TOKEN = "34f49677d64b59d165c5ed3354e8fedb5e6c302a"
    
    print("="*60)
    print("WAQI 24-Hour Historical Vehicle Emission Data Fetcher (with NOx)")
    print("="*60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    print("\nFetching past 24 hours of data for:")
    print("  • NO2 (Nitrogen Dioxide)")
    print("  • CO (Carbon Monoxide)")
    print("  • PM2.5 & PM10 (Particulate matter)")
    print("  • NOx (Nitrogen Oxides)")
    print("="*60)
    
    fetcher = WAQIVehicleEmissionDataFetcher(TOKEN, max_workers=10)
    
    data = fetcher.fetch_north_america_vehicle_emission_data(vehicle_only=True)
    
    if data:
        # ✅ Print JSON sample (10–20 records)
        print("\n📜 Sample JSON Output (first 15 records):")
        print(json.dumps(data[:15], indent=2, ensure_ascii=False))
        
        df = fetcher.save_to_csv('north_america_24h_vehicle_emission_data.csv')
        
        if len(df) > 0:
            print("\nFirst 10 rows of data (as DataFrame):")
            print(df.head(10))
            print("\n✅ Success! 24-hour data collection complete (with NOx).")
            print(f"📁 File saved: north_america_24h_vehicle_emission_data.csv")
        else:
            print("\n⚠️ Warning: No data was collected.")
    else:
        print("\nNo data was retrieved. Please check your API token and internet connection.")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n💡 Note: WAQI API provides forecast/historical data in daily format.")
    print("   Some stations may have limited NOx data — often only current values (iaqi).")


  if __name__ == "__main__":
    main()


MISSING_DATA_NOx_WAQI()
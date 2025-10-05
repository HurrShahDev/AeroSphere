"""
Environmental Query System - Complete with SendGrid Email Alerts - FIXED
"""

import os
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
import time

# Load environment variables
load_dotenv()


# ============================================================================
# DATABASE MANAGER
# ============================================================================

class DatabaseManager:
    """Manages PostgreSQL database connections and queries."""
    
    def __init__(self, host="localhost", port="5000", dbname="db", 
                 user="db_user", password="db_password"):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn = None
    
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            print("Connected to PostgreSQL database")
            return self.conn
        except psycopg2.Error as e:
            print(f"Database connection failed: {e}")
            raise
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            print("Database connection closed")
    
    def query_air_quality(self, city: str, country: Optional[str], date: str) -> Dict[str, Any]:
        """Query air quality data for a location and date."""
        try:
            cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
            if country:
                query = """
                SELECT 
                    parameter_name,
                    AVG(value) as avg_value,
                    units,
                    city,
                    country,
                    latitude,
                    longitude,
                    datetime_utc,
                    DATE(datetime_utc) as data_date
                FROM air_quality_data
                WHERE LOWER(city) LIKE LOWER(%s)
                    AND LOWER(country) LIKE LOWER(%s)
                    AND DATE(datetime_utc) >= %s::date - INTERVAL '7 days'
                    AND DATE(datetime_utc) <= %s::date
                GROUP BY parameter_name, units, city, country, latitude, longitude, datetime_utc
                ORDER BY datetime_utc DESC
                LIMIT 50
                """
                cursor.execute(query, (f"%{city}%", f"%{country}%", date, date))
            else:
                query = """
                SELECT 
                    parameter_name,
                    AVG(value) as avg_value,
                    units,
                    city,
                    country,
                    latitude,
                    longitude,
                    datetime_utc,
                    DATE(datetime_utc) as data_date
                FROM air_quality_data
                WHERE LOWER(city) LIKE LOWER(%s)
                    AND DATE(datetime_utc) >= %s::date - INTERVAL '7 days'
                    AND DATE(datetime_utc) <= %s::date
                GROUP BY parameter_name, units, city, country, latitude, longitude, datetime_utc
                ORDER BY datetime_utc DESC
                LIMIT 50
                """
                cursor.execute(query, (f"%{city}%", date, date))
            
            results = cursor.fetchall()
            
            if not results:
                cursor.close()
                return None
            
            data = {
                'location': f"{results[0]['city']}, {results[0]['country']}",
                'latitude': results[0]['latitude'],
                'longitude': results[0]['longitude'],
                'date': date,
                'actual_date': str(results[0]['data_date']),
                'pollutants': {}
            }
            
            for row in results:
                param = row['parameter_name']
                if param not in data['pollutants']:
                    data['pollutants'][param] = {
                        'value': float(row['avg_value']),
                        'units': row['units']
                    }
            
            cursor.close()
            
            if 'pm25' in data['pollutants']:
                pm25 = data['pollutants']['pm25']['value']
                data['aqi'] = self.calculate_aqi_from_pm25(pm25)
            
            return data
            
        except Exception as e:
            print(f"Error querying air quality data: {e}")
            return None
    
    def calculate_aqi_from_pm25(self, pm25: float) -> Tuple[int, str]:
        """Calculate AQI and category from PM2.5 concentration."""
        if pm25 <= 12.0:
            aqi = (50 / 12.0) * pm25
            category = "Good"
        elif pm25 <= 35.4:
            aqi = 50 + ((100 - 50) / (35.4 - 12.1)) * (pm25 - 12.1)
            category = "Moderate"
        elif pm25 <= 55.4:
            aqi = 100 + ((150 - 100) / (55.4 - 35.5)) * (pm25 - 35.5)
            category = "Unhealthy for Sensitive Groups"
        elif pm25 <= 150.4:
            aqi = 150 + ((200 - 150) / (150.4 - 55.5)) * (pm25 - 55.5)
            category = "Unhealthy"
        elif pm25 <= 250.4:
            aqi = 200 + ((300 - 200) / (250.4 - 150.5)) * (pm25 - 150.5)
            category = "Very Unhealthy"
        else:
            aqi = 300 + ((500 - 300) / (500.4 - 250.5)) * (pm25 - 250.5)
            category = "Hazardous"
        
        return int(aqi), category
    
    def get_latest_air_quality(self, city: str, country: Optional[str] = None) -> Dict[str, Any]:
        """Get the most recent air quality data for monitoring."""
        today = datetime.now().strftime("%Y-%m-%d")
        return self.query_air_quality(city, country, today)
    
    def get_historical_average(self, city: str, country: Optional[str], hours: int = 6) -> Dict[str, Any]:
        """Get historical average for the past N hours."""
        try:
            cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
            time_threshold = datetime.now() - timedelta(hours=hours)
            
            query = """
            SELECT 
                parameter_name,
                AVG(value) as avg_value,
                units
            FROM air_quality_data
            WHERE LOWER(city) LIKE LOWER(%s)
                AND datetime_utc >= %s
                AND datetime_utc < NOW()
            GROUP BY parameter_name, units
            """
            
            cursor.execute(query, (f"%{city}%", time_threshold))
            results = cursor.fetchall()
            cursor.close()
            
            if not results:
                return {}
            
            averages = {}
            for row in results:
                param = row['parameter_name']
                averages[param] = {
                    'avg_value': float(row['avg_value']),
                    'units': row['units']
                }
            
            return averages
            
        except Exception as e:
            print(f"Error getting historical average: {e}")
            return {}
    
    def setup_alert_tables(self):
        """Create tables for alert subscriptions and sent alerts."""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS alert_subscriptions (
                    id SERIAL PRIMARY KEY,
                    user_email VARCHAR(255) NOT NULL,
                    city VARCHAR(100) NOT NULL,
                    country VARCHAR(100),
                    alert_types TEXT[] DEFAULT ARRAY['pm25', 'pm10', 'no2', 'o3', 'co', 'so2'],
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(user_email, city, country)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sent_alerts (
                    id SERIAL PRIMARY KEY,
                    user_email VARCHAR(255) NOT NULL,
                    city VARCHAR(100) NOT NULL,
                    country VARCHAR(100),
                    alert_type VARCHAR(50) NOT NULL,
                    severity VARCHAR(20) NOT NULL,
                    pollutant VARCHAR(50),
                    value FLOAT,
                    sent_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_sent_alerts_lookup 
                ON sent_alerts(user_email, city, pollutant, sent_at DESC)
            """)
            
            self.conn.commit()
            cursor.close()
            print("Alert tables created successfully")
            
        except Exception as e:
            print(f"Error creating alert tables: {e}")
            self.conn.rollback()
    
    def add_alert_subscription(self, email: str, city: str, country: Optional[str] = None, 
                              alert_types: Optional[List[str]] = None):
        """Add a new alert subscription."""
        try:
            cursor = self.conn.cursor()
            
            if alert_types is None:
                alert_types = ['pm25', 'pm10', 'no2', 'o3', 'co', 'so2']
            
            cursor.execute("""
                INSERT INTO alert_subscriptions (user_email, city, country, alert_types)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_email, city, country) 
                DO UPDATE SET is_active = TRUE, alert_types = EXCLUDED.alert_types
            """, (email, city, country, alert_types))
            
            self.conn.commit()
            cursor.close()
            print(f"Alert subscription added: {email} -> {city}")
            return True
            
        except Exception as e:
            print(f"Error adding subscription: {e}")
            self.conn.rollback()
            return False
    
    def get_active_subscriptions(self) -> List[Dict[str, Any]]:
        """Get all active alert subscriptions."""
        try:
            cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT user_email, city, country, alert_types
                FROM alert_subscriptions
                WHERE is_active = TRUE
            """)
            
            results = cursor.fetchall()
            cursor.close()
            return results
            
        except Exception as e:
            print(f"Error getting subscriptions: {e}")
            return []
    
    def was_alert_sent_recently(self, email: str, city: str, pollutant: str, hours: int = 2) -> bool:
        """Check if similar alert was sent recently to avoid spam."""
        try:
            cursor = self.conn.cursor()
            
            time_threshold = datetime.now() - timedelta(hours=hours)
            
            cursor.execute("""
                SELECT COUNT(*) FROM sent_alerts
                WHERE user_email = %s
                    AND city = %s
                    AND pollutant = %s
                    AND sent_at >= %s
            """, (email, city, pollutant, time_threshold))
            
            count = cursor.fetchone()[0]
            cursor.close()
            
            return count > 0
            
        except Exception as e:
            print(f"Error checking sent alerts: {e}")
            return False
    
    def log_sent_alert(self, email: str, city: str, country: Optional[str], 
                      alert_type: str, severity: str, pollutant: str, value: float):
        """Log that an alert was sent."""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                INSERT INTO sent_alerts (user_email, city, country, alert_type, severity, pollutant, value)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (email, city, country, alert_type, severity, pollutant, value))
            
            self.conn.commit()
            cursor.close()
            
        except Exception as e:
            print(f"Error logging sent alert: {e}")
            self.conn.rollback()


# ============================================================================
# ALERT GENERATOR
# ============================================================================

class AlertGenerator:
    """Detects air quality threshold violations and anomalies."""
    
    THRESHOLDS = {
        'pm25': {
            'good': 12.0,
            'moderate': 35.4,
            'unhealthy_sensitive': 55.4,
            'unhealthy': 150.4,
            'very_unhealthy': 250.4
        },
        'pm10': {
            'good': 54,
            'moderate': 154,
            'unhealthy_sensitive': 254,
            'unhealthy': 354,
            'very_unhealthy': 424
        },
        'no2': {
            'good': 53,
            'moderate': 100,
            'unhealthy_sensitive': 360,
            'unhealthy': 649,
            'very_unhealthy': 1249
        },
        'o3': {
            'good': 54,
            'moderate': 70,
            'unhealthy_sensitive': 85,
            'unhealthy': 105,
            'very_unhealthy': 200
        },
        'co': {
            'good': 4.4,
            'moderate': 9.4,
            'unhealthy_sensitive': 12.4,
            'unhealthy': 15.4,
            'very_unhealthy': 30.4
        },
        'so2': {
            'good': 35,
            'moderate': 75,
            'unhealthy_sensitive': 185,
            'unhealthy': 304,
            'very_unhealthy': 604
        }
    }
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def check_thresholds(self, current_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check if any pollutants exceed thresholds."""
        if not current_data or 'pollutants' not in current_data:
            return []
        
        alerts = []
        
        for pollutant, data in current_data['pollutants'].items():
            value = data['value']
            threshold = self.THRESHOLDS.get(pollutant)
            
            if not threshold:
                continue
            
            severity = None
            message = None
            
            if value > threshold.get('very_unhealthy', float('inf')):
                severity = 'HAZARDOUS'
                message = f'{pollutant.upper()} is at hazardous levels ({value:.1f})!'
            elif value > threshold.get('unhealthy', float('inf')):
                severity = 'VERY_UNHEALTHY'
                message = f'{pollutant.upper()} is very unhealthy ({value:.1f})'
            elif value > threshold.get('unhealthy_sensitive', float('inf')):
                severity = 'UNHEALTHY'
                message = f'{pollutant.upper()} is unhealthy ({value:.1f})'
            elif value > threshold.get('moderate', float('inf')):
                severity = 'WARNING'
                message = f'{pollutant.upper()} is elevated ({value:.1f})'
            
            if severity:
                alerts.append({
                    'type': 'THRESHOLD',
                    'pollutant': pollutant,
                    'value': value,
                    'units': data['units'],
                    'severity': severity,
                    'message': message
                })
        
        return alerts
    
    def detect_sudden_changes(self, city: str, country: Optional[str], 
                             current_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect sudden spikes or drops in pollutant levels."""
        if not current_data or 'pollutants' not in current_data:
            return []
        
        historical = self.db_manager.get_historical_average(city, country, hours=6)
        
        if not historical:
            return []
        
        alerts = []
        
        for pollutant, current_info in current_data['pollutants'].items():
            current_value = current_info['value']
            
            if pollutant not in historical:
                continue
            
            historical_avg = historical[pollutant]['avg_value']
            
            if historical_avg < 0.1:
                continue
            
            change_percent = ((current_value - historical_avg) / historical_avg) * 100
            
            if change_percent > 50:
                alerts.append({
                    'type': 'SPIKE',
                    'pollutant': pollutant,
                    'current_value': current_value,
                    'historical_avg': historical_avg,
                    'change_percent': change_percent,
                    'severity': 'WARNING',
                    'message': f'{pollutant.upper()} spiked {change_percent:.0f}% in last 6 hours'
                })
            elif change_percent < -40:
                alerts.append({
                    'type': 'DROP',
                    'pollutant': pollutant,
                    'current_value': current_value,
                    'historical_avg': historical_avg,
                    'change_percent': abs(change_percent),
                    'severity': 'INFO',
                    'message': f'{pollutant.upper()} dropped {abs(change_percent):.0f}% (improving)'
                })
        
        return alerts
    
    def generate_alerts(self, city: str, country: Optional[str], 
                       current_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate all alerts for current data."""
        threshold_alerts = self.check_thresholds(current_data)
        spike_alerts = self.detect_sudden_changes(city, country, current_data)
        
        all_alerts = threshold_alerts + spike_alerts
        
        severity_order = {'HAZARDOUS': 0, 'VERY_UNHEALTHY': 1, 'UNHEALTHY': 2, 
                         'WARNING': 3, 'INFO': 4}
        all_alerts.sort(key=lambda x: severity_order.get(x['severity'], 99))
        
        return all_alerts


# ============================================================================
# EMAIL SENDER - SENDGRID
# ============================================================================

class EmailSender:
    """Sends alert emails via SendGrid."""
    
    def __init__(self):
        self.sendgrid_api_key = os.getenv("SENDGRID_API_KEY")
        self.sender_email = os.getenv("SENDER_EMAIL") or os.getenv("SENDGRID_FROM_EMAIL")
        
        if not self.sendgrid_api_key:
            print("WARNING: SENDGRID_API_KEY not found in environment variables")
        
        if not self.sender_email:
            print("WARNING: Sender email not found in environment variables")
    
    def send_alert_email(self, recipient: str, subject: str, html_body: str) -> bool:
        """Send an HTML email alert via SendGrid."""
        if not self.sendgrid_api_key or not self.sender_email:
            print(f"Cannot send email: Missing SendGrid credentials")
            return False
        
        try:
            message = Mail(
                from_email=Email(self.sender_email),
                to_emails=To(recipient),
                subject=subject,
                html_content=Content("text/html", html_body)
            )
            
            sg = SendGridAPIClient(self.sendgrid_api_key)
            response = sg.send(message)
            
            if response.status_code in [200, 202]:
                print(f"Email sent to {recipient} (Status: {response.status_code})")
                return True
            else:
                print(f"Unexpected response: {response.status_code}")
                return False
            
        except Exception as e:
            print(f"Failed to send email to {recipient}: {e}")
            return False


# ============================================================================
# CONTINUOUS MONITOR
# ============================================================================

class ContinuousMonitor:
    """Monitors air quality continuously and sends alerts."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.alert_generator = AlertGenerator(db_manager)
        self.email_sender = EmailSender()
        self.llm = AzureChatOpenAI(
            deployment_name="gpt-4o",
            api_version="2024-12-01-preview",
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "https://hassan-siddiqui.openai.azure.com/"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY"),
            temperature=0.7
        )
    
    def monitor_all_subscriptions(self):
        """Check all active subscriptions and send alerts if needed."""
        print(f"\n{'='*60}")
        print(f"Monitoring Check: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        subscriptions = self.db_manager.get_active_subscriptions()
        
        if not subscriptions:
            print("No active subscriptions found")
            return
        
        print(f"Checking {len(subscriptions)} subscription(s)...")
        
        for sub in subscriptions:
            self.check_subscription(sub)
    
    def check_subscription(self, subscription: Dict[str, Any]):
        """Check a single subscription and send alert if needed."""
        email = subscription['user_email']
        city = subscription['city']
        country = subscription.get('country')
        
        print(f"\n  Checking: {city} for {email}")
        
        current_data = self.db_manager.get_latest_air_quality(city, country)
        
        if not current_data:
            print(f"    No data available for {city}")
            return
        
        alerts = self.alert_generator.generate_alerts(city, country, current_data)
        
        if not alerts:
            print(f"    All levels normal in {city}")
            return
        
        alerts_to_send = []
        for alert in alerts:
            if alert['severity'] == 'INFO':
                continue
            
            pollutant = alert['pollutant']
            if not self.db_manager.was_alert_sent_recently(email, city, pollutant, hours=2):
                alerts_to_send.append(alert)
        
        if not alerts_to_send:
            print(f"    Alerts detected but already notified recently")
            return
        
        print(f"    {len(alerts_to_send)} alert(s) to send!")
        self.send_alert_notification(email, city, country, current_data, alerts_to_send)
    
    def send_alert_notification(self, email: str, city: str, country: Optional[str],
                               current_data: Dict[str, Any], alerts: List[Dict[str, Any]]):
        """Generate and send alert email using LLM."""
        
        email_html = self.generate_alert_email_body(city, current_data, alerts)
        
        highest_severity = alerts[0]['severity']
        severity_emoji = {
            'HAZARDOUS': 'URGENT',
            'VERY_UNHEALTHY': 'ALERT',
            'UNHEALTHY': 'WARNING',
            'WARNING': 'NOTICE'
        }
        
        prefix = severity_emoji.get(highest_severity, 'ALERT')
        subject = f"{prefix}: Air Quality Alert - {city}"
        
        success = self.email_sender.send_alert_email(email, subject, email_html)
        
        if success:
            for alert in alerts:
                self.db_manager.log_sent_alert(
                    email=email,
                    city=city,
                    country=country,
                    alert_type=alert['type'],
                    severity=alert['severity'],
                    pollutant=alert['pollutant'],
                    value=alert.get('value', alert.get('current_value', 0))
                )
    
    def generate_alert_email_body(self, city: str, current_data: Dict[str, Any], 
                                  alerts: List[Dict[str, Any]]) -> str:
        """Use LLM to generate natural language alert email."""
        
        pollutant_details = []
        for pollutant, data in current_data.get('pollutants', {}).items():
            pollutant_details.append(f"- {pollutant.upper()}: {data['value']:.1f} {data['units']}")
        
        pollutants_text = "\n".join(pollutant_details)
        
        alert_summary = []
        for alert in alerts:
            if alert['type'] == 'THRESHOLD':
                alert_summary.append(
                    f"- {alert['pollutant'].upper()}: {alert['value']:.1f} {alert['units']} "
                    f"({alert['severity'].replace('_', ' ')})"
                )
            elif alert['type'] == 'SPIKE':
                alert_summary.append(
                    f"- {alert['pollutant'].upper()}: Spiked {alert['change_percent']:.0f}% "
                    f"to {alert['current_value']:.1f}"
                )
        
        alert_text = "\n".join(alert_summary)
        
        aqi_info = ""
        if current_data.get('aqi'):
            aqi_value, aqi_category = current_data['aqi']
            aqi_info = f"AQI: {aqi_value} ({aqi_category})"
        
        prompt = f"""Generate a comprehensive air quality report email for {city}.

CURRENT AIR QUALITY LEVELS:
{pollutants_text}
{aqi_info}

ALERTS DETECTED:
{alert_text}

LOCATION: {current_data.get('location', city)}
TIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Create an HTML email that includes:
1. A clear status headline stating overall air quality (Good/Moderate/Unhealthy/Hazardous)
2. Current pollutant levels with their status
3. Specific health risks based on the pollution levels
4. DETAILED health recommendations:
   - For HAZARDOUS/VERY UNHEALTHY: Stay indoors, seal windows, use air purifiers, wear N95 masks if must go out, avoid all outdoor exercise
   - For UNHEALTHY: Limit outdoor activities, wear masks outdoors, sensitive groups stay inside, close windows
   - For MODERATE: Sensitive groups should reduce prolonged outdoor exertion
   - For GOOD: Safe for outdoor activities
5. Who is most at risk (children, elderly, people with respiratory conditions)
6. Use HTML formatting: <h2> for title, <h3> for sections, <p> for text, <strong> for emphasis, <ul><li> for recommendations
7. Use color coding: red for hazardous, orange for unhealthy, yellow for moderate, green for good
8. Keep professional but caring tone

IMPORTANT: Generate ONLY the HTML body content. Make it informative and actionable."""

        try:
            response = self.llm.invoke(prompt)
            email_body = response.content
            
            full_html = f"""<!DOCTYPE html>
<html>
<head>
<style>
body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px; }}
h2 {{ color: #d32f2f; }}
.footer {{ margin-top: 20px; padding-top: 10px; border-top: 1px solid #ddd; font-size: 12px; color: #666; }}
</style>
</head>
<body>
{email_body}
<div class="footer">
<p><em>This is an automated air quality monitoring alert.</em></p>
</div>
</body>
</html>"""
            
            return full_html
            
        except Exception as e:
            print(f"Error generating email with LLM: {e}")
            return self.generate_fallback_email(city, current_data, alerts)
    
    def generate_fallback_email(self, city: str, current_data: Dict[str, Any], 
                                alerts: List[Dict[str, Any]]) -> str:
        """Fallback email template if LLM fails."""
        
        highest_severity = alerts[0]['severity'] if alerts else 'MODERATE'
        
        colors = {
            'HAZARDOUS': '#8B0000',
            'VERY_UNHEALTHY': '#FF4500',
            'UNHEALTHY': '#FFA500',
            'WARNING': '#FFD700',
            'INFO': '#228B22'
        }
        
        color = colors.get(highest_severity, '#FFA500')
        
        pollutant_items = ""
        for pollutant, data in current_data.get('pollutants', {}).items():
            pollutant_items += f"<li><strong>{pollutant.upper()}</strong>: {data['value']:.1f} {data['units']}</li>"
        
        if highest_severity in ['HAZARDOUS', 'VERY_UNHEALTHY']:
            recommendations = """<li><strong>STAY INDOORS:</strong> Do not go outside unless absolutely necessary</li>
<li><strong>SEAL YOUR HOME:</strong> Close all windows and doors</li>
<li><strong>USE AIR PURIFIERS:</strong> Run them on high settings if available</li>
<li><strong>WEAR N95 MASKS:</strong> If you must go outside, wear a properly fitted N95 or KN95 mask</li>
<li><strong>NO OUTDOOR EXERCISE:</strong> Avoid all outdoor physical activities</li>
<li><strong>MONITOR SYMPTOMS:</strong> Watch for breathing difficulties, chest pain, or dizziness</li>"""
            at_risk = "Everyone is at risk, especially children, elderly, pregnant women, and people with heart or lung conditions."
            status_text = "HAZARDOUS AIR QUALITY"
        elif highest_severity == 'UNHEALTHY':
            recommendations = """<li><strong>LIMIT OUTDOOR TIME:</strong> Reduce prolonged outdoor activities</li>
<li><strong>WEAR A MASK:</strong> Use N95 masks when going outside</li>
<li><strong>KEEP WINDOWS CLOSED:</strong> Minimize outdoor air coming inside</li>
<li><strong>SENSITIVE GROUPS STAY INSIDE:</strong> Children, elderly, and those with respiratory conditions should remain indoors</li>
<li><strong>AVOID STRENUOUS EXERCISE:</strong> Do light activities only if you must be outside</li>"""
            at_risk = "Children, elderly, pregnant women, and people with asthma, COPD, or heart disease should stay indoors."
            status_text = "UNHEALTHY AIR QUALITY"
        elif highest_severity == 'WARNING':
            recommendations = """<li><strong>SENSITIVE GROUPS BE CAUTIOUS:</strong> People with respiratory conditions should limit prolonged outdoor exertion</li>
<li><strong>MONITOR AIR QUALITY:</strong> Check AQI before planning outdoor activities</li>
<li><strong>CONSIDER MASKS:</strong> Vulnerable individuals may want to wear masks outdoors</li>
<li><strong>REDUCE OUTDOOR EXERCISE:</strong> Especially for sensitive groups</li>"""
            at_risk = "People with asthma or respiratory conditions should take precautions."
            status_text = "MODERATE AIR QUALITY"
        else:
            recommendations = """<li><strong>AIR QUALITY IS GOOD:</strong> Safe for all outdoor activities</li>
<li><strong>ENJOY OUTDOOR ACTIVITIES:</strong> No restrictions necessary</li>
<li><strong>VENTILATE YOUR HOME:</strong> It's safe to open windows</li>"""
            at_risk = "Air quality is safe for everyone."
            status_text = "GOOD AIR QUALITY"
        
        aqi_display = ""
        if current_data.get('aqi'):
            aqi_value, aqi_category = current_data['aqi']
            aqi_display = f"<p><strong>Air Quality Index:</strong> {aqi_value} ({aqi_category})</p>"
        
        return f"""<!DOCTYPE html>
<html>
<head>
<style>
body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px; }}
h2 {{ color: {color}; margin-bottom: 10px; }}
h3 {{ color: #555; margin-top: 20px; margin-bottom: 10px; }}
.status-box {{ background: #f8f9fa; border-left: 4px solid {color}; padding: 15px; margin: 15px 0; }}
.warning-box {{ background: #fff3cd; border: 1px solid #ffc107; padding: 15px; margin: 15px 0; border-radius: 5px; }}
ul {{ padding-left: 20px; }}
li {{ margin: 10px 0; }}
.footer {{ margin-top: 30px; padding-top: 15px; border-top: 1px solid #ddd; font-size: 12px; color: #666; }}
</style>
</head>
<body>
<h2>{status_text}: {city}</h2>
{aqi_display}
<div class="status-box">
<h3>Current Pollution Levels:</h3>
<ul>
{pollutant_items}
</ul>
</div>
<h3>Health Recommendations:</h3>
<ul>
{recommendations}
</ul>
<div class="warning-box">
<strong>Who is at Risk:</strong><br>
{at_risk}
</div>
<div class="footer">
<p><em>This is an automated air quality alert. Monitor local conditions and follow official health advisories.</em></p>
<p><em>Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em></p>
</div>
</body>
</html>"""


# ============================================================================
# ALERT MANAGEMENT SYSTEM
# ============================================================================

class AlertManagementSystem:
    """Manages alert subscriptions and monitoring scheduler."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.monitor = None
        self.scheduler = None
        self.is_running = False
        self.user_email = None
    
    def setup(self):
        """Initialize alert system tables and get user email."""
        print("\n" + "="*60)
        print("SETTING UP ALERT SYSTEM")
        print("="*60)
        self.db_manager.setup_alert_tables()
        
        self._get_user_email_from_input()
        
        self.monitor = ContinuousMonitor(self.db_manager)
        
        print("Alert system ready")
    
    def _get_user_email_from_input(self):
        """Get user email from terminal input."""
        print("\nEMAIL SETUP")
        print("-" * 60)
        
        while True:
            email = input("Enter your email address for receiving alerts: ").strip()
            
            if not email:
                print("Email cannot be empty. Please try again.")
                continue
            
            if '@' not in email or '.' not in email.split('@')[1]:
                print("Invalid email format. Please enter a valid email address.")
                continue
            
            print(f"\nEmail entered: {email}")
            confirm = input("Is this correct? (yes/no): ").strip().lower()
            
            if confirm in ['yes', 'y']:
                self.user_email = email
                print(f"Email configured: {email}")
                break
            else:
                print("Let's try again...\n")
    
    def add_city_subscription(self, city: str, country: Optional[str] = None):
        """Add city to monitor for the user's email."""
        if not self.user_email:
            print("User email not configured. Run setup() first.")
            return False
        
        success = self.db_manager.add_alert_subscription(self.user_email, city, country)
        if success:
            print(f"Now monitoring {city} for {self.user_email}")
            return True
        return False
    
    def start_monitoring(self, interval_minutes: int = 15):
        """Start continuous monitoring with specified interval."""
        if not self.user_email:
            print("User email not configured. Run setup() first.")
            return
        
        if self.is_running:
            print("Monitoring already running")
            return
        
        if not self.monitor:
            self.monitor = ContinuousMonitor(self.db_manager)
        
        print(f"\n{'='*60}")
        print(f"STARTING CONTINUOUS MONITORING")
        print(f"{'='*60}")
        print(f"User email: {self.user_email}")
        print(f"Check interval: Every {interval_minutes} minutes")
        print(f"Sender email: {self.monitor.email_sender.sender_email or 'NOT CONFIGURED'}")
        
        if not self.monitor.email_sender.sendgrid_api_key:
            print("\nWARNING: SendGrid API key not configured!")
            print("Set SENDGRID_API_KEY in .env file")
            print("Monitoring will run but emails won't be sent\n")
        
        self.scheduler = BackgroundScheduler()
        
        self.scheduler.add_job(
            func=self.monitor.monitor_all_subscriptions,
            trigger='interval',
            minutes=interval_minutes,
            id='air_quality_monitor',
            replace_existing=True
        )
        
        self.scheduler.start()
        self.is_running = True
        
        print(f"Monitoring started successfully")
        print(f"Next check: {datetime.now() + timedelta(minutes=interval_minutes)}")
        
        print("\nRunning initial check...")
        self.monitor.monitor_all_subscriptions()
    
    def stop_monitoring(self):
        """Stop continuous monitoring."""
        if not self.is_running:
            print("Monitoring not running")
            return
        
        if self.scheduler:
            self.scheduler.shutdown()
        self.is_running = False
        print("\nMonitoring stopped")
    
    def get_subscription_status(self):
        """Show current subscription status."""
        subscriptions = self.db_manager.get_active_subscriptions()
        
        print(f"\n{'='*60}")
        print(f"ACTIVE SUBSCRIPTIONS ({len(subscriptions)})")
        print(f"{'='*60}")
        
        if not subscriptions:
            print("No active subscriptions")
        else:
            for sub in subscriptions:
                print(f"  Email: {sub['user_email']}")
                print(f"  Location: {sub['city']}, {sub['country'] or 'N/A'}")
                print(f"  Monitoring: {', '.join(sub['alert_types'])}")
                print()


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Main function to run the alert system."""
    
    print("\n" + "="*60)
    print("AIR QUALITY ALERT SYSTEM")
    print("="*60)
    
    db_manager = DatabaseManager(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5000"),
        dbname=os.getenv("DB_NAME", "db"),
        user=os.getenv("DB_USER", "db_user"),
        password=os.getenv("DB_PASSWORD", "db_password")
    )
    
    try:
        db_manager.connect()
        
        alert_system = AlertManagementSystem(db_manager)
        alert_system.setup()
        
        print("\n" + "="*60)
        print("CITY MONITORING SETUP")
        print("="*60)
        
        cities_added = 0
        while True:
            city = input("\nEnter city name to monitor (or 'done' to finish): ").strip()
            
            if city.lower() == 'done':
                if cities_added == 0:
                    print("You must add at least one city to monitor.")
                    continue
                break
            
            if not city:
                continue
            
            country = input("Enter country (optional, press Enter to skip): ").strip()
            country = country if country else None
            
            if alert_system.add_city_subscription(city, country):
                cities_added += 1
        
        alert_system.get_subscription_status()
        
        print("\n" + "="*60)
        print("START MONITORING")
        print("="*60)
        start = input("Start monitoring now? (yes/no): ").strip().lower()
        
        if start in ['yes', 'y']:
            interval = input("Check interval in minutes (default 15): ").strip()
            interval = int(interval) if interval.isdigit() else 15
            
            alert_system.start_monitoring(interval_minutes=interval)
            
            print("\n" + "="*60)
            print("MONITORING ACTIVE")
            print("="*60)
            print("Press Ctrl+C to stop monitoring")
            
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n\nStopping monitoring...")
                alert_system.stop_monitoring()
        else:
            print("\nMonitoring not started. You can start it later by running this script again.")
    
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db_manager.close()
        print("\nProgram ended.")


if __name__ == "__main__":
    main()
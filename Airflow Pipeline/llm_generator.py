# # short answers
# """
# Environmental Query System - Brief Response Version
# Fixed to give concise 4-6 line answers
# """

# import os
# from datetime import datetime, timedelta
# from typing import Optional, Dict, Any, List, Tuple
# from langchain_openai import AzureChatOpenAI
# from dotenv import load_dotenv
# from langchain.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate
# from langchain.output_parsers import PydanticOutputParser
# from langchain.chains import LLMChain
# from pydantic import BaseModel, Field
# import psycopg2
# from psycopg2.extras import RealDictCursor
# import json

# # Load environment variables
# load_dotenv()


# # ============================================================================
# # DATABASE MANAGER
# # ============================================================================

# class DatabaseManager:
#     """Manages PostgreSQL database connections and queries."""
    
#     def __init__(self, host="localhost", port="5000", dbname="db", 
#                  user="db_user", password="db_password"):
#         self.host = host
#         self.port = port
#         self.dbname = dbname
#         self.user = user
#         self.password = password
#         self.conn = None
    
#     def connect(self):
#         """Establish database connection."""
#         try:
#             self.conn = psycopg2.connect(
#                 host=self.host,
#                 port=self.port,
#                 dbname=self.dbname,
#                 user=self.user,
#                 password=self.password
#             )
#             print("Connected to PostgreSQL database")
#             return self.conn
#         except psycopg2.Error as e:
#             print(f"Database connection failed: {e}")
#             raise
    
#     def close(self):
#         """Close database connection."""
#         if self.conn:
#             self.conn.close()
#             print("Database connection closed")
    
#     def query_air_quality(self, city: str, country: Optional[str], date: str) -> Dict[str, Any]:
#         """Query air quality data for a location and date."""
#         try:
#             cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
#             if country:
#                 query = """
#                 SELECT 
#                     parameter_name,
#                     AVG(value) as avg_value,
#                     units,
#                     city,
#                     country,
#                     latitude,
#                     longitude,
#                     datetime_utc,
#                     DATE(datetime_utc) as data_date
#                 FROM air_quality_data
#                 WHERE LOWER(city) LIKE LOWER(%s)
#                     AND LOWER(country) LIKE LOWER(%s)
#                     AND DATE(datetime_utc) >= %s::date - INTERVAL '7 days'
#                     AND DATE(datetime_utc) <= %s::date
#                 GROUP BY parameter_name, units, city, country, latitude, longitude, datetime_utc
#                 ORDER BY datetime_utc DESC
#                 LIMIT 50
#                 """
#                 cursor.execute(query, (f"%{city}%", f"%{country}%", date, date))
#             else:
#                 query = """
#                 SELECT 
#                     parameter_name,
#                     AVG(value) as avg_value,
#                     units,
#                     city,
#                     country,
#                     latitude,
#                     longitude,
#                     datetime_utc,
#                     DATE(datetime_utc) as data_date
#                 FROM air_quality_data
#                 WHERE LOWER(city) LIKE LOWER(%s)
#                     AND DATE(datetime_utc) >= %s::date - INTERVAL '7 days'
#                     AND DATE(datetime_utc) <= %s::date
#                 GROUP BY parameter_name, units, city, country, latitude, longitude, datetime_utc
#                 ORDER BY datetime_utc DESC
#                 LIMIT 50
#                 """
#                 cursor.execute(query, (f"%{city}%", date, date))
            
#             results = cursor.fetchall()
            
#             if not results:
#                 cursor.close()
#                 return None
            
#             data = {
#                 'location': f"{results[0]['city']}, {results[0]['country']}",
#                 'latitude': results[0]['latitude'],
#                 'longitude': results[0]['longitude'],
#                 'date': date,
#                 'actual_date': str(results[0]['data_date']),
#                 'pollutants': {}
#             }
            
#             for row in results:
#                 param = row['parameter_name']
#                 if param not in data['pollutants']:
#                     data['pollutants'][param] = {
#                         'value': float(row['avg_value']),
#                         'units': row['units']
#                     }
            
#             cursor.close()
            
#             if 'pm25' in data['pollutants']:
#                 pm25 = data['pollutants']['pm25']['value']
#                 data['aqi'] = self.calculate_aqi_from_pm25(pm25)
            
#             return data
            
#         except Exception as e:
#             print(f"Error querying air quality data: {e}")
#             return None
    
#     def query_weather_data(self, city: str, country: Optional[str], date: str) -> Dict[str, Any]:
#         """Query meteorological data from MERRA-2."""
#         try:
#             cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
#             loc_query = """
#             SELECT DISTINCT latitude, longitude
#             FROM air_quality_data
#             WHERE LOWER(city) LIKE LOWER(%s)
#             LIMIT 1
#             """
#             cursor.execute(loc_query, (f"%{city}%",))
#             location = cursor.fetchone()
            
#             if not location:
#                 cursor.close()
#                 return None
            
#             lat = float(location['latitude'])
#             lon = float(location['longitude'])
            
#             query = """
#             SELECT 
#                 variable_name,
#                 AVG(variable_value) as avg_value,
#                 variable_units,
#                 latitude,
#                 longitude,
#                 DATE(granule_time_start) as data_date
#             FROM merra2_slv_data
#             WHERE DATE(granule_time_start) >= %s::date - INTERVAL '7 days'
#                 AND DATE(granule_time_start) <= %s::date
#                 AND latitude BETWEEN %s AND %s
#                 AND longitude BETWEEN %s AND %s
#             GROUP BY variable_name, variable_units, latitude, longitude, data_date
#             ORDER BY data_date DESC
#             LIMIT 50
#             """
            
#             cursor.execute(query, (date, date, lat-0.5, lat+0.5, lon-0.5, lon+0.5))
#             results = cursor.fetchall()
#             cursor.close()
            
#             if not results:
#                 return None
            
#             data = {
#                 'latitude': lat,
#                 'longitude': lon,
#                 'date': date,
#                 'actual_date': str(results[0]['data_date']) if results else date,
#                 'variables': {}
#             }
            
#             for row in results:
#                 var_name = row['variable_name']
#                 value = float(row['avg_value']) if row['avg_value'] else None
                
#                 if var_name == 'T2M' and value:
#                     value = value - 273.15
#                     data['variables']['temperature'] = {'value': value, 'units': '°C'}
#                 elif var_name == 'QV2M':
#                     data['variables']['humidity'] = {'value': value * 100, 'units': '%'}
#                 elif var_name in ['U10M', 'V10M']:
#                     if 'wind_speed' not in data['variables']:
#                         data['variables']['wind_speed'] = {'value': value, 'units': 'm/s'}
            
#             return data
            
#         except Exception as e:
#             print(f"Error querying weather data: {e}")
#             return None
    
#     def query_no2_data(self, city: str, country: Optional[str], date: str) -> Dict[str, Any]:
#         """Query NO2 tropospheric column data."""
#         try:
#             cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
#             loc_query = """
#             SELECT DISTINCT latitude, longitude
#             FROM air_quality_data
#             WHERE LOWER(city) LIKE LOWER(%s)
#             LIMIT 1
#             """
#             cursor.execute(loc_query, (f"%{city}%",))
#             location = cursor.fetchone()
            
#             if not location:
#                 cursor.close()
#                 return None
            
#             lat = float(location['latitude'])
#             lon = float(location['longitude'])
            
#             query = """
#             SELECT 
#                 AVG(no2_tropospheric_column) as avg_no2,
#                 observation_datetime
#             FROM tempo_no2_data
#             WHERE DATE(observation_datetime) = %s
#                 AND latitude BETWEEN %s AND %s
#                 AND longitude BETWEEN %s AND %s
#             GROUP BY observation_datetime
#             ORDER BY observation_datetime DESC
#             LIMIT 1
#             """
            
#             cursor.execute(query, (date, lat-0.5, lat+0.5, lon-0.5, lon+0.5))
#             result = cursor.fetchone()
#             cursor.close()
            
#             if result and result['avg_no2']:
#                 return {
#                     'no2_column': float(result['avg_no2']),
#                     'units': 'molecules/cm²',
#                     'observation_time': result['observation_datetime']
#                 }
            
#             return None
            
#         except Exception as e:
#             print(f"Error querying NO2 data: {e}")
#             return None
    
#     def query_fire_data(self, city: str, country: Optional[str], date: str) -> Dict[str, Any]:
#         """Query fire detection data."""
#         try:
#             cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
#             loc_query = """
#             SELECT DISTINCT latitude, longitude
#             FROM air_quality_data
#             WHERE LOWER(city) LIKE LOWER(%s)
#             LIMIT 1
#             """
#             cursor.execute(loc_query, (f"%{city}%",))
#             location = cursor.fetchone()
            
#             if not location:
#                 cursor.close()
#                 return None
            
#             lat = float(location['latitude'])
#             lon = float(location['longitude'])
            
#             query = """
#             SELECT 
#                 COUNT(*) as fire_count,
#                 AVG(frp) as avg_frp,
#                 MAX(confidence) as max_confidence,
#                 acq_date as data_date
#             FROM fire_detection_data
#             WHERE acq_date >= %s::date - INTERVAL '7 days'
#                 AND acq_date <= %s::date
#                 AND latitude BETWEEN %s AND %s
#                 AND longitude BETWEEN %s AND %s
#             GROUP BY acq_date
#             ORDER BY acq_date DESC
#             LIMIT 1
#             """
            
#             cursor.execute(query, (date, date, lat-1, lat+1, lon-1, lon+1))
#             result = cursor.fetchone()
#             cursor.close()
            
#             if result and result['fire_count'] > 0:
#                 return {
#                     'fire_count': result['fire_count'],
#                     'avg_fire_power': float(result['avg_frp']) if result['avg_frp'] else 0,
#                     'max_confidence': result['max_confidence'],
#                     'actual_date': str(result['data_date']) if result.get('data_date') else date
#                 }
            
#             return None
            
#         except Exception as e:
#             print(f"Error querying fire data: {e}")
#             return None
    
#     def calculate_aqi_from_pm25(self, pm25: float) -> Tuple[int, str]:
#         """Calculate AQI and category from PM2.5 concentration."""
#         if pm25 <= 12.0:
#             aqi = (50 / 12.0) * pm25
#             category = "Good"
#         elif pm25 <= 35.4:
#             aqi = 50 + ((100 - 50) / (35.4 - 12.1)) * (pm25 - 12.1)
#             category = "Moderate"
#         elif pm25 <= 55.4:
#             aqi = 100 + ((150 - 100) / (55.4 - 35.5)) * (pm25 - 35.5)
#             category = "Unhealthy for Sensitive Groups"
#         elif pm25 <= 150.4:
#             aqi = 150 + ((200 - 150) / (150.4 - 55.5)) * (pm25 - 55.5)
#             category = "Unhealthy"
#         elif pm25 <= 250.4:
#             aqi = 200 + ((300 - 200) / (250.4 - 150.5)) * (pm25 - 150.5)
#             category = "Very Unhealthy"
#         else:
#             aqi = 300 + ((500 - 300) / (500.4 - 250.5)) * (pm25 - 250.5)
#             category = "Hazardous"
        
#         return int(aqi), category
    
#     def get_comprehensive_data(self, city: str, country: Optional[str], date: str) -> Dict[str, Any]:
#         """Get all available environmental data for a location and date."""
#         result = {
#             'location': None,
#             'date': date,
#             'air_quality': None,
#             'weather': None,
#             'no2': None,
#             'fire': None
#         }
        
#         aq_data = self.query_air_quality(city, country, date)
#         if aq_data:
#             result['location'] = aq_data['location']
#             result['air_quality'] = aq_data
        
#         weather_data = self.query_weather_data(city, country, date)
#         if weather_data:
#             result['weather'] = weather_data
        
#         no2_data = self.query_no2_data(city, country, date)
#         if no2_data:
#             result['no2'] = no2_data
        
#         fire_data = self.query_fire_data(city, country, date)
#         if fire_data:
#             result['fire'] = fire_data
        
#         return result
    
#     def get_available_cities(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
#         """Get list of cities available in the database."""
#         try:
#             cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
#             query = """
#             SELECT DISTINCT 
#                 city, 
#                 country, 
#                 COUNT(*) as record_count,
#                 MAX(DATE(datetime_utc)) as latest_date,
#                 MIN(DATE(datetime_utc)) as earliest_date
#             FROM air_quality_data
#             GROUP BY city, country
#             ORDER BY record_count DESC
#             """
            
#             if limit:
#                 query += f" LIMIT {limit}"
            
#             cursor.execute(query)
#             results = cursor.fetchall()
#             cursor.close()
            
#             print(f"Retrieved {len(results)} cities from database")
#             return results
            
#         except Exception as e:
#             print(f"Error getting available cities: {e}")
#             return []
    
#     def diagnose_database_content(self) -> Dict[str, Any]:
#         """Diagnose what's actually in the database."""
#         try:
#             cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
#             cursor.execute("""
#                 SELECT COUNT(DISTINCT city) as unique_cities,
#                        COUNT(DISTINCT country) as unique_countries,
#                        COUNT(*) as total_records
#                 FROM air_quality_data
#             """)
#             summary = cursor.fetchone()
            
#             cursor.execute("""
#                 SELECT DISTINCT city, country
#                 FROM air_quality_data
#                 ORDER BY city, country
#             """)
#             all_locations = cursor.fetchall()
            
#             cursor.execute("""
#                 SELECT city, country, parameter_name, datetime_utc
#                 FROM air_quality_data
#                 ORDER BY datetime_utc DESC
#                 LIMIT 5
#             """)
#             sample_records = cursor.fetchall()
            
#             cursor.close()
            
#             return {
#                 'summary': summary,
#                 'all_locations': all_locations,
#                 'sample_records': sample_records
#             }
            
#         except Exception as e:
#             print(f"Diagnostic error: {e}")
#             return None


# # ============================================================================
# # PARSED QUERY MODEL
# # ============================================================================

# class ParsedQuery(BaseModel):
#     """Structured output for parsed user query with validation."""
#     city: Optional[str] = Field(default=None, description="City name if explicitly mentioned, otherwise None")
#     country: Optional[str] = Field(default=None, description="Country name if mentioned")
#     query_type: List[str] = Field(description="Type of information requested")
#     time_reference: str = Field(description="Time reference: 'today', 'tomorrow', or YYYY-MM-DD")
#     original_query: str = Field(description="The original user query")
#     needs_clarification: bool = Field(default=False, description="True if city is missing or ambiguous")
#     clarification_reason: Optional[str] = Field(default=None, description="Why clarification is needed")


# # ============================================================================
# # QUERY PARSER
# # ============================================================================

# class ImprovedQueryParser:
#     """Enhanced parser that validates city information."""
    
#     def __init__(self):
#         self.llm = AzureChatOpenAI(
#             deployment_name="gpt-4o",
#             api_version="2024-12-01-preview",
#             azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "https://hassan-siddiqui.openai.azure.com/"),
#             api_key=os.getenv("AZURE_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY"),
#             temperature=0
#         )
#         self.parser = PydanticOutputParser(pydantic_object=ParsedQuery)
#         self.setup_chain()
    
#     def setup_chain(self):
#         """Setup the parsing chain with improved prompt."""
        
#         system_template = """You are an expert at parsing environmental and weather queries.

# CRITICAL RULES FOR CITY DETECTION:
# 1. **ONLY extract a city name if it is EXPLICITLY mentioned in the query**
# 2. If NO city is mentioned, you MUST set city=None and needs_clarification=True
# 3. DO NOT assume, infer, or use default cities under ANY circumstances
# 4. DO NOT use cities like "Burlington", "New York", or any other city unless explicitly stated

# CRITICAL RULES FOR TIME REFERENCE:
# 1. **ALWAYS provide a time_reference as a string, NEVER null**
# 2. If no time is mentioned, default to "today"
# 3. Convert "now", "currently", "right now" to "today"
# 4. Convert "tomorrow" to "tomorrow"
# 5. Keep explicit dates as-is (YYYY-MM-DD format)

# Extract the following information:
# - city: Extract ONLY if explicitly mentioned, otherwise set to None
# - country: Extract if mentioned
# - query_type: air_quality, weather, aqi, pollutants, forecast, activity_safety, temperature, humidity, wind, fire
# - time_reference: MUST be a string ('today', 'tomorrow', or YYYY-MM-DD), NEVER null. Default to 'today' if not specified.
# - needs_clarification: Set to True if city is not specified
# - clarification_reason: Explain what information is missing
# - original_query: The exact user query

# Current date: {current_date}

# {format_instructions}

# EXAMPLES:

# Query: "Can I go for a picnic in Toronto tomorrow?"
# Output: {{"city": "Toronto", "country": null, "needs_clarification": false, "query_type": ["activity_safety", "air_quality", "weather"], "time_reference": "tomorrow", "original_query": "Can I go for a picnic in Toronto tomorrow?"}}

# Query: "Is it safe to go jogging in morning?"
# Output: {{"city": null, "country": null, "needs_clarification": true, "clarification_reason": "City not specified in query", "query_type": ["activity_safety", "air_quality"], "time_reference": "today", "original_query": "Is it safe to go jogging in morning?"}}

# Query: "What's the weather like?"
# Output: {{"city": null, "country": null, "needs_clarification": true, "clarification_reason": "Location not provided", "query_type": ["weather"], "time_reference": "today", "original_query": "What's the weather like?"}}

# Query: "Air quality in Miami today?"
# Output: {{"city": "Miami", "country": null, "needs_clarification": false, "query_type": ["air_quality"], "time_reference": "today", "original_query": "Air quality in Miami today?"}}

# Query: "Is it good to visit Miami or not?"
# Output: {{"city": "Miami", "country": null, "needs_clarification": false, "query_type": ["activity_safety", "air_quality", "weather"], "time_reference": "today", "original_query": "Is it good to visit Miami or not?"}}

# REMEMBER: 
# - When in doubt about city, set city=None and ask for clarification
# - ALWAYS set time_reference to a string, never null (default to "today")
# """
        
#         human_template = """Parse this query: {query}"""
        
#         system_message = SystemMessagePromptTemplate.from_template(system_template)
#         human_message = HumanMessagePromptTemplate.from_template(human_template)
        
#         chat_prompt = ChatPromptTemplate.from_messages([system_message, human_message])
        
#         self.chain = LLMChain(llm=self.llm, prompt=chat_prompt)
    
#     def parse(self, query: str) -> ParsedQuery:
#         """Parse a natural language query with validation."""
#         current_date = datetime.now().strftime("%Y-%m-%d")
        
#         result = self.chain.run(
#             query=query,
#             current_date=current_date,
#             format_instructions=self.parser.get_format_instructions()
#         )
        
#         parsed = self.parser.parse(result)
        
#         if not parsed.city or parsed.city.strip() == "":
#             parsed.city = None
#             parsed.needs_clarification = True
#             if not parsed.clarification_reason:
#                 parsed.clarification_reason = "Please specify which city you're asking about"
        
#         return parsed
    
#     def convert_time_to_date(self, time_reference: str) -> str:
#         """Convert time reference to date string."""
#         today = datetime.now()
        
#         if time_reference.lower() in ["today", "now"]:
#             return today.strftime("%Y-%m-%d")
#         elif time_reference.lower() == "tomorrow":
#             return (today + timedelta(days=1)).strftime("%Y-%m-%d")
#         else:
#             return time_reference


# # ============================================================================
# # RESPONSE GENERATOR - BRIEF VERSION
# # ============================================================================

# class ImprovedResponseGenerator:
#     """Enhanced response generator with brief, concise outputs."""
    
#     def __init__(self):
#         self.llm = AzureChatOpenAI(
#             deployment_name="gpt-4o",
#             api_version="2024-12-01-preview",
#             azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "https://hassan-siddiqui.openai.azure.com/"),
#             api_key=os.getenv("AZURE_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY"),
#             temperature=0.7
#         )
#         self.setup_chain()
    
#     def setup_chain(self):
#         """Setup response generation chain with brief output prompt."""
        
#         system_template = """You are an environmental data assistant providing brief, factual information.

# CRITICAL: Keep responses to 4-6 lines maximum. Be concise and direct.

# RESPONSE RULES:
# 1. Answer in 4-6 lines ONLY
# 2. State the key finding first
# 3. Give a direct yes/no or recommendation when asked
# 4. Include only the most relevant data point
# 5. Skip lengthy explanations

# EPA AQI Quick Reference:
# - Good (0-50): Safe for all activities
# - Moderate (51-100): Acceptable for most
# - Unhealthy for Sensitive (101-150): Sensitive groups take precautions
# - Unhealthy (151-200): General public affected
# - Very Unhealthy (201-300): Serious effects for everyone
# - Hazardous (301+): Emergency conditions

# DATABASE INFORMATION:
# {database_result}

# USER'S QUESTION: {original_query}

# Provide a brief, direct answer in 4-6 lines."""

#         human_template = """Provide a response based on the database information."""
        
#         system_message = SystemMessagePromptTemplate.from_template(system_template)
#         human_message = HumanMessagePromptTemplate.from_template(human_template)
        
#         chat_prompt = ChatPromptTemplate.from_messages([system_message, human_message])
        
#         self.chain = LLMChain(llm=self.llm, prompt=chat_prompt)
    
#     def format_database_result(self, data: Dict[str, Any]) -> str:
#         """Format database result for the LLM prompt."""
#         if not data['location'] and not any([data['air_quality'], data['weather'], data['no2'], data['fire']]):
#             return "No data available in the database for this location and date."
        
#         result = f"REQUESTED DATE: {data['date']}\n"
        
#         if data['location']:
#             result += f"LOCATION: {data['location']}\n"
        
#         if data.get('air_quality') and data['air_quality'].get('actual_date'):
#             if data['air_quality']['actual_date'] != data['date']:
#                 result += f"NOTE: Showing data from {data['air_quality']['actual_date']} (most recent available)\n"
        
#         result += "\n"
        
#         if data['air_quality']:
#             aq = data['air_quality']
#             result += "AIR QUALITY DATA:\n"
            
#             if 'aqi' in aq:
#                 aqi_val, aqi_cat = aq['aqi']
#                 result += f"- AQI: {aqi_val} ({aqi_cat})\n"
            
#             if 'pollutants' in aq:
#                 for param, info in aq['pollutants'].items():
#                     result += f"- {param.upper()}: {info['value']:.2f} {info['units']}\n"
#             result += "\n"
        
#         if data['weather']:
#             w = data['weather']
#             result += "WEATHER DATA:\n"
            
#             if w.get('actual_date') and w['actual_date'] != data['date']:
#                 result += f"(Data from {w['actual_date']})\n"
            
#             if 'variables' in w:
#                 for var, info in w['variables'].items():
#                     result += f"- {var.replace('_', ' ').title()}: {info['value']:.2f} {info['units']}\n"
#             result += "\n"
        
#         if data['no2']:
#             result += f"NITROGEN DIOXIDE:\n"
#             result += f"- NO2 Column: {data['no2']['no2_column']:.2e} {data['no2']['units']}\n\n"
        
#         if data['fire']:
#             f = data['fire']
#             result += "FIRE ACTIVITY:\n"
#             if f.get('actual_date') and f['actual_date'] != data['date']:
#                 result += f"(Data from {f['actual_date']})\n"
#             result += f"- Active fires detected: {f['fire_count']}\n"
#             result += f"- Average fire power: {f['avg_fire_power']:.2f} MW\n"
#             result += f"- Detection confidence: {f['max_confidence']}\n\n"
        
#         return result
    
#     def generate(self, original_query: str, database_result: Dict[str, Any]) -> str:
#         """Generate a response with error handling."""
#         formatted_result = self.format_database_result(database_result)
        
#         try:
#             response = self.chain.run(
#                 original_query=original_query,
#                 database_result=formatted_result
#             )
#             return response
            
#         except Exception as e:
#             if "content_filter" in str(e) or "ResponsibleAIPolicyViolation" in str(e):
#                 return self.generate_safe_fallback(original_query, formatted_result)
#             else:
#                 raise
    
#     def generate_safe_fallback(self, query: str, data: str) -> str:
#         """Safe fallback response for content filter issues."""
#         if "No data available" in data:
#             return "I don't have environmental data available for that location. Please specify a city with available data."
        
#         return f"""Based on the available environmental data:

# {data}

# For specific recommendations about outdoor activities, please consult the AQI values and EPA guidelines above. Generally:
# - AQI 0-50 (Good): Outdoor activities are suitable for everyone
# - AQI 51-100 (Moderate): Acceptable for most people
# - AQI 101-150: Sensitive individuals should consider reducing prolonged outdoor exertion
# - AQI 151+: Everyone should reduce outdoor activities

# Would you like more specific information about any particular measurement?"""


# # ============================================================================
# # MAIN SYSTEM
# # ============================================================================

# class ImprovedEnvironmentalQuerySystem:
#     """Enhanced system with validation and better error handling."""
    
#     def __init__(self, db_config: Optional[Dict] = None):
#         if db_config:
#             self.db_manager = DatabaseManager(**db_config)
#         else:
#             self.db_manager = DatabaseManager()
        
#         self.db_manager.connect()
#         self.query_parser = ImprovedQueryParser()
#         self.response_generator = ImprovedResponseGenerator()
    
#     def process_query(self, user_query: str) -> str:
#         """Process a user query with validation."""
        
#         try:
#             print("\nParsing query...")
#             parsed_query = self.query_parser.parse(user_query)
#             print(f"   City: {parsed_query.city}")
#             print(f"   Country: {parsed_query.country}")
#             print(f"   Query Type: {parsed_query.query_type}")
#             print(f"   Time: {parsed_query.time_reference}")
#             print(f"   Needs Clarification: {parsed_query.needs_clarification}")
            
#             if parsed_query.needs_clarification or not parsed_query.city:
#                 print("\nCity not specified, requesting clarification...")
#                 return self.request_clarification(parsed_query)
            
#             date = self.query_parser.convert_time_to_date(parsed_query.time_reference)
#             print(f"   Date: {date}")
            
#             print("\nQuerying database...")
#             db_result = self.db_manager.get_comprehensive_data(
#                 parsed_query.city,
#                 parsed_query.country,
#                 date
#             )
            
#             if not db_result['location'] and not any([
#                 db_result['air_quality'], 
#                 db_result['weather'], 
#                 db_result['no2'], 
#                 db_result['fire']
#             ]):
#                 print(f"\nNo data found for {parsed_query.city}")
#                 return self.suggest_available_cities(parsed_query.city)
            
#             print("\nGenerating response...\n")
#             response = self.response_generator.generate(user_query, db_result)
            
#             return response
            
#         except Exception as e:
#             if "content_filter" in str(e):
#                 return self.handle_content_filter_error(user_query)
#             return f"An error occurred while processing your query: {str(e)}"
    
#     def request_clarification(self, parsed_query: ParsedQuery) -> str:
#         """Request city information from user."""
#         cities = self.db_manager.get_available_cities(limit=10)
        
#         if not cities:
#             return "I'd be happy to help, but I need to know which city you're asking about. Could you please specify a city name?"
        
#         city_examples = [f"{c['city']}, {c['country']}" for c in cities]
#         city_list = ", ".join(city_examples[:5])
        
#         return f"""I'd be happy to help you with your question about {parsed_query.query_type[0].replace('_', ' ')}!

# However, I need to know which city you're asking about to provide accurate information.

# Please specify a city name. For example:
# - "Is it safe to go jogging in Miami?"
# - "What's the air quality in Los Angeles today?"
# - "Tell me about weather conditions in Toronto"

# Some cities with available data include: {city_list}, and many more.

# Which city would you like information for?"""
    
#     def suggest_available_cities(self, attempted_city: str) -> str:
#         """Suggest alternatives when city not found."""
#         cities = self.db_manager.get_available_cities(limit=30)
        
#         if not cities:
#             return f"I couldn't find data for '{attempted_city}' in the database. The database appears to be empty or unavailable."
        
#         city_list = "\n".join([
#             f"  - {c['city']}, {c['country']} (data from {c['earliest_date']} to {c['latest_date']})" 
#             for c in cities
#         ])
        
#         return f"""I couldn't find environmental data for "{attempted_city}" in our database.

# Here are some cities with available air quality and weather data:

# {city_list}

# Would you like to query one of these cities instead? Just ask about any city from the list!"""
    
#     def handle_content_filter_error(self, query: str) -> str:
#         """Handle Azure content filter errors gracefully."""
#         return """I can help you with environmental and air quality information!

# To provide you with accurate data, please ask specific questions like:
# - "What is the air quality index in [city name] today?"
# - "Show me PM2.5 and pollutant levels in [city name]"
# - "What are the current weather conditions in [city name]?"
# - "Tell me about air quality in [city name] for outdoor activities"

# I'll provide objective measurements and standard EPA interpretations to help inform your decisions.

# Which city would you like information about?"""
    
#     def close(self):
#         """Clean up resources."""
#         self.db_manager.close()


# # ============================================================================
# # USAGE EXAMPLE
# # ============================================================================

# def main():
#     """Example usage with brief responses."""
    
#     DB_CONFIG = {
#         "host": "localhost",
#         "port": "5000",
#         "dbname": "db",
#         "user": "db_user",
#         "password": "db_password"
#     }
    
#     system = ImprovedEnvironmentalQuerySystem(DB_CONFIG)
    
#     try:
#         print("\n" + "=" * 80)
#         print("DATABASE DIAGNOSTICS")
#         print("=" * 80)
#         diagnostics = system.db_manager.diagnose_database_content()
        
#         if diagnostics:
#             summary = diagnostics['summary']
#             print(f"\nDatabase Summary:")
#             print(f"   Total records: {summary['total_records']}")
#             print(f"   Unique cities: {summary['unique_cities']}")
#             print(f"   Unique countries: {summary['unique_countries']}")
            
#             print(f"\nAll City/Country Combinations ({len(diagnostics['all_locations'])} total):")
#             for i, loc in enumerate(diagnostics['all_locations'][:50], 1):
#                 print(f"   {i:3}. {loc['city']}, {loc['country']}")
            
#             if len(diagnostics['all_locations']) > 50:
#                 print(f"   ... and {len(diagnostics['all_locations']) - 50} more")
            
#             print(f"\nSample Records:")
#             for rec in diagnostics['sample_records']:
#                 print(f"   {rec['city']}, {rec['country']} - {rec['parameter_name']} at {rec['datetime_utc']}")
        
#         print("\n" + "=" * 80)
#         print("DATABASE STATUS")
#         print("=" * 80)
#         cities = system.db_manager.get_available_cities()
        
#         if cities:
#             print(f"\nFound {len(cities)} cities with air quality data\n")
#             print(f"{'City':<30} {'Country':<30} {'Records':<10} {'Date Range':<30}")
#             print("=" * 100)
#             for city in cities[:20]:
#                 date_range = f"{city['earliest_date']} to {city['latest_date']}"
#                 print(f"{city['city']:<30} {city['country']:<30} {city['record_count']:<10} {date_range:<30}")
            
#             if len(cities) > 20:
#                 print(f"\n... and {len(cities) - 20} more cities")
#         else:
#             print("\nNo cities found in database!")
        
#         test_queries = [
#             "Is it safe to go jogging in the morning?",
#             "Is it safe to go jogging in Miami?",
#             "What's the air quality like?",
#             "Tell me about air quality in Los Angeles today",
#             "What is the air quality index in New York today?",
#         ]
        
#         print("\n" + "=" * 80)
#         print("TESTING ENVIRONMENTAL QUERY SYSTEM - BRIEF RESPONSES")
#         print("=" * 80)
        
#         for i, query in enumerate(test_queries, 1):
#             print(f"\n{'=' * 80}")
#             print(f"TEST QUERY {i}: {query}")
#             print('=' * 80)
            
#             response = system.process_query(query)
#             print(f"\nRESPONSE:\n{response}\n")
        
#         print("\n" + "=" * 80)
#         print("INTERACTIVE MODE")
#         print("=" * 80)
#         print("\nYou can now ask questions about air quality and weather.")
#         print("Examples:")
#         print("  - 'What is the AQI in Toronto today?'")
#         print("  - 'Is it safe to exercise outdoors in Miami?'")
#         print("  - 'Tell me about air quality in Los Angeles'")
#         print("\nType 'quit' to exit\n")
        
#         while True:
#             user_input = input("Your question: ").strip()
            
#             if not user_input:
#                 continue
                
#             if user_input.lower() in ['quit', 'exit', 'q']:
#                 print("\nThank you for using the Environmental Query System!")
#                 break
            
#             response = system.process_query(user_input)
#             print(f"\nResponse:\n{response}\n")
    
#     except KeyboardInterrupt:
#         print("\n\nInterrupted by user. Closing...")
#     except Exception as e:
#         print(f"\n\nAn error occurred: {e}")
#     finally:
#         system.close()


# if __name__ == "__main__":
#     main()









# # short answers
# """
# Environmental Query System - Brief Response Version
# Fixed to give concise 4-6 line answers with improved error handling
# """

# import os
# from datetime import datetime, timedelta
# from typing import Optional, Dict, Any, List, Tuple
# from langchain_openai import AzureChatOpenAI
# from dotenv import load_dotenv
# from langchain.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate
# from langchain.output_parsers import PydanticOutputParser
# from langchain.chains import LLMChain
# from pydantic import BaseModel, Field
# import psycopg2
# from psycopg2.extras import RealDictCursor
# import json

# # Load environment variables
# load_dotenv()


# # ============================================================================
# # DATABASE MANAGER
# # ============================================================================

# class DatabaseManager:
#     """Manages PostgreSQL database connections and queries."""
    
#     def __init__(self, host="localhost", port="5000", dbname="db", 
#                  user="db_user", password="db_password"):
#         self.host = host
#         self.port = port
#         self.dbname = dbname
#         self.user = user
#         self.password = password
#         self.conn = None
    
#     def connect(self):
#         """Establish database connection."""
#         try:
#             self.conn = psycopg2.connect(
#                 host=self.host,
#                 port=self.port,
#                 dbname=self.dbname,
#                 user=self.user,
#                 password=self.password
#             )
#             print("Connected to PostgreSQL database")
#             return self.conn
#         except psycopg2.Error as e:
#             print(f"Database connection failed: {e}")
#             raise
    
#     def close(self):
#         """Close database connection."""
#         if self.conn:
#             self.conn.close()
#             print("Database connection closed")
    
#     def query_air_quality(self, city: str, country: Optional[str], date: str) -> Dict[str, Any]:
#         """Query air quality data for a location and date."""
#         try:
#             cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
#             if country:
#                 query = """
#                 SELECT 
#                     parameter_name,
#                     AVG(value) as avg_value,
#                     units,
#                     city,
#                     country,
#                     latitude,
#                     longitude,
#                     datetime_utc,
#                     DATE(datetime_utc) as data_date
#                 FROM air_quality_data
#                 WHERE LOWER(city) LIKE LOWER(%s)
#                     AND LOWER(country) LIKE LOWER(%s)
#                     AND DATE(datetime_utc) >= %s::date - INTERVAL '7 days'
#                     AND DATE(datetime_utc) <= %s::date
#                 GROUP BY parameter_name, units, city, country, latitude, longitude, datetime_utc
#                 ORDER BY datetime_utc DESC
#                 LIMIT 50
#                 """
#                 cursor.execute(query, (f"%{city}%", f"%{country}%", date, date))
#             else:
#                 query = """
#                 SELECT 
#                     parameter_name,
#                     AVG(value) as avg_value,
#                     units,
#                     city,
#                     country,
#                     latitude,
#                     longitude,
#                     datetime_utc,
#                     DATE(datetime_utc) as data_date
#                 FROM air_quality_data
#                 WHERE LOWER(city) LIKE LOWER(%s)
#                     AND DATE(datetime_utc) >= %s::date - INTERVAL '7 days'
#                     AND DATE(datetime_utc) <= %s::date
#                 GROUP BY parameter_name, units, city, country, latitude, longitude, datetime_utc
#                 ORDER BY datetime_utc DESC
#                 LIMIT 50
#                 """
#                 cursor.execute(query, (f"%{city}%", date, date))
            
#             results = cursor.fetchall()
            
#             if not results:
#                 cursor.close()
#                 return None
            
#             data = {
#                 'location': f"{results[0]['city']}, {results[0]['country']}",
#                 'latitude': results[0]['latitude'],
#                 'longitude': results[0]['longitude'],
#                 'date': date,
#                 'actual_date': str(results[0]['data_date']),
#                 'pollutants': {}
#             }
            
#             for row in results:
#                 param = row['parameter_name']
#                 if param not in data['pollutants']:
#                     data['pollutants'][param] = {
#                         'value': float(row['avg_value']),
#                         'units': row['units']
#                     }
            
#             cursor.close()
            
#             if 'pm25' in data['pollutants']:
#                 pm25 = data['pollutants']['pm25']['value']
#                 data['aqi'] = self.calculate_aqi_from_pm25(pm25)
            
#             return data
            
#         except Exception as e:
#             print(f"Error querying air quality data: {e}")
#             return None
    
#     def query_weather_data(self, city: str, country: Optional[str], date: str) -> Dict[str, Any]:
#         """Query meteorological data from MERRA-2."""
#         try:
#             cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
#             loc_query = """
#             SELECT DISTINCT latitude, longitude
#             FROM air_quality_data
#             WHERE LOWER(city) LIKE LOWER(%s)
#             LIMIT 1
#             """
#             cursor.execute(loc_query, (f"%{city}%",))
#             location = cursor.fetchone()
            
#             if not location:
#                 cursor.close()
#                 return None
            
#             lat = float(location['latitude'])
#             lon = float(location['longitude'])
            
#             query = """
#             SELECT 
#                 variable_name,
#                 AVG(variable_value) as avg_value,
#                 variable_units,
#                 latitude,
#                 longitude,
#                 DATE(granule_time_start) as data_date
#             FROM merra2_slv_data
#             WHERE DATE(granule_time_start) >= %s::date - INTERVAL '7 days'
#                 AND DATE(granule_time_start) <= %s::date
#                 AND latitude BETWEEN %s AND %s
#                 AND longitude BETWEEN %s AND %s
#             GROUP BY variable_name, variable_units, latitude, longitude, data_date
#             ORDER BY data_date DESC
#             LIMIT 50
#             """
            
#             cursor.execute(query, (date, date, lat-0.5, lat+0.5, lon-0.5, lon+0.5))
#             results = cursor.fetchall()
#             cursor.close()
            
#             if not results:
#                 return None
            
#             data = {
#                 'latitude': lat,
#                 'longitude': lon,
#                 'date': date,
#                 'actual_date': str(results[0]['data_date']) if results else date,
#                 'variables': {}
#             }
            
#             for row in results:
#                 var_name = row['variable_name']
#                 value = float(row['avg_value']) if row['avg_value'] else None
                
#                 if var_name == 'T2M' and value:
#                     value = value - 273.15
#                     data['variables']['temperature'] = {'value': value, 'units': '°C'}
#                 elif var_name == 'QV2M':
#                     data['variables']['humidity'] = {'value': value * 100, 'units': '%'}
#                 elif var_name in ['U10M', 'V10M']:
#                     if 'wind_speed' not in data['variables']:
#                         data['variables']['wind_speed'] = {'value': value, 'units': 'm/s'}
            
#             return data
            
#         except Exception as e:
#             print(f"Error querying weather data: {e}")
#             return None
    
#     def query_no2_data(self, city: str, country: Optional[str], date: str) -> Dict[str, Any]:
#         """Query NO2 tropospheric column data."""
#         try:
#             cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
#             loc_query = """
#             SELECT DISTINCT latitude, longitude
#             FROM air_quality_data
#             WHERE LOWER(city) LIKE LOWER(%s)
#             LIMIT 1
#             """
#             cursor.execute(loc_query, (f"%{city}%",))
#             location = cursor.fetchone()
            
#             if not location:
#                 cursor.close()
#                 return None
            
#             lat = float(location['latitude'])
#             lon = float(location['longitude'])
            
#             query = """
#             SELECT 
#                 AVG(no2_tropospheric_column) as avg_no2,
#                 observation_datetime
#             FROM tempo_no2_data
#             WHERE DATE(observation_datetime) = %s
#                 AND latitude BETWEEN %s AND %s
#                 AND longitude BETWEEN %s AND %s
#             GROUP BY observation_datetime
#             ORDER BY observation_datetime DESC
#             LIMIT 1
#             """
            
#             cursor.execute(query, (date, lat-0.5, lat+0.5, lon-0.5, lon+0.5))
#             result = cursor.fetchone()
#             cursor.close()
            
#             if result and result['avg_no2']:
#                 return {
#                     'no2_column': float(result['avg_no2']),
#                     'units': 'molecules/cm²',
#                     'observation_time': result['observation_datetime']
#                 }
            
#             return None
            
#         except Exception as e:
#             print(f"Error querying NO2 data: {e}")
#             return None
    
#     def query_fire_data(self, city: str, country: Optional[str], date: str) -> Dict[str, Any]:
#         """Query fire detection data."""
#         try:
#             cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
#             loc_query = """
#             SELECT DISTINCT latitude, longitude
#             FROM air_quality_data
#             WHERE LOWER(city) LIKE LOWER(%s)
#             LIMIT 1
#             """
#             cursor.execute(loc_query, (f"%{city}%",))
#             location = cursor.fetchone()
            
#             if not location:
#                 cursor.close()
#                 return None
            
#             lat = float(location['latitude'])
#             lon = float(location['longitude'])
            
#             query = """
#             SELECT 
#                 COUNT(*) as fire_count,
#                 AVG(frp) as avg_frp,
#                 MAX(confidence) as max_confidence,
#                 acq_date as data_date
#             FROM fire_detection_data
#             WHERE acq_date >= %s::date - INTERVAL '7 days'
#                 AND acq_date <= %s::date
#                 AND latitude BETWEEN %s AND %s
#                 AND longitude BETWEEN %s AND %s
#             GROUP BY acq_date
#             ORDER BY acq_date DESC
#             LIMIT 1
#             """
            
#             cursor.execute(query, (date, date, lat-1, lat+1, lon-1, lon+1))
#             result = cursor.fetchone()
#             cursor.close()
            
#             if result and result['fire_count'] > 0:
#                 return {
#                     'fire_count': result['fire_count'],
#                     'avg_fire_power': float(result['avg_frp']) if result['avg_frp'] else 0,
#                     'max_confidence': result['max_confidence'],
#                     'actual_date': str(result['data_date']) if result.get('data_date') else date
#                 }
            
#             return None
            
#         except Exception as e:
#             print(f"Error querying fire data: {e}")
#             return None
    
#     def calculate_aqi_from_pm25(self, pm25: float) -> Tuple[int, str]:
#         """Calculate AQI and category from PM2.5 concentration."""
#         if pm25 <= 12.0:
#             aqi = (50 / 12.0) * pm25
#             category = "Good"
#         elif pm25 <= 35.4:
#             aqi = 50 + ((100 - 50) / (35.4 - 12.1)) * (pm25 - 12.1)
#             category = "Moderate"
#         elif pm25 <= 55.4:
#             aqi = 100 + ((150 - 100) / (55.4 - 35.5)) * (pm25 - 35.5)
#             category = "Unhealthy for Sensitive Groups"
#         elif pm25 <= 150.4:
#             aqi = 150 + ((200 - 150) / (150.4 - 55.5)) * (pm25 - 55.5)
#             category = "Unhealthy"
#         elif pm25 <= 250.4:
#             aqi = 200 + ((300 - 200) / (250.4 - 150.5)) * (pm25 - 150.5)
#             category = "Very Unhealthy"
#         else:
#             aqi = 300 + ((500 - 300) / (500.4 - 250.5)) * (pm25 - 250.5)
#             category = "Hazardous"
        
#         return int(aqi), category
    
#     def get_comprehensive_data(self, city: str, country: Optional[str], date: str) -> Dict[str, Any]:
#         """Get all available environmental data for a location and date."""
#         result = {
#             'location': None,
#             'date': date,
#             'air_quality': None,
#             'weather': None,
#             'no2': None,
#             'fire': None
#         }
        
#         aq_data = self.query_air_quality(city, country, date)
#         if aq_data:
#             result['location'] = aq_data['location']
#             result['air_quality'] = aq_data
        
#         weather_data = self.query_weather_data(city, country, date)
#         if weather_data:
#             result['weather'] = weather_data
        
#         no2_data = self.query_no2_data(city, country, date)
#         if no2_data:
#             result['no2'] = no2_data
        
#         fire_data = self.query_fire_data(city, country, date)
#         if fire_data:
#             result['fire'] = fire_data
        
#         return result
    
#     def get_available_cities(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
#         """Get list of cities available in the database."""
#         try:
#             cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
#             query = """
#             SELECT DISTINCT 
#                 city, 
#                 country, 
#                 COUNT(*) as record_count,
#                 MAX(DATE(datetime_utc)) as latest_date,
#                 MIN(DATE(datetime_utc)) as earliest_date
#             FROM air_quality_data
#             GROUP BY city, country
#             ORDER BY record_count DESC
#             """
            
#             if limit:
#                 query += f" LIMIT {limit}"
            
#             cursor.execute(query)
#             results = cursor.fetchall()
#             cursor.close()
            
#             print(f"Retrieved {len(results)} cities from database")
#             return results
            
#         except Exception as e:
#             print(f"Error getting available cities: {e}")
#             return []
    
#     def diagnose_database_content(self) -> Dict[str, Any]:
#         """Diagnose what's actually in the database."""
#         try:
#             cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
#             cursor.execute("""
#                 SELECT COUNT(DISTINCT city) as unique_cities,
#                        COUNT(DISTINCT country) as unique_countries,
#                        COUNT(*) as total_records
#                 FROM air_quality_data
#             """)
#             summary = cursor.fetchone()
            
#             cursor.execute("""
#                 SELECT DISTINCT city, country
#                 FROM air_quality_data
#                 ORDER BY city, country
#             """)
#             all_locations = cursor.fetchall()
            
#             cursor.execute("""
#                 SELECT city, country, parameter_name, datetime_utc
#                 FROM air_quality_data
#                 ORDER BY datetime_utc DESC
#                 LIMIT 5
#             """)
#             sample_records = cursor.fetchall()
            
#             cursor.close()
            
#             return {
#                 'summary': summary,
#                 'all_locations': all_locations,
#                 'sample_records': sample_records
#             }
            
#         except Exception as e:
#             print(f"Diagnostic error: {e}")
#             return None


# # ============================================================================
# # PARSED QUERY MODEL
# # ============================================================================

# class ParsedQuery(BaseModel):
#     """Structured output for parsed user query with validation."""
#     city: Optional[str] = Field(default=None, description="City name if explicitly mentioned, otherwise None")
#     country: Optional[str] = Field(default=None, description="Country name if mentioned")
#     query_type: List[str] = Field(description="Type of information requested")
#     time_reference: str = Field(description="Time reference: 'today', 'tomorrow', or YYYY-MM-DD")
#     original_query: str = Field(description="The original user query")
#     needs_clarification: bool = Field(default=False, description="True if city is missing or ambiguous")
#     clarification_reason: Optional[str] = Field(default=None, description="Why clarification is needed")


# # ============================================================================
# # QUERY PARSER
# # ============================================================================

# class ImprovedQueryParser:
#     """Enhanced parser that validates city information."""
    
#     def __init__(self):
#         self.llm = AzureChatOpenAI(
#             deployment_name="gpt-4o",
#             api_version="2024-12-01-preview",
#             azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "https://hassan-siddiqui.openai.azure.com/"),
#             api_key=os.getenv("AZURE_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY"),
#             temperature=0
#         )
#         self.parser = PydanticOutputParser(pydantic_object=ParsedQuery)
#         self.setup_chain()
    
#     def setup_chain(self):
#         """Setup the parsing chain with improved prompt."""
        
#         system_template = """You are an expert at parsing environmental and weather queries.

# CRITICAL RULES FOR CITY DETECTION:
# 1. **ONLY extract a city name if it is EXPLICITLY mentioned in the query**
# 2. If NO city is mentioned, you MUST set city=None and needs_clarification=True
# 3. DO NOT assume, infer, or use default cities under ANY circumstances
# 4. DO NOT use cities like "Burlington", "New York", or any other city unless explicitly stated

# CRITICAL RULES FOR TIME REFERENCE:
# 1. **ALWAYS provide a time_reference as a string, NEVER null**
# 2. If no time is mentioned, default to "today"
# 3. Convert "now", "currently", "right now" to "today"
# 4. Convert "tomorrow" to "tomorrow"
# 5. Keep explicit dates as-is (YYYY-MM-DD format)

# Extract the following information:
# - city: Extract ONLY if explicitly mentioned, otherwise set to None
# - country: Extract if mentioned
# - query_type: air_quality, weather, aqi, pollutants, forecast, activity_safety, temperature, humidity, wind, fire
# - time_reference: MUST be a string ('today', 'tomorrow', or YYYY-MM-DD), NEVER null. Default to 'today' if not specified.
# - needs_clarification: Set to True if city is not specified
# - clarification_reason: Explain what information is missing
# - original_query: The exact user query

# Current date: {current_date}

# {format_instructions}

# EXAMPLES:

# Query: "Can I go for a picnic in Toronto tomorrow?"
# Output: {{"city": "Toronto", "country": null, "needs_clarification": false, "query_type": ["activity_safety", "air_quality", "weather"], "time_reference": "tomorrow", "original_query": "Can I go for a picnic in Toronto tomorrow?"}}

# Query: "Is it safe to go jogging in morning?"
# Output: {{"city": null, "country": null, "needs_clarification": true, "clarification_reason": "City not specified in query", "query_type": ["activity_safety", "air_quality"], "time_reference": "today", "original_query": "Is it safe to go jogging in morning?"}}

# Query: "What's the weather like?"
# Output: {{"city": null, "country": null, "needs_clarification": true, "clarification_reason": "Location not provided", "query_type": ["weather"], "time_reference": "today", "original_query": "What's the weather like?"}}

# Query: "Air quality in Miami today?"
# Output: {{"city": "Miami", "country": null, "needs_clarification": false, "query_type": ["air_quality"], "time_reference": "today", "original_query": "Air quality in Miami today?"}}

# Query: "Is it good to visit Miami or not?"
# Output: {{"city": "Miami", "country": null, "needs_clarification": false, "query_type": ["activity_safety", "air_quality", "weather"], "time_reference": "today", "original_query": "Is it good to visit Miami or not?"}}

# REMEMBER: 
# - When in doubt about city, set city=None and ask for clarification
# - ALWAYS set time_reference to a string, never null (default to "today")
# """
        
#         human_template = """Parse this query: {query}"""
        
#         system_message = SystemMessagePromptTemplate.from_template(system_template)
#         human_message = HumanMessagePromptTemplate.from_template(human_template)
        
#         chat_prompt = ChatPromptTemplate.from_messages([system_message, human_message])
        
#         self.chain = LLMChain(llm=self.llm, prompt=chat_prompt)
    
#     def parse(self, query: str) -> ParsedQuery:
#         """Parse a natural language query with validation."""
#         current_date = datetime.now().strftime("%Y-%m-%d")
        
#         result = self.chain.run(
#             query=query,
#             current_date=current_date,
#             format_instructions=self.parser.get_format_instructions()
#         )
        
#         parsed = self.parser.parse(result)
        
#         if not parsed.city or parsed.city.strip() == "":
#             parsed.city = None
#             parsed.needs_clarification = True
#             if not parsed.clarification_reason:
#                 parsed.clarification_reason = "Please specify which city you're asking about"
        
#         return parsed
    
#     def convert_time_to_date(self, time_reference: str) -> str:
#         """Convert time reference to date string."""
#         today = datetime.now()
        
#         if time_reference.lower() in ["today", "now"]:
#             return today.strftime("%Y-%m-%d")
#         elif time_reference.lower() == "tomorrow":
#             return (today + timedelta(days=1)).strftime("%Y-%m-%d")
#         else:
#             return time_reference


# # ============================================================================
# # RESPONSE GENERATOR - BRIEF VERSION
# # ============================================================================

# class ImprovedResponseGenerator:
#     """Enhanced response generator with brief, concise outputs."""
    
#     def __init__(self):
#         self.llm = AzureChatOpenAI(
#             deployment_name="gpt-4o",
#             api_version="2024-12-01-preview",
#             azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "https://hassan-siddiqui.openai.azure.com/"),
#             api_key=os.getenv("AZURE_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY"),
#             temperature=0.7
#         )
#         self.setup_chain()
    
#     def setup_chain(self):
#         """Setup response generation chain with brief output prompt."""
        
#         system_template = """You are an environmental data assistant providing brief, factual information.

# CRITICAL: Keep responses to 4-6 lines maximum. Be concise and direct.

# RESPONSE RULES:
# 1. Answer in 4-6 lines ONLY
# 2. State the key finding first
# 3. Give a direct yes/no or recommendation when asked
# 4. Include only the most relevant data point
# 5. Skip lengthy explanations

# EPA AQI Quick Reference:
# - Good (0-50): Safe for all activities
# - Moderate (51-100): Acceptable for most
# - Unhealthy for Sensitive (101-150): Sensitive groups take precautions
# - Unhealthy (151-200): General public affected
# - Very Unhealthy (201-300): Serious effects for everyone
# - Hazardous (301+): Emergency conditions

# DATABASE INFORMATION:
# {database_result}

# USER'S QUESTION: {original_query}

# Provide a brief, direct answer in 4-6 lines."""

#         human_template = """Provide a response based on the database information."""
        
#         system_message = SystemMessagePromptTemplate.from_template(system_template)
#         human_message = HumanMessagePromptTemplate.from_template(human_template)
        
#         chat_prompt = ChatPromptTemplate.from_messages([system_message, human_message])
        
#         self.chain = LLMChain(llm=self.llm, prompt=chat_prompt)
    
#     def format_database_result(self, data: Dict[str, Any]) -> str:
#         """Format database result for the LLM prompt."""
#         if not data['location'] and not any([data['air_quality'], data['weather'], data['no2'], data['fire']]):
#             return "No data available in the database for this location and date."
        
#         result = f"REQUESTED DATE: {data['date']}\n"
        
#         if data['location']:
#             result += f"LOCATION: {data['location']}\n"
        
#         if data.get('air_quality') and data['air_quality'].get('actual_date'):
#             if data['air_quality']['actual_date'] != data['date']:
#                 result += f"NOTE: Showing data from {data['air_quality']['actual_date']} (most recent available)\n"
        
#         result += "\n"
        
#         if data['air_quality']:
#             aq = data['air_quality']
#             result += "AIR QUALITY DATA:\n"
            
#             if 'aqi' in aq:
#                 aqi_val, aqi_cat = aq['aqi']
#                 result += f"- AQI: {aqi_val} ({aqi_cat})\n"
            
#             if 'pollutants' in aq:
#                 for param, info in aq['pollutants'].items():
#                     result += f"- {param.upper()}: {info['value']:.2f} {info['units']}\n"
#             result += "\n"
        
#         if data['weather']:
#             w = data['weather']
#             result += "WEATHER DATA:\n"
            
#             if w.get('actual_date') and w['actual_date'] != data['date']:
#                 result += f"(Data from {w['actual_date']})\n"
            
#             if 'variables' in w:
#                 for var, info in w['variables'].items():
#                     result += f"- {var.replace('_', ' ').title()}: {info['value']:.2f} {info['units']}\n"
#             result += "\n"
        
#         if data['no2']:
#             result += f"NITROGEN DIOXIDE:\n"
#             result += f"- NO2 Column: {data['no2']['no2_column']:.2e} {data['no2']['units']}\n\n"
        
#         if data['fire']:
#             f = data['fire']
#             result += "FIRE ACTIVITY:\n"
#             if f.get('actual_date') and f['actual_date'] != data['date']:
#                 result += f"(Data from {f['actual_date']})\n"
#             result += f"- Active fires detected: {f['fire_count']}\n"
#             result += f"- Average fire power: {f['avg_fire_power']:.2f} MW\n"
#             result += f"- Detection confidence: {f['max_confidence']}\n\n"
        
#         return result
    
#     def generate(self, original_query: str, database_result: Dict[str, Any]) -> str:
#         """Generate a response with error handling."""
#         formatted_result = self.format_database_result(database_result)
        
#         try:
#             response = self.chain.run(
#                 original_query=original_query,
#                 database_result=formatted_result
#             )
#             return response
            
#         except Exception as e:
#             if "content_filter" in str(e) or "ResponsibleAIPolicyViolation" in str(e):
#                 return self.generate_safe_fallback(original_query, formatted_result)
#             else:
#                 raise
    
#     def generate_safe_fallback(self, query: str, data: str) -> str:
#         """Safe fallback response for content filter issues."""
#         if "No data available" in data:
#             return "I don't have environmental data available for that location. Please specify a city with available data."
        
#         return f"""Based on the available environmental data:

# {data}

# For specific recommendations about outdoor activities, please consult the AQI values and EPA guidelines above. Generally:
# - AQI 0-50 (Good): Outdoor activities are suitable for everyone
# - AQI 51-100 (Moderate): Acceptable for most people
# - AQI 101-150: Sensitive individuals should consider reducing prolonged outdoor exertion
# - AQI 151+: Everyone should reduce outdoor activities

# Would you like more specific information about any particular measurement?"""


# # ============================================================================
# # MAIN SYSTEM
# # ============================================================================

# class ImprovedEnvironmentalQuerySystem:
#     """Enhanced system with validation and better error handling."""
    
#     def __init__(self, db_config: Optional[Dict] = None):
#         if db_config:
#             self.db_manager = DatabaseManager(**db_config)
#         else:
#             self.db_manager = DatabaseManager()
        
#         self.db_manager.connect()
#         self.query_parser = ImprovedQueryParser()
#         self.response_generator = ImprovedResponseGenerator()
    
#     def process_query(self, user_query: str) -> str:
#         """Process a user query with validation."""
        
#         try:
#             print("\nParsing query...")
#             parsed_query = self.query_parser.parse(user_query)
#             print(f"   City: {parsed_query.city}")
#             print(f"   Country: {parsed_query.country}")
#             print(f"   Query Type: {parsed_query.query_type}")
#             print(f"   Time: {parsed_query.time_reference}")
#             print(f"   Needs Clarification: {parsed_query.needs_clarification}")
            
#             # Check if query_type is empty or query is invalid
#             if not parsed_query.query_type or len(parsed_query.query_type) == 0:
#                 return """I'm not sure I understand your question. 

# I can help you with environmental and air quality information. Please ask questions like:
# - "What is the air quality in [city name] today?"
# - "Is it safe to go jogging in [city name]?"
# - "Tell me about weather conditions in [city name]"
# - "What's the AQI in [city name]?"

# Which city would you like information about?"""
            
#             if parsed_query.needs_clarification or not parsed_query.city:
#                 print("\nCity not specified, requesting clarification...")
#                 return self.request_clarification(parsed_query)
            
#             date = self.query_parser.convert_time_to_date(parsed_query.time_reference)
#             print(f"   Date: {date}")
            
#             print("\nQuerying database...")
#             db_result = self.db_manager.get_comprehensive_data(
#                 parsed_query.city,
#                 parsed_query.country,
#                 date
#             )
            
#             if not db_result['location'] and not any([
#                 db_result['air_quality'], 
#                 db_result['weather'], 
#                 db_result['no2'], 
#                 db_result['fire']
#             ]):
#                 print(f"\nNo data found for {parsed_query.city}")
#                 return self.suggest_available_cities(parsed_query.city)
            
#             print("\nGenerating response...\n")
#             response = self.response_generator.generate(user_query, db_result)
            
#             return response
            
#         except Exception as e:
#             if "content_filter" in str(e):
#                 return self.handle_content_filter_error(user_query)
#             print(f"Error details: {str(e)}")
#             return """I encountered an issue processing your request. 

# Please make sure to:
# 1. Specify a city name in your query
# 2. Ask about environmental or weather conditions
# 3. Use complete questions

# Examples:
# - "What is the air quality in Miami today?"
# - "Is it safe to exercise outdoors in Los Angeles?"
# - "Tell me about weather in Toronto"

# What would you like to know?"""
    
#     def request_clarification(self, parsed_query: ParsedQuery) -> str:
#         """Request city information from user."""
#         cities = self.db_manager.get_available_cities(limit=10)
        
#         if not cities:
#             return "I'd be happy to help, but I need to know which city you're asking about. Could you please specify a city name?"
        
#         city_examples = [f"{c['city']}, {c['country']}" for c in cities]
#         city_list = ", ".join(city_examples[:5])
        
#         query_type_text = "environmental conditions"
#         if parsed_query.query_type and len(parsed_query.query_type) > 0:
#             query_type_text = parsed_query.query_type[0].replace('_', ' ')
        
#         return f"""I'd be happy to help you with your question about {query_type_text}!

# However, I need to know which city you're asking about to provide accurate information.

# Please specify a city name. For example:
# - "Is it safe to go jogging in Miami?"
# - "What's the air quality in Los Angeles today?"
# - "Tell me about weather conditions in Toronto"

# Some cities with available data include: {city_list}, and many more.

# Which city would you like information for?"""
    
#     def suggest_available_cities(self, attempted_city: str) -> str:
#         """Suggest alternatives when city not found."""
#         cities = self.db_manager.get_available_cities(limit=30)
        
#         if not cities:
#             return f"I couldn't find data for '{attempted_city}' in the database. The database appears to be empty or unavailable."
        
#         city_list = "\n".join([
#             f"  - {c['city']}, {c['country']} (data from {c['earliest_date']} to {c['latest_date']})" 
#             for c in cities
#         ])
        
#         return f"""I couldn't find environmental data for "{attempted_city}" in our database.

# Here are some cities with available air quality and weather data:

# {city_list}

# Would you like to query one of these cities instead? Just ask about any city from the list!"""
    
#     def handle_content_filter_error(self, query: str) -> str:
#         """Handle Azure content filter errors gracefully."""
#         return """I can help you with environmental and air quality information!

# To provide you with accurate data, please ask specific questions like:
# - "What is the air quality index in [city name] today?"
# - "Show me PM2.5 and pollutant levels in [city name]"
# - "What are the current weather conditions in [city name]?"
# - "Tell me about air quality in [city name] for outdoor activities"

# I'll provide objective measurements and standard EPA interpretations to help inform your decisions.

# Which city would you like information about?"""
    
#     def close(self):
#         """Clean up resources."""
#         self.db_manager.close()


# # ============================================================================
# # USAGE EXAMPLE
# # ============================================================================

# def main():
#     """Example usage with brief responses."""
    
#     DB_CONFIG = {
#         "host": "localhost",
#         "port": "5000",
#         "dbname": "db",
#         "user": "db_user",
#         "password": "db_password"
#     }
    
#     system = ImprovedEnvironmentalQuerySystem(DB_CONFIG)
    
#     try:
#         print("\n" + "=" * 80)
#         print("DATABASE DIAGNOSTICS")
#         print("=" * 80)
#         diagnostics = system.db_manager.diagnose_database_content()
        
#         if diagnostics:
#             summary = diagnostics['summary']
#             print(f"\nDatabase Summary:")
#             print(f"   Total records: {summary['total_records']}")
#             print(f"   Unique cities: {summary['unique_cities']}")
#             print(f"   Unique countries: {summary['unique_countries']}")
            
#             print(f"\nAll City/Country Combinations ({len(diagnostics['all_locations'])} total):")
#             for i, loc in enumerate(diagnostics['all_locations'][:50], 1):
#                 print(f"   {i:3}. {loc['city']}, {loc['country']}")
            
#             if len(diagnostics['all_locations']) > 50:
#                 print(f"   ... and {len(diagnostics['all_locations']) - 50} more")
            
#             print(f"\nSample Records:")
#             for rec in diagnostics['sample_records']:
#                 print(f"   {rec['city']}, {rec['country']} - {rec['parameter_name']} at {rec['datetime_utc']}")
        
#         print("\n" + "=" * 80)
#         print("DATABASE STATUS")
#         print("=" * 80)
#         cities = system.db_manager.get_available_cities()
        
#         if cities:
#             print(f"\nFound {len(cities)} cities with air quality data\n")
#             print(f"{'City':<30} {'Country':<30} {'Records':<10} {'Date Range':<30}")
#             print("=" * 100)
#             for city in cities[:20]:
#                 date_range = f"{city['earliest_date']} to {city['latest_date']}"
#                 print(f"{city['city']:<30} {city['country']:<30} {city['record_count']:<10} {date_range:<30}")
            
#             if len(cities) > 20:
#                 print(f"\n... and {len(cities) - 20} more cities")
#         else:
#             print("\nNo cities found in database!")
        
#         test_queries = [
#             "Is it safe to go jogging in the morning?",
#             "Is it safe to go jogging in Miami?",
#             "What's the air quality like?",
#             "Tell me about air quality in Los Angeles today",
#             "What is the air quality index in New York today?",
#         ]
        
#         print("\n" + "=" * 80)
#         print("TESTING ENVIRONMENTAL QUERY SYSTEM - BRIEF RESPONSES")
#         print("=" * 80)
        
#         for i, query in enumerate(test_queries, 1):
#             print(f"\n{'=' * 80}")
#             print(f"TEST QUERY {i}: {query}")
#             print('=' * 80)
            
#             response = system.process_query(query)
#             print(f"\nRESPONSE:\n{response}\n")
        
#         print("\n" + "=" * 80)
#         print("INTERACTIVE MODE")
#         print("=" * 80)
#         print("\nYou can now ask questions about air quality and weather.")
#         print("Examples:")
#         print("  - 'What is the AQI in Toronto today?'")
#         print("  - 'Is it safe to exercise outdoors in Miami?'")
#         print("  - 'Tell me about air quality in Los Angeles'")
#         print("\nType 'quit' to exit\n")
        
#         while True:
#             user_input = input("Your question: ").strip()
            
#             if not user_input:
#                 continue
                
#             if user_input.lower() in ['quit', 'exit', 'q']:
#                 print("\nThank you for using the Environmental Query System!")
#                 break
            
#             response = system.process_query(user_input)
#             print(f"\nResponse:\n{response}\n")
    
#     except KeyboardInterrupt:
#         print("\n\nInterrupted by user. Closing...")
#     except Exception as e:
#         print(f"\n\nAn error occurred: {e}")
#     finally:
#         system.close()


# if __name__ == "__main__":
#     main()







"""
Safer Skies Query System - Brief Response Version
Fixed to give concise 4-6 line answers with improved error handling
Updated with greeting/farewell handling
"""

import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from langchain_openai import AzureChatOpenAI
from dotenv import load_dotenv
from langchain.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate
from langchain.output_parsers import PydanticOutputParser
from langchain.chains import LLMChain
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.extras import RealDictCursor
import json

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
    
    def query_weather_data(self, city: str, country: Optional[str], date: str) -> Dict[str, Any]:
        """Query meteorological data from MERRA-2."""
        try:
            cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
            loc_query = """
            SELECT DISTINCT latitude, longitude
            FROM air_quality_data
            WHERE LOWER(city) LIKE LOWER(%s)
            LIMIT 1
            """
            cursor.execute(loc_query, (f"%{city}%",))
            location = cursor.fetchone()
            
            if not location:
                cursor.close()
                return None
            
            lat = float(location['latitude'])
            lon = float(location['longitude'])
            
            query = """
            SELECT 
                variable_name,
                AVG(variable_value) as avg_value,
                variable_units,
                latitude,
                longitude,
                DATE(granule_time_start) as data_date
            FROM merra2_slv_data
            WHERE DATE(granule_time_start) >= %s::date - INTERVAL '7 days'
                AND DATE(granule_time_start) <= %s::date
                AND latitude BETWEEN %s AND %s
                AND longitude BETWEEN %s AND %s
            GROUP BY variable_name, variable_units, latitude, longitude, data_date
            ORDER BY data_date DESC
            LIMIT 50
            """
            
            cursor.execute(query, (date, date, lat-0.5, lat+0.5, lon-0.5, lon+0.5))
            results = cursor.fetchall()
            cursor.close()
            
            if not results:
                return None
            
            data = {
                'latitude': lat,
                'longitude': lon,
                'date': date,
                'actual_date': str(results[0]['data_date']) if results else date,
                'variables': {}
            }
            
            for row in results:
                var_name = row['variable_name']
                value = float(row['avg_value']) if row['avg_value'] else None
                
                if var_name == 'T2M' and value:
                    value = value - 273.15
                    data['variables']['temperature'] = {'value': value, 'units': '°C'}
                elif var_name == 'QV2M':
                    data['variables']['humidity'] = {'value': value * 100, 'units': '%'}
                elif var_name in ['U10M', 'V10M']:
                    if 'wind_speed' not in data['variables']:
                        data['variables']['wind_speed'] = {'value': value, 'units': 'm/s'}
            
            return data
            
        except Exception as e:
            print(f"Error querying weather data: {e}")
            return None
    
    def query_no2_data(self, city: str, country: Optional[str], date: str) -> Dict[str, Any]:
        """Query NO2 tropospheric column data."""
        try:
            cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
            loc_query = """
            SELECT DISTINCT latitude, longitude
            FROM air_quality_data
            WHERE LOWER(city) LIKE LOWER(%s)
            LIMIT 1
            """
            cursor.execute(loc_query, (f"%{city}%",))
            location = cursor.fetchone()
            
            if not location:
                cursor.close()
                return None
            
            lat = float(location['latitude'])
            lon = float(location['longitude'])
            
            query = """
            SELECT 
                AVG(no2_tropospheric_column) as avg_no2,
                observation_datetime
            FROM tempo_no2_data
            WHERE DATE(observation_datetime) = %s
                AND latitude BETWEEN %s AND %s
                AND longitude BETWEEN %s AND %s
            GROUP BY observation_datetime
            ORDER BY observation_datetime DESC
            LIMIT 1
            """
            
            cursor.execute(query, (date, lat-0.5, lat+0.5, lon-0.5, lon+0.5))
            result = cursor.fetchone()
            cursor.close()
            
            if result and result['avg_no2']:
                return {
                    'no2_column': float(result['avg_no2']),
                    'units': 'molecules/cm²',
                    'observation_time': result['observation_datetime']
                }
            
            return None
            
        except Exception as e:
            print(f"Error querying NO2 data: {e}")
            return None
    
    def query_fire_data(self, city: str, country: Optional[str], date: str) -> Dict[str, Any]:
        """Query fire detection data."""
        try:
            cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
            loc_query = """
            SELECT DISTINCT latitude, longitude
            FROM air_quality_data
            WHERE LOWER(city) LIKE LOWER(%s)
            LIMIT 1
            """
            cursor.execute(loc_query, (f"%{city}%",))
            location = cursor.fetchone()
            
            if not location:
                cursor.close()
                return None
            
            lat = float(location['latitude'])
            lon = float(location['longitude'])
            
            query = """
            SELECT 
                COUNT(*) as fire_count,
                AVG(frp) as avg_frp,
                MAX(confidence) as max_confidence,
                acq_date as data_date
            FROM fire_detection_data
            WHERE acq_date >= %s::date - INTERVAL '7 days'
                AND acq_date <= %s::date
                AND latitude BETWEEN %s AND %s
                AND longitude BETWEEN %s AND %s
            GROUP BY acq_date
            ORDER BY acq_date DESC
            LIMIT 1
            """
            
            cursor.execute(query, (date, date, lat-1, lat+1, lon-1, lon+1))
            result = cursor.fetchone()
            cursor.close()
            
            if result and result['fire_count'] > 0:
                return {
                    'fire_count': result['fire_count'],
                    'avg_fire_power': float(result['avg_frp']) if result['avg_frp'] else 0,
                    'max_confidence': result['max_confidence'],
                    'actual_date': str(result['data_date']) if result.get('data_date') else date
                }
            
            return None
            
        except Exception as e:
            print(f"Error querying fire data: {e}")
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
    
    def get_comprehensive_data(self, city: str, country: Optional[str], date: str) -> Dict[str, Any]:
        """Get all available environmental data for a location and date."""
        result = {
            'location': None,
            'date': date,
            'air_quality': None,
            'weather': None,
            'no2': None,
            'fire': None
        }
        
        aq_data = self.query_air_quality(city, country, date)
        if aq_data:
            result['location'] = aq_data['location']
            result['air_quality'] = aq_data
        
        weather_data = self.query_weather_data(city, country, date)
        if weather_data:
            result['weather'] = weather_data
        
        no2_data = self.query_no2_data(city, country, date)
        if no2_data:
            result['no2'] = no2_data
        
        fire_data = self.query_fire_data(city, country, date)
        if fire_data:
            result['fire'] = fire_data
        
        return result
    
    def get_available_cities(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get list of cities available in the database."""
        try:
            cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
            query = """
            SELECT DISTINCT 
                city, 
                country, 
                COUNT(*) as record_count,
                MAX(DATE(datetime_utc)) as latest_date,
                MIN(DATE(datetime_utc)) as earliest_date
            FROM air_quality_data
            GROUP BY city, country
            ORDER BY record_count DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            print(f"Retrieved {len(results)} cities from database")
            return results
            
        except Exception as e:
            print(f"Error getting available cities: {e}")
            return []
    
    def diagnose_database_content(self) -> Dict[str, Any]:
        """Diagnose what's actually in the database."""
        try:
            cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT COUNT(DISTINCT city) as unique_cities,
                       COUNT(DISTINCT country) as unique_countries,
                       COUNT(*) as total_records
                FROM air_quality_data
            """)
            summary = cursor.fetchone()
            
            cursor.execute("""
                SELECT DISTINCT city, country
                FROM air_quality_data
                ORDER BY city, country
            """)
            all_locations = cursor.fetchall()
            
            cursor.execute("""
                SELECT city, country, parameter_name, datetime_utc
                FROM air_quality_data
                ORDER BY datetime_utc DESC
                LIMIT 5
            """)
            sample_records = cursor.fetchall()
            
            cursor.close()
            
            return {
                'summary': summary,
                'all_locations': all_locations,
                'sample_records': sample_records
            }
            
        except Exception as e:
            print(f"Diagnostic error: {e}")
            return None


# ============================================================================
# PARSED QUERY MODEL
# ============================================================================

class ParsedQuery(BaseModel):
    """Structured output for parsed user query with validation."""
    city: Optional[str] = Field(default=None, description="City name if explicitly mentioned, otherwise None")
    country: Optional[str] = Field(default=None, description="Country name if mentioned")
    query_type: List[str] = Field(description="Type of information requested")
    time_reference: str = Field(description="Time reference: 'today', 'tomorrow', or YYYY-MM-DD")
    original_query: str = Field(description="The original user query")
    needs_clarification: bool = Field(default=False, description="True if city is missing or ambiguous")
    clarification_reason: Optional[str] = Field(default=None, description="Why clarification is needed")


# ============================================================================
# QUERY PARSER
# ============================================================================

class ImprovedQueryParser:
    """Enhanced parser that validates city information."""
    
    def __init__(self):
        self.llm = AzureChatOpenAI(
            deployment_name="gpt-4o",
            api_version="2024-12-01-preview",
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "https://hassan-siddiqui.openai.azure.com/"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY"),
            temperature=0
        )
        self.parser = PydanticOutputParser(pydantic_object=ParsedQuery)
        self.setup_chain()
    
    def setup_chain(self):
        """Setup the parsing chain with improved prompt."""
        
        system_template = """You are an expert at parsing environmental and weather queries.

CRITICAL RULES FOR CITY DETECTION:
1. **ONLY extract a city name if it is EXPLICITLY mentioned in the query**
2. If NO city is mentioned, you MUST set city=None and needs_clarification=True
3. DO NOT assume, infer, or use default cities under ANY circumstances
4. DO NOT use cities like "Burlington", "New York", or any other city unless explicitly stated

CRITICAL RULES FOR TIME REFERENCE:
1. **ALWAYS provide a time_reference as a string, NEVER null**
2. If no time is mentioned, default to "today"
3. Convert "now", "currently", "right now" to "today"
4. Convert "tomorrow" to "tomorrow"
5. Keep explicit dates as-is (YYYY-MM-DD format)

Extract the following information:
- city: Extract ONLY if explicitly mentioned, otherwise set to None
- country: Extract if mentioned
- query_type: air_quality, weather, aqi, pollutants, forecast, activity_safety, temperature, humidity, wind, fire
- time_reference: MUST be a string ('today', 'tomorrow', or YYYY-MM-DD), NEVER null. Default to 'today' if not specified.
- needs_clarification: Set to True if city is not specified
- clarification_reason: Explain what information is missing
- original_query: The exact user query

Current date: {current_date}

{format_instructions}

EXAMPLES:

Query: "Can I go for a picnic in Toronto tomorrow?"
Output: {{"city": "Toronto", "country": null, "needs_clarification": false, "query_type": ["activity_safety", "air_quality", "weather"], "time_reference": "tomorrow", "original_query": "Can I go for a picnic in Toronto tomorrow?"}}

Query: "Is it safe to go jogging in morning?"
Output: {{"city": null, "country": null, "needs_clarification": true, "clarification_reason": "City not specified in query", "query_type": ["activity_safety", "air_quality"], "time_reference": "today", "original_query": "Is it safe to go jogging in morning?"}}

Query: "What's the weather like?"
Output: {{"city": null, "country": null, "needs_clarification": true, "clarification_reason": "Location not provided", "query_type": ["weather"], "time_reference": "today", "original_query": "What's the weather like?"}}

Query: "Air quality in Miami today?"
Output: {{"city": "Miami", "country": null, "needs_clarification": false, "query_type": ["air_quality"], "time_reference": "today", "original_query": "Air quality in Miami today?"}}

Query: "Is it good to visit Miami or not?"
Output: {{"city": "Miami", "country": null, "needs_clarification": false, "query_type": ["activity_safety", "air_quality", "weather"], "time_reference": "today", "original_query": "Is it good to visit Miami or not?"}}

REMEMBER: 
- When in doubt about city, set city=None and ask for clarification
- ALWAYS set time_reference to a string, never null (default to "today")
"""
        
        human_template = """Parse this query: {query}"""
        
        system_message = SystemMessagePromptTemplate.from_template(system_template)
        human_message = HumanMessagePromptTemplate.from_template(human_template)
        
        chat_prompt = ChatPromptTemplate.from_messages([system_message, human_message])
        
        self.chain = LLMChain(llm=self.llm, prompt=chat_prompt)
    
    def parse(self, query: str) -> ParsedQuery:
        """Parse a natural language query with validation."""
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        result = self.chain.run(
            query=query,
            current_date=current_date,
            format_instructions=self.parser.get_format_instructions()
        )
        
        parsed = self.parser.parse(result)
        
        if not parsed.city or parsed.city.strip() == "":
            parsed.city = None
            parsed.needs_clarification = True
            if not parsed.clarification_reason:
                parsed.clarification_reason = "Please specify which city you're asking about"
        
        return parsed
    
    def convert_time_to_date(self, time_reference: str) -> str:
        """Convert time reference to date string."""
        today = datetime.now()
        
        if time_reference.lower() in ["today", "now"]:
            return today.strftime("%Y-%m-%d")
        elif time_reference.lower() == "tomorrow":
            return (today + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            return time_reference


# ============================================================================
# RESPONSE GENERATOR - BRIEF VERSION
# ============================================================================

class ImprovedResponseGenerator:
    """Enhanced response generator with brief, concise outputs."""
    
    def __init__(self):
        self.llm = AzureChatOpenAI(
            deployment_name="gpt-4o",
            api_version="2024-12-01-preview",
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", "https://hassan-siddiqui.openai.azure.com/"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY"),
            temperature=0.7
        )
        self.setup_chain()
    
    def setup_chain(self):
        """Setup response generation chain with brief output prompt."""
        
        system_template = """You are an environmental data assistant providing brief, factual information.

CRITICAL: Keep responses to 4-6 lines maximum. Be concise and direct.

RESPONSE RULES:
1. Answer in 4-6 lines ONLY
2. State the key finding first
3. Give a direct yes/no or recommendation when asked
4. Include only the most relevant data point
5. Skip lengthy explanations

EPA AQI Quick Reference:
- Good (0-50): Safe for all activities
- Moderate (51-100): Acceptable for most
- Unhealthy for Sensitive (101-150): Sensitive groups take precautions
- Unhealthy (151-200): General public affected
- Very Unhealthy (201-300): Serious effects for everyone
- Hazardous (301+): Emergency conditions

DATABASE INFORMATION:
{database_result}

USER'S QUESTION: {original_query}

Provide a brief, direct answer in 4-6 lines."""

        human_template = """Provide a response based on the database information."""
        
        system_message = SystemMessagePromptTemplate.from_template(system_template)
        human_message = HumanMessagePromptTemplate.from_template(human_template)
        
        chat_prompt = ChatPromptTemplate.from_messages([system_message, human_message])
        
        self.chain = LLMChain(llm=self.llm, prompt=chat_prompt)
    
    def format_database_result(self, data: Dict[str, Any]) -> str:
        """Format database result for the LLM prompt."""
        if not data['location'] and not any([data['air_quality'], data['weather'], data['no2'], data['fire']]):
            return "No data available in the database for this location and date."
        
        result = f"REQUESTED DATE: {data['date']}\n"
        
        if data['location']:
            result += f"LOCATION: {data['location']}\n"
        
        if data.get('air_quality') and data['air_quality'].get('actual_date'):
            if data['air_quality']['actual_date'] != data['date']:
                result += f"NOTE: Showing data from {data['air_quality']['actual_date']} (most recent available)\n"
        
        result += "\n"
        
        if data['air_quality']:
            aq = data['air_quality']
            result += "AIR QUALITY DATA:\n"
            
            if 'aqi' in aq:
                aqi_val, aqi_cat = aq['aqi']
                result += f"- AQI: {aqi_val} ({aqi_cat})\n"
            
            if 'pollutants' in aq:
                for param, info in aq['pollutants'].items():
                    result += f"- {param.upper()}: {info['value']:.2f} {info['units']}\n"
            result += "\n"
        
        if data['weather']:
            w = data['weather']
            result += "WEATHER DATA:\n"
            
            if w.get('actual_date') and w['actual_date'] != data['date']:
                result += f"(Data from {w['actual_date']})\n"
            
            if 'variables' in w:
                for var, info in w['variables'].items():
                    result += f"- {var.replace('_', ' ').title()}: {info['value']:.2f} {info['units']}\n"
            result += "\n"
        
        if data['no2']:
            result += f"NITROGEN DIOXIDE:\n"
            result += f"- NO2 Column: {data['no2']['no2_column']:.2e} {data['no2']['units']}\n\n"
        
        if data['fire']:
            f = data['fire']
            result += "FIRE ACTIVITY:\n"
            if f.get('actual_date') and f['actual_date'] != data['date']:
                result += f"(Data from {f['actual_date']})\n"
            result += f"- Active fires detected: {f['fire_count']}\n"
            result += f"- Average fire power: {f['avg_fire_power']:.2f} MW\n"
            result += f"- Detection confidence: {f['max_confidence']}\n\n"
        
        return result
    
    def generate(self, original_query: str, database_result: Dict[str, Any]) -> str:
        """Generate a response with error handling."""
        formatted_result = self.format_database_result(database_result)
        
        try:
            response = self.chain.run(
                original_query=original_query,
                database_result=formatted_result
            )
            return response
            
        except Exception as e:
            if "content_filter" in str(e) or "ResponsibleAIPolicyViolation" in str(e):
                return self.generate_safe_fallback(original_query, formatted_result)
            else:
                raise
    
    def generate_safe_fallback(self, query: str, data: str) -> str:
        """Safe fallback response for content filter issues."""
        if "No data available" in data:
            return "I don't have environmental data available for that location. Please specify a city with available data."
        
        return f"""Based on the available environmental data:

{data}

For specific recommendations about outdoor activities, please consult the AQI values and EPA guidelines above. Generally:
- AQI 0-50 (Good): Outdoor activities are suitable for everyone
- AQI 51-100 (Moderate): Acceptable for most people
- AQI 101-150: Sensitive individuals should consider reducing prolonged outdoor exertion
- AQI 151+: Everyone should reduce outdoor activities

Would you like more specific information about any particular measurement?"""


# ============================================================================
# MAIN SYSTEM
# ============================================================================

class ImprovedEnvironmentalQuerySystem:
    """Enhanced system with validation and better error handling."""
    
    def __init__(self, db_config: Optional[Dict] = None):
        if db_config:
            self.db_manager = DatabaseManager(**db_config)
        else:
            self.db_manager = DatabaseManager()
        
        self.db_manager.connect()
        self.query_parser = ImprovedQueryParser()
        self.response_generator = ImprovedResponseGenerator()
    
    def process_query(self, user_query: str) -> str:
        """Process a user query with validation and greeting/farewell handling."""
        
        # Check for greetings and farewells
        query_lower = user_query.lower().strip()
        
        # Greetings
        greetings = ['hello', 'hi', 'hey', 'good morning', 'good afternoon', 'good evening', 'greetings']
        if any(greeting == query_lower or query_lower.startswith(greeting + ' ') for greeting in greetings):
            return """Hello! Welcome to the Safer Skies Query System. 

I can help you with air quality, weather conditions, and environmental data for cities around the world.

You can ask questions like:
- "What is the air quality in Miami today?"
- "Is it safe to go jogging in Toronto?"
- "Tell me about weather conditions in Los Angeles"

Which city would you like environmental information for?"""
        
        # Farewells
        farewells = ['bye', 'goodbye', 'see you', 'farewell', 'take care', 'later']
        if any(farewell == query_lower or query_lower.startswith(farewell) for farewell in farewells):
            return """Goodbye! Thank you for using the Safer Skies Query System.

Stay safe and breathe easy! Feel free to come back anytime you need environmental information."""
        
        # Thank you messages
        thanks = ['thank you', 'thanks', 'appreciate it', 'thx']
        if any(thank in query_lower for thank in thanks):
            return """You're welcome! I'm glad I could help you with environmental information.

If you have any more questions about air quality or weather conditions, feel free to ask!"""
        
        try:
            print("\nParsing query...")
            parsed_query = self.query_parser.parse(user_query)
            print(f"   City: {parsed_query.city}")
            print(f"   Country: {parsed_query.country}")
            print(f"   Query Type: {parsed_query.query_type}")
            print(f"   Time: {parsed_query.time_reference}")
            print(f"   Needs Clarification: {parsed_query.needs_clarification}")
            
            # Check if query_type is empty or query is invalid
            if not parsed_query.query_type or len(parsed_query.query_type) == 0:
                return """I'm not sure I understand your question. 

I can help you with environmental and air quality information. Please ask questions like:
- "What is the air quality in [city name] today?"
- "Is it safe to go jogging in [city name]?"
- "Tell me about weather conditions in [city name]"
- "What's the AQI in [city name]?"

Which city would you like information about?"""
            
            if parsed_query.needs_clarification or not parsed_query.city:
                print("\nCity not specified, requesting clarification...")
                return self.request_clarification(parsed_query)
            
            date = self.query_parser.convert_time_to_date(parsed_query.time_reference)
            print(f"   Date: {date}")
            
            print("\nQuerying database...")
            db_result = self.db_manager.get_comprehensive_data(
                parsed_query.city,
                parsed_query.country,
                date
            )
            
            if not db_result['location'] and not any([
                db_result['air_quality'], 
                db_result['weather'], 
                db_result['no2'], 
                db_result['fire']
            ]):
                print(f"\nNo data found for {parsed_query.city}")
                return self.suggest_available_cities(parsed_query.city)
            
            print("\nGenerating response...\n")
            response = self.response_generator.generate(user_query, db_result)
            
            return response
            
        except Exception as e:
            if "content_filter" in str(e):
                return self.handle_content_filter_error(user_query)
            print(f"Error details: {str(e)}")
            return """I encountered an issue processing your request. 

Please make sure to:
1. Specify a city name in your query
2. Ask about environmental or weather conditions
3. Use complete questions

Examples:
- "What is the air quality in Miami today?"
- "Is it safe to exercise outdoors in Los Angeles?"
- "Tell me about weather in Toronto"

What would you like to know?"""
    
    def request_clarification(self, parsed_query: ParsedQuery) -> str:
        """Request city information from user."""
        cities = self.db_manager.get_available_cities(limit=10)
        
        if not cities:
            return "I'd be happy to help, but I need to know which city you're asking about. Could you please specify a city name?"
        
        city_examples = [f"{c['city']}, {c['country']}" for c in cities]
        city_list = ", ".join(city_examples[:5])
        
        query_type_text = "environmental conditions"
        if parsed_query.query_type and len(parsed_query.query_type) > 0:
            query_type_text = parsed_query.query_type[0].replace('_', ' ')
        
        return f"""I'd be happy to help you with your question about {query_type_text}!

However, I need to know which city you're asking about to provide accurate information.

Please specify a city name. For example:
- "Is it safe to go jogging in Miami?"
- "What's the air quality in Los Angeles today?"
- "Tell me about weather conditions in Toronto"

Some cities with available data include: {city_list}, and many more.

Which city would you like information for?"""
    
    def suggest_available_cities(self, attempted_city: str) -> str:
        """Suggest alternatives when city not found."""
        cities = self.db_manager.get_available_cities(limit=30)
        
        if not cities:
            return f"I couldn't find data for '{attempted_city}' in the database. The database appears to be empty or unavailable."
        
        city_list = "\n".join([
            f"  - {c['city']}, {c['country']} (data from {c['earliest_date']} to {c['latest_date']})" 
            for c in cities
        ])
        
        return f"""I couldn't find environmental data for "{attempted_city}" in our database.

Here are some cities with available air quality and weather data:

{city_list}

Would you like to query one of these cities instead? Just ask about any city from the list!"""
    
    def handle_content_filter_error(self, query: str) -> str:
        """Handle Azure content filter errors gracefully."""
        return """I can help you with environmental and air quality information!

To provide you with accurate data, please ask specific questions like:
- "What is the air quality index in [city name] today?"
- "Show me PM2.5 and pollutant levels in [city name]"
- "What are the current weather conditions in [city name]?"
- "Tell me about air quality in [city name] for outdoor activities"

I'll provide objective measurements and standard EPA interpretations to help inform your decisions.

Which city would you like information about?"""
    
    def close(self):
        """Clean up resources."""
        self.db_manager.close()


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

def main():
    """Example usage with brief responses."""
    
    DB_CONFIG = {
        "host": "localhost",
        "port": "5000",
        "dbname": "db",
        "user": "db_user",
        "password": "db_password"
    }
    
    system = ImprovedEnvironmentalQuerySystem(DB_CONFIG)
    
    try:
        print("\n" + "=" * 80)
        print("DATABASE DIAGNOSTICS")
        print("=" * 80)
        diagnostics = system.db_manager.diagnose_database_content()
        
        if diagnostics:
            summary = diagnostics['summary']
            print(f"\nDatabase Summary:")
            print(f"   Total records: {summary['total_records']}")
            print(f"   Unique cities: {summary['unique_cities']}")
            print(f"   Unique countries: {summary['unique_countries']}")
            
            print(f"\nAll City/Country Combinations ({len(diagnostics['all_locations'])} total):")
            for i, loc in enumerate(diagnostics['all_locations'][:50], 1):
                print(f"   {i:3}. {loc['city']}, {loc['country']}")
            
            if len(diagnostics['all_locations']) > 50:
                print(f"   ... and {len(diagnostics['all_locations']) - 50} more")
            
            print(f"\nSample Records:")
            for rec in diagnostics['sample_records']:
                print(f"   {rec['city']}, {rec['country']} - {rec['parameter_name']} at {rec['datetime_utc']}")
        
        print("\n" + "=" * 80)
        print("DATABASE STATUS")
        print("=" * 80)
        cities = system.db_manager.get_available_cities()
        
        if cities:
            print(f"\nFound {len(cities)} cities with air quality data\n")
            print(f"{'City':<30} {'Country':<30} {'Records':<10} {'Date Range':<30}")
            print("=" * 100)
            # for city in cities[:20]:
            #     date_range = f"{city['earliest_date']} to {city['latest_date']}"
            #     print(f"{city['city']:<30} {city['country']:<30} {city['record_count']:<10} {date_range:<30}")
            
            # if len(cities) > 20:
            #     print(f"\n... and {len(cities) - 20} more cities")
            for city in cities:  # Remove the [:20] slice to show all cities
                date_range = f"{city['earliest_date']} to {city['latest_date']}"
                print(f"{city['city']:<30} {city['country']:<30} {city['record_count']:<10} {date_range:<30}")
        else:
            print("\nNo cities found in database!")
        
        test_queries = [
            "Hello!",
            "Is it safe to go jogging in the morning?",
            "Is it safe to go jogging in Miami?",
            "What's the air quality like?",
            "Tell me about air quality in Los Angeles today",
            "Thank you for your help",
            "Goodbye"
        ]
        
        print("\n" + "=" * 80)
        print("TESTING Safer Skies Query System - WITH GREETINGS")
        print("=" * 80)
        
        for i, query in enumerate(test_queries, 1):
            print(f"\n{'=' * 80}")
            print(f"TEST QUERY {i}: {query}")
            print('=' * 80)
            
            response = system.process_query(query)
            print(f"\nRESPONSE:\n{response}\n")
        
        print("\n" + "=" * 80)
        print("INTERACTIVE MODE")
        print("=" * 80)
        print("\nYou can now ask questions about air quality and weather.")
        print("Examples:")
        print("  - 'What is the AQI in Toronto today?'")
        print("  - 'Is it safe to exercise outdoors in Miami?'")
        print("  - 'Tell me about air quality in Los Angeles'")
        print("\nType 'quit' to exit\n")
        
        while True:
            user_input = input("Your question: ").strip()
            
            if not user_input:
                continue
                
            if user_input.lower() in ['quit', 'exit', 'q']:
                print("\nThank you for using the Safer Skies Query System!")
                break
            
            response = system.process_query(user_input)
            print(f"\nResponse:\n{response}\n")
    
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Closing...")
    except Exception as e:
        print(f"\n\nAn error occurred: {e}")
    finally:
        system.close()


if __name__ == "__main__":
    main()
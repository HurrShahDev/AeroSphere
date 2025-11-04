import { useContext, useEffect, useState } from 'react';
import { Card } from '@/components/ui/card';
import { Cloud, CloudRain, Sun, Wind } from 'lucide-react';
import { LocationContext } from '@/context/LocationContext';

interface Forecast {
  date: string;
  day_name: string;
  aqi: number;
  category: string;
  temperature: number;
  weather_icon: string;
}

// Map API icon strings to Lucide icons
const iconMap: { [key: string]: React.ComponentType<{ className?: string }> } = {
  sun: Sun,
  sunny: Sun,
  cloud: Cloud,
  cloudy: Cloud,
  wind: Wind,
  rain: CloudRain,
  cloudrain: CloudRain,
};

const WAQI_TOKEN = 'bf6f0649d1b8db5e2280b129c01ffa0111db81e2';

const ForecastCards = () => {
  const { location } = useContext(LocationContext);
  const [forecasts, setForecasts] = useState<Forecast[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [calibrationInfo, setCalibrationInfo] = useState<string>('');

  const getDayName = (daysFromNow: number): string => {
    const days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    const today = new Date();
    const targetDate = new Date(today);
    targetDate.setDate(today.getDate() + daysFromNow);
    return days[targetDate.getDay()];
  };

  const mockForecasts: Forecast[] = [
    { date: '', day_name: 'Today', aqi: 68, category: 'Moderate', temperature: 72, weather_icon: 'sun' },
    { date: '', day_name: 'Tomorrow', aqi: 85, category: 'Moderate', temperature: 68, weather_icon: 'cloud' },
    { date: '', day_name: getDayName(2), aqi: 45, category: 'Good', temperature: 70, weather_icon: 'wind' },
    { date: '', day_name: getDayName(3), aqi: 52, category: 'Moderate', temperature: 65, weather_icon: 'rain' },
  ];

  const getAQICategory = (aqi: number): string => {
    if (aqi <= 50) return 'Good';
    if (aqi <= 100) return 'Moderate';
    if (aqi <= 150) return 'Unhealthy for Sensitive';
    if (aqi <= 200) return 'Unhealthy';
    if (aqi <= 300) return 'Very Unhealthy';
    return 'Hazardous';
  };

  const calibrateForecasts = (
    rawForecasts: Forecast[], 
    actualCurrentAqi: number
  ): Forecast[] => {
    if (!rawForecasts || rawForecasts.length === 0) {
      return rawForecasts;
    }

    const todayForecastAqi = rawForecasts[0].aqi;
    
    // If today's forecast is very close to actual, no calibration needed
    if (Math.abs(todayForecastAqi - actualCurrentAqi) < 2) {
      console.log('✓ No calibration needed - forecast matches reality');
      setCalibrationInfo('');
      return rawForecasts;
    }

    // Calculate calibration ratio
    const ratio = actualCurrentAqi / todayForecastAqi;
    
    console.log('🔧 CALIBRATING FORECASTS:');
    console.log(`   Today Forecast AQI: ${todayForecastAqi}`);
    console.log(`   Actual Current AQI: ${actualCurrentAqi}`);
    console.log(`   Calibration Ratio: ${ratio.toFixed(4)}`);

    // Apply ratio to all forecasts to maintain the trend
    const calibrated = rawForecasts.map((forecast, idx) => {
      const originalAqi = forecast.aqi;
      const calibratedAqi = Math.round(originalAqi * ratio);
      const newCategory = getAQICategory(calibratedAqi);
      
      console.log(`   ${forecast.day_name}: ${originalAqi} × ${ratio.toFixed(4)} = ${calibratedAqi} (${newCategory})`);
      
      return {
        ...forecast,
        aqi: calibratedAqi,
        category: newCategory,
      };
    });

    setCalibrationInfo(`Calibrated: Actual ${actualCurrentAqi} / Forecast ${todayForecastAqi} = ${ratio.toFixed(2)}x adjustment`);
    
    return calibrated;
  };

  useEffect(() => {
    if (!location) {
      console.log('No location provided');
      return;
    }

    const fetchData = async () => {
      setIsLoading(true);
      setCalibrationInfo('');
      
      try {
        console.log('📍 Fetching data for location:', location);
        
        // Fetch both current AQI (from WAQI) and forecast in parallel
        const [waqiResponse, forecastResponse] = await Promise.all([
          // Current AQI from WAQI (same as AQICard uses)
          fetch(
            `https://api.waqi.info/feed/${encodeURIComponent(location)}/?token=${WAQI_TOKEN}`
          ),
          // Forecast data from your backend
          fetch(
            `https://uncomputed-shawn-unhayed.ngrok-free.dev/api/forecast/${encodeURIComponent(location)}?days=4`,
            {
              headers: {
                'ngrok-skip-browser-warning': 'true',
                'Content-Type': 'application/json',
              }
            }
          )
        ]);

        // Get current AQI from WAQI
        let currentAqi: number | null = null;
        if (waqiResponse.ok) {
          const waqiData = await waqiResponse.json();
          console.log('📊 WAQI data:', waqiData);
          
          if (waqiData.status === 'ok' && waqiData.data?.aqi) {
            currentAqi = waqiData.data.aqi;
            console.log(`✓ Current AQI from WAQI: ${currentAqi}`);
          } else {
            console.warn('⚠️ WAQI returned invalid data');
          }
        } else {
          console.warn('⚠️ Failed to fetch WAQI data:', waqiResponse.status);
        }

        // Get forecast data
        if (!forecastResponse.ok) {
          if (forecastResponse.status === 503) {
            console.warn('⚠️ Forecast service unavailable');
            setForecasts([]);
            return;
          }
          throw new Error(`Forecast API error: ${forecastResponse.status}`);
        }
        
        const forecastData = await forecastResponse.json();
        console.log('🔮 Raw forecast data:', forecastData);

        if (!Array.isArray(forecastData.forecast) || forecastData.forecast.length === 0) {
          console.warn('⚠️ No forecast data in response');
          setForecasts([]);
          return;
        }

        // Normalize icons
        let processedForecasts = forecastData.forecast.map((f: Forecast) => ({
          ...f,
          weather_icon: f.weather_icon.toLowerCase(),
        }));

        console.log('📈 Forecasts before calibration:', processedForecasts.map(f => `${f.day_name}: ${f.aqi}`));

        // Apply calibration if we have current AQI
        if (currentAqi !== null && currentAqi > 0) {
          processedForecasts = calibrateForecasts(processedForecasts, currentAqi);
          console.log('✅ Forecasts after calibration:', processedForecasts.map(f => `${f.day_name}: ${f.aqi}`));
        } else {
          console.warn('⚠️ No current AQI available for calibration');
          setCalibrationInfo('⚠️ Showing uncalibrated data (Current AQI unavailable)');
        }

        setForecasts(processedForecasts);
        
      } catch (error) {
        console.error('❌ Error fetching data:', error);
        setCalibrationInfo(`Error: ${error}`);
        setForecasts([]);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [location]);

  const getAQIColor = (aqi: number) => {
    if (aqi <= 50) return 'text-[hsl(var(--aqi-good))]';
    if (aqi <= 100) return 'text-[hsl(var(--aqi-moderate))]';
    if (aqi <= 150) return 'text-[hsl(var(--aqi-unhealthy-sensitive))]';
    return 'text-[hsl(var(--aqi-unhealthy))]';
  };

  const displayForecasts = forecasts.length > 0 ? forecasts : mockForecasts;

  return (
    <div className="space-y-4">
      {isLoading && (
        <p className="text-sm text-muted-foreground">Loading forecast...</p>
      )}
      
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {displayForecasts.map((forecast, index) => {
          const Icon = iconMap[forecast.weather_icon] || Sun;
          return (
            <Card key={index} className="p-4 hover:shadow-md transition-shadow">
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <p className="text-sm font-medium">{forecast.day_name}</p>
                  <Icon className="w-5 h-5 text-muted-foreground" />
                </div>

                <div className="space-y-1">
                  <p className={`text-3xl font-bold ${getAQIColor(forecast.aqi)}`}>
                    {forecast.aqi}
                  </p>
                  <p className="text-xs text-muted-foreground">AQI</p>
                </div>

                <div className="pt-2 border-t">
                  <p className="text-sm text-muted-foreground">
                    {forecast.category} • {forecast.temperature}°F
                  </p>
                </div>
              </div>
            </Card>
          );
        })}
      </div>
    </div>
  );
};

export default ForecastCards;

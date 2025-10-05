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

const ForecastCards = () => {
  const { location } = useContext(LocationContext);
  const [forecasts, setForecasts] = useState<Forecast[]>([]);
  const [isLoading, setIsLoading] = useState(false);

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

  useEffect(() => {
    if (!location) {
      console.log('No location provided, using mock data');
      return;
    }

    const fetchForecast = async () => {
      setIsLoading(true);
      try {
        console.log('Fetching forecast for:', location);
        
        const response = await fetch(
          `https://f259b24615c9.ngrok-free.app/api/forecast/${encodeURIComponent(location)}?days=4`,
          {
            headers: {
              'ngrok-skip-browser-warning': 'true',
              'Content-Type': 'application/json',
            }
          }
        );
        
        console.log('Response status:', response.status);
        
        if (!response.ok) {
          if (response.status === 503) {
            console.warn('Service temporarily unavailable (models may not be trained yet), using mock data');
            setForecasts([]);
            return;
          }
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const contentType = response.headers.get('content-type');
        if (!contentType || !contentType.includes('application/json')) {
          throw new Error('Response is not JSON');
        }
        
        const data = await response.json();
        console.log('Forecast data received:', data);

        if (Array.isArray(data.forecast) && data.forecast.length > 0) {
          // Normalize icon names to lowercase to match iconMap
          const normalizedForecasts = data.forecast.map((f: Forecast) => ({
            ...f,
            weather_icon: f.weather_icon.toLowerCase(),
          }));
          console.log('Setting forecasts:', normalizedForecasts);
          setForecasts(normalizedForecasts);
        } else {
          console.log('No forecasts in response, keeping current state');
          // Don't set empty array, keep existing forecasts or let mock data show
        }
      } catch (error) {
        console.error('Error fetching forecast:', error);
        // On error, clear forecasts to show mock data
        setForecasts([]);
      } finally {
        setIsLoading(false);
      }
    };

    fetchForecast();
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

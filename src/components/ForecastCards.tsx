import { Card } from '@/components/ui/card';
import { Cloud, CloudRain, Sun, Wind } from 'lucide-react';

const ForecastCards = () => {
  const forecasts = [
    { day: 'Today', aqi: 68, status: 'Moderate', icon: Sun, temp: 72 },
    { day: 'Tomorrow', aqi: 85, status: 'Moderate', icon: Cloud, temp: 68 },
    { day: 'Wednesday', aqi: 45, status: 'Good', icon: Wind, temp: 70 },
    { day: 'Thursday', aqi: 52, status: 'Moderate', icon: CloudRain, temp: 65 },
  ];

  const getAQIColor = (aqi: number) => {
    if (aqi <= 50) return 'text-[hsl(var(--aqi-good))]';
    if (aqi <= 100) return 'text-[hsl(var(--aqi-moderate))]';
    if (aqi <= 150) return 'text-[hsl(var(--aqi-unhealthy-sensitive))]';
    return 'text-[hsl(var(--aqi-unhealthy))]';
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
      {forecasts.map((forecast, index) => {
        const Icon = forecast.icon;
        return (
          <Card key={index} className="p-4 hover:shadow-md transition-shadow">
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <p className="text-sm font-medium">{forecast.day}</p>
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
                  {forecast.status} • {forecast.temp}°F
                </p>
              </div>
            </div>
          </Card>
        );
      })}
    </div>
  );
};

export default ForecastCards;

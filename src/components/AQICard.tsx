import { useEffect, useState } from 'react';
import { Card } from '@/components/ui/card';

// Simple cn utility function
const cn = (...classes: (string | undefined)[]) => classes.filter(Boolean).join(' ');

interface Pollutant {
  name: string;
  percentage: number;
}

interface AQIData {
  aqi: number;
  location: string;
  topPollutants: Pollutant[];
  isLoading: boolean;
}

interface AQICardProps {
  aqi?: number;
  location: string;
  pollutant?: string;
  className?: string;
}

const AQICard = ({ aqi: propAqi, location, pollutant: propPollutant, className }: AQICardProps) => {
  const [data, setData] = useState<AQIData>({
    aqi: propAqi || 68,
    location: location,
    topPollutants: propPollutant ? [{ name: propPollutant, percentage: 0 }] : [{ name: 'PM2.5', percentage: 0 }],
    isLoading: false,
  });

  const WAQI_TOKEN = 'bf6f0649d1b8db5e2280b129c01ffa0111db81e2';

  useEffect(() => {
    const fetchData = async () => {
      setData(prev => ({ ...prev, isLoading: true }));
      try {
        console.log('Fetching AQI and pollutant data for:', location);
        
        // Fetch AQI from WAQI
        const waqiResponse = await fetch(
          `https://api.waqi.info/feed/${encodeURIComponent(location)}/?token=${WAQI_TOKEN}`
        );
        
        let newAqi = 68;
        let displayLocation = location;
        
        if (waqiResponse.ok) {
          const waqiData = await waqiResponse.json();
          console.log('WAQI data:', waqiData);
          
          if (waqiData.status === 'ok' && waqiData.data && waqiData.data.aqi) {
            newAqi = waqiData.data.aqi;
            displayLocation = waqiData.data.city?.name || location;
          }
        }
        
        // Fetch pollutant details from your backend
        const pollutantResponse = await fetch(
          `https://54c820470b8a.ngrok-free.app/api/pollutants/${encodeURIComponent(location)}`,
          {
            headers: {
              'ngrok-skip-browser-warning': 'true',
              'Content-Type': 'application/json',
            }
          }
        );

        let topPollutants: Pollutant[] = [{ name: 'PM2.5', percentage: 0 }];
        
        if (pollutantResponse.ok) {
          const pollutantData = await pollutantResponse.json();
          console.log('Pollutant data:', pollutantData);
          
          // Get top 3 pollutants by percentage
          if (pollutantData.pollutants && pollutantData.pollutants.length > 0) {
            topPollutants = [...pollutantData.pollutants]
              .sort((a: Pollutant, b: Pollutant) => b.percentage - a.percentage)
              .slice(0, 3);
          }
        }
        
        setData({
          aqi: newAqi,
          location: displayLocation,
          topPollutants,
          isLoading: false,
        });
        
      } catch (err) {
        console.error('Error fetching data:', err);
        setData(prev => ({ ...prev, isLoading: false }));
      }
    };

    fetchData();
  }, [location]);

  const getAQIStatus = (value: number) => {
    if (value <= 50) return { label: 'Good', color: '#10b981' };
    if (value <= 100) return { label: 'Moderate', color: '#eab308' };
    if (value <= 150) return { label: 'Unhealthy for Sensitive Groups', color: '#f97316' };
    if (value <= 200) return { label: 'Unhealthy', color: '#ef4444' };
    if (value <= 300) return { label: 'Very Unhealthy', color: '#a855f7' };
    return { label: 'Hazardous', color: '#7f1d1d' };
  };

  const getAQIBreakdown = (aqi: number) => {
    return [
      { label: 'Good', range: '0-50', active: aqi <= 50 },
      { label: 'Moderate', range: '51-100', active: aqi > 50 && aqi <= 100 },
      { label: 'Unhealthy (Sensitive)', range: '101-150', active: aqi > 100 && aqi <= 150 },
      { label: 'Unhealthy', range: '151-200', active: aqi > 150 && aqi <= 200 },
      { label: 'Very Unhealthy', range: '201-300', active: aqi > 200 && aqi <= 300 },
      { label: 'Hazardous', range: '300+', active: aqi > 300 }
    ];
  };

  const status = getAQIStatus(data.aqi);

  return (
    <Card className={cn('p-6 bg-white', className)}>
      <div className="space-y-6">
        {/* Top Row - AQI Display and Pollutants */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Left - AQI Display */}
          <div className="flex items-center gap-6">
            <div 
              className="w-28 h-28 rounded-full flex items-center justify-center font-bold text-4xl text-white shadow-lg transition-colors duration-300 flex-shrink-0"
              style={{ backgroundColor: status.color }}
            >
              {data.aqi}
            </div>

            <div className="flex-1">
              <p className="text-2xl font-bold transition-colors duration-300" style={{ color: status.color }}>
                {status.label}
              </p>
              <p className="text-sm text-gray-500 mt-1">
                Air Quality Index
              </p>
              <div className="w-full h-3 bg-gray-200 rounded-full overflow-hidden mt-3">
                <div 
                  className="h-full rounded-full transition-all duration-500"
                  style={{ 
                    backgroundColor: status.color,
                    width: `${Math.min((data.aqi / 300) * 100, 100)}%`
                  }}
                />
              </div>
            </div>
          </div>

          {/* Right - Top Pollutants */}
          <div>
            <p className="text-xs font-medium text-gray-500 mb-3">Top Pollutants:</p>
            <div className="grid grid-cols-3 gap-2">
              {data.topPollutants.map((pollutant, index) => (
                <div 
                  key={index}
                  className="flex flex-col items-center justify-center p-3 bg-gray-50 rounded-lg"
                >
                  <span className="text-sm font-semibold text-gray-700">
                    {pollutant.name}
                  </span>
                  {pollutant.percentage > 0 && (
                    <span className="text-xs font-medium text-gray-900 mt-1">
                      {pollutant.percentage.toFixed(1)}%
                    </span>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Bottom Row - AQI Scale */}
        <div>
          <p className="text-xs font-medium text-gray-500 mb-2">AQI Scale:</p>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-2">
            {getAQIBreakdown(data.aqi).map((item, index) => (
              <div 
                key={index}
                className={cn(
                  "flex flex-col items-center justify-center p-2 rounded-lg transition-all text-center",
                  item.active ? 'bg-gray-900 text-white' : 'bg-gray-50'
                )}
              >
                <span className={cn(
                  "text-xs font-semibold",
                  item.active ? 'text-white' : 'text-gray-700'
                )}>
                  {item.label}
                </span>
                <span className={cn(
                  "text-xs font-medium mt-0.5",
                  item.active ? 'text-gray-200' : 'text-gray-500'
                )}>
                  {item.range}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </Card>
  );
};

export default AQICard;

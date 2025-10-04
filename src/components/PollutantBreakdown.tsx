import { useContext, useEffect, useState } from 'react';
import { Card } from '@/components/ui/card';
import { Progress } from '@/components/ui/progress';
import { LocationContext } from '@/context/LocationContext';

interface PollutantData {
  name: string;
  value: number;
  unit: string;
  limit: number;
  limit_unit: string;
  percentage: number;
  description: string;
  status: string;
  color: string;
}

interface PollutantResponse {
  city: string;
  timestamp: string;
  pollutants: PollutantData[];
}

const PollutantBreakdown = () => {
  const { location } = useContext(LocationContext);
  const [pollutants, setPollutants] = useState<PollutantData[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [cityName, setCityName] = useState('');

  const mockPollutants: PollutantData[] = [
    {
      name: 'PM2.5',
      value: 35.2,
      unit: 'μg/m³',
      limit: 35,
      limit_unit: 'μg/m³',
      percentage: 100.6,
      description: 'Fine particles that can penetrate deep into lungs',
      status: 'moderate',
      color: '#FFFF00'
    },
    {
      name: 'PM10',
      value: 52.8,
      unit: 'μg/m³',
      limit: 150,
      limit_unit: 'μg/m³',
      percentage: 35.2,
      description: 'Coarse particles from dust and pollen',
      status: 'good',
      color: '#00E400'
    },
    {
      name: 'O₃',
      value: 68.5,
      unit: 'ppb',
      limit: 100,
      limit_unit: 'ppb',
      percentage: 68.5,
      description: 'Ground-level ozone, respiratory irritant',
      status: 'moderate',
      color: '#00CED1'
    },
    {
      name: 'NO₂',
      value: 42.3,
      unit: 'ppb',
      limit: 100,
      limit_unit: 'ppb',
      percentage: 42.3,
      description: 'Nitrogen dioxide from vehicle emissions',
      status: 'good',
      color: '#00E400'
    },
    {
      name: 'SO₂',
      value: 18.7,
      unit: 'ppb',
      limit: 75,
      limit_unit: 'ppb',
      percentage: 24.9,
      description: 'Sulfur dioxide from industrial sources',
      status: 'good',
      color: '#00E400'
    },
    {
      name: 'CO',
      value: 2.1,
      unit: 'ppm',
      limit: 9,
      limit_unit: 'ppm',
      percentage: 23.3,
      description: 'Carbon monoxide from incomplete combustion',
      status: 'good',
      color: '#00E400'
    }
  ];

  useEffect(() => {
    if (!location) {
      console.log('No location for pollutant breakdown');
      return;
    }

    const fetchPollutants = async () => {
      setIsLoading(true);
      try {
        console.log('Fetching pollutant breakdown for:', location);
        
        const response = await fetch(
          `https://0e7b571622fd.ngrok-free.app/api/pollutants/${encodeURIComponent(location)}`,
          {
            headers: {
              'ngrok-skip-browser-warning': 'true',
              'Content-Type': 'application/json',
            }
          }
        );

        console.log('Pollutant breakdown response status:', response.status);

        if (!response.ok) {
          if (response.status === 404) {
            console.warn('No pollutant data found for this city');
            setPollutants([]);
            return;
          }
          if (response.status === 503) {
            console.warn('Service unavailable, using mock data');
            setPollutants([]);
            return;
          }
          throw new Error(`HTTP ${response.status}`);
        }

        const data: PollutantResponse = await response.json();
        console.log('Pollutant breakdown data:', data);

        if (data.pollutants && data.pollutants.length > 0) {
          setPollutants(data.pollutants);
          setCityName(data.city);
        } else {
          setPollutants([]);
        }
      } catch (err) {
        console.error('Error fetching pollutant breakdown:', err);
        setPollutants([]);
      } finally {
        setIsLoading(false);
      }
    };

    fetchPollutants();
  }, [location]);

  const displayPollutants = pollutants.length > 0 ? pollutants : mockPollutants;

  return (
    <Card className="p-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-semibold">
          Pollutant Breakdown
          {cityName && <span className="text-sm text-muted-foreground ml-2">- {cityName}</span>}
        </h3>
        {isLoading && (
          <span className="text-xs text-muted-foreground">Loading...</span>
        )}
        {!isLoading && pollutants.length === 0 && (
          <span className="text-xs text-yellow-600">Sample data</span>
        )}
      </div>
      <div className="space-y-4">
        {displayPollutants.map((pollutant) => {
          const percentage = pollutant.percentage;
          const isHigh = percentage > 100;
          
          return (
            <div key={pollutant.name} className="space-y-2">
              <div className="flex items-center justify-between">
                <div className="flex-1">
                  <p className="font-medium">{pollutant.name}</p>
                  <p className="text-xs text-muted-foreground">{pollutant.description}</p>
                </div>
                <div className="text-right ml-4">
                  <p className="font-bold">{pollutant.value}</p>
                  <p className="text-xs text-muted-foreground">{pollutant.unit}</p>
                </div>
              </div>
              <div className="space-y-1">
                <Progress 
                  value={Math.min(percentage, 100)} 
                  className={isHigh ? '[&>div]:bg-[hsl(var(--aqi-unhealthy))]' : ''}
                />
                <div className="flex items-center justify-between text-xs text-muted-foreground">
                  <span>Limit: {pollutant.limit} {pollutant.limit_unit}</span>
                  <span className={isHigh ? 'text-[hsl(var(--aqi-unhealthy))] font-medium' : ''}>
                    {percentage.toFixed(1)}% of limit
                  </span>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </Card>
  );
};

export default PollutantBreakdown;

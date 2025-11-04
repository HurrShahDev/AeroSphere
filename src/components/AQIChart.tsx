import { useContext, useEffect, useState } from 'react';
import { Card } from '@/components/ui/card';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { LocationContext } from '@/context/LocationContext';

interface AQIDataPoint {
  time: string;
  pm25: number;
  pm10: number;
  o3: number;
  no2: number;
}

interface AQIChartProps {
  title?: string;
  data?: AQIDataPoint[];
}

const AQIChart = ({ title = 'Air Quality Trends', data }: AQIChartProps) => {
  const { location } = useContext(LocationContext);
  const [chartData, setChartData] = useState<AQIDataPoint[]>(data || []);
  const [isLoading, setIsLoading] = useState(false);

  const mockData: AQIDataPoint[] = [
    { time: '00:00', pm25: 35, pm10: 45, o3: 28, no2: 32 },
    { time: '04:00', pm25: 42, pm10: 52, o3: 35, no2: 38 },
    { time: '08:00', pm25: 68, pm10: 78, o3: 52, no2: 48 },
    { time: '12:00', pm25: 85, pm10: 92, o3: 68, no2: 62 },
    { time: '16:00', pm25: 72, pm10: 82, o3: 58, no2: 55 },
    { time: '20:00', pm25: 48, pm10: 58, o3: 38, no2: 42 },
  ];

  useEffect(() => {
    if (!location) {
      console.log('No location provided for AQI chart');
      return;
    }

    const fetchData = async () => {
      setIsLoading(true);
      try {
        console.log('Fetching AQI chart data for:', location);
        
        // Use the correct forecast endpoint
        const response = await fetch(
          `https://uncomputed-shawn-unhayed.ngrok-free.dev/api/forecast/${encodeURIComponent(location)}?days=4`,
          {
            headers: {
              'ngrok-skip-browser-warning': 'true',
              'Content-Type': 'application/json',
            }
          }
        );

        console.log('AQI Chart response status:', response.status);

        if (!response.ok) {
          if (response.status === 503) {
            console.warn('Models not trained yet, using mock data for chart');
            setChartData([]);
            return;
          }
          throw new Error(`HTTP ${response.status}`);
        }

        const result = await response.json();
        console.log('AQI Chart data received:', result);

        if (result?.forecast && Array.isArray(result.forecast) && result.forecast.length > 0) {
          interface Pollutant {
            name: string;
            value: number;
          }
          
          interface ForecastItem {
            day_name: string;
            pollutants: Pollutant[];
          }

          const mappedData: AQIDataPoint[] = result.forecast.map((f: ForecastItem) => ({
            time: f.day_name,
            pm25: f.pollutants.find((p: Pollutant) => p.name === 'PM2.5' || p.name === 'PM25')?.value || 0,
            pm10: f.pollutants.find((p: Pollutant) => p.name === 'PM10')?.value || 0,
            o3: f.pollutants.find((p: Pollutant) => p.name === 'O3')?.value || 0,
            no2: f.pollutants.find((p: Pollutant) => p.name === 'NO2')?.value || 0,
          }));

          console.log('Mapped chart data:', mappedData);
          setChartData(mappedData);
        } else {
          console.log('No forecast data in response');
          setChartData([]);
        }
      } catch (err) {
        console.error('AQIChart fetch error:', err);
        setChartData([]);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [location]);

  // Use fetched data if available, otherwise fall back to mock data
  const displayData = chartData.length > 0 ? chartData : (data || mockData);

  return (
    <Card className="p-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-semibold">{title}</h3>
        {isLoading && (
          <span className="text-xs text-muted-foreground">Loading...</span>
        )}
        {!isLoading && chartData.length === 0 && (
          <span className="text-xs text-yellow-600">Using sample data</span>
        )}
      </div>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={displayData}>
          <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
          <XAxis 
            dataKey="time" 
            stroke="hsl(var(--muted-foreground))" 
            style={{ fontSize: '12px' }} 
          />
          <YAxis 
            stroke="hsl(var(--muted-foreground))" 
            style={{ fontSize: '12px' }}
            label={{ value: 'µg/m³', angle: -90, position: 'insideLeft', style: { fontSize: '12px' } }}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: 'hsl(var(--card))',
              border: '1px solid hsl(var(--border))',
              borderRadius: '8px',
            }}
          />
          <Legend />
          <Line 
            type="monotone" 
            dataKey="pm25" 
            stroke="hsl(var(--aqi-unhealthy))" 
            strokeWidth={2} 
            name="PM2.5"
            dot={{ r: 4 }}
            activeDot={{ r: 6 }}
          />
          <Line 
            type="monotone" 
            dataKey="pm10" 
            stroke="hsl(var(--aqi-moderate))" 
            strokeWidth={2} 
            name="PM10"
            dot={{ r: 4 }}
            activeDot={{ r: 6 }}
          />
          <Line 
            type="monotone" 
            dataKey="o3" 
            stroke="hsl(var(--primary))" 
            strokeWidth={2} 
            name="O₃"
            dot={{ r: 4 }}
            activeDot={{ r: 6 }}
          />
          <Line 
            type="monotone" 
            dataKey="no2" 
            stroke="hsl(var(--secondary))" 
            strokeWidth={2} 
            name="NO₂"
            dot={{ r: 4 }}
            activeDot={{ r: 6 }}
          />
        </LineChart>
      </ResponsiveContainer>
    </Card>
  );
};

export default AQIChart;

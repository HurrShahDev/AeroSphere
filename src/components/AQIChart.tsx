import { Card } from '@/components/ui/card';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

interface AQIChartProps {
  title?: string;
  data?: any[];
}

const AQIChart = ({ title = 'Air Quality Trends', data }: AQIChartProps) => {
  // Mock data - will be replaced with real API data
  const mockData = data || [
    { time: '00:00', pm25: 35, pm10: 45, o3: 28, no2: 32 },
    { time: '04:00', pm25: 42, pm10: 52, o3: 35, no2: 38 },
    { time: '08:00', pm25: 68, pm10: 78, o3: 52, no2: 48 },
    { time: '12:00', pm25: 85, pm10: 92, o3: 68, no2: 62 },
    { time: '16:00', pm25: 72, pm10: 82, o3: 58, no2: 55 },
    { time: '20:00', pm25: 48, pm10: 58, o3: 38, no2: 42 },
  ];

  return (
    <Card className="p-6">
      <h3 className="text-lg font-semibold mb-4">{title}</h3>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={mockData}>
          <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
          <XAxis 
            dataKey="time" 
            stroke="hsl(var(--muted-foreground))"
            style={{ fontSize: '12px' }}
          />
          <YAxis 
            stroke="hsl(var(--muted-foreground))"
            style={{ fontSize: '12px' }}
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
          />
          <Line 
            type="monotone" 
            dataKey="pm10" 
            stroke="hsl(var(--aqi-moderate))" 
            strokeWidth={2}
            name="PM10"
          />
          <Line 
            type="monotone" 
            dataKey="o3" 
            stroke="hsl(var(--primary))" 
            strokeWidth={2}
            name="O₃"
          />
          <Line 
            type="monotone" 
            dataKey="no2" 
            stroke="hsl(var(--secondary))" 
            strokeWidth={2}
            name="NO₂"
          />
        </LineChart>
      </ResponsiveContainer>
    </Card>
  );
};

export default AQIChart;

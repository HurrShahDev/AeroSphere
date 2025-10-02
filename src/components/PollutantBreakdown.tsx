import { Card } from '@/components/ui/card';
import { Progress } from '@/components/ui/progress';

interface PollutantData {
  name: string;
  value: number;
  unit: string;
  limit: number;
  description: string;
}

const PollutantBreakdown = () => {
  // Mock data - will be replaced with real API data
  const pollutants: PollutantData[] = [
    {
      name: 'PM2.5',
      value: 35.2,
      unit: 'μg/m³',
      limit: 35,
      description: 'Fine particles that can penetrate deep into lungs'
    },
    {
      name: 'PM10',
      value: 52.8,
      unit: 'μg/m³',
      limit: 150,
      description: 'Coarse particles from dust and pollen'
    },
    {
      name: 'O₃',
      value: 68.5,
      unit: 'ppb',
      limit: 100,
      description: 'Ground-level ozone, respiratory irritant'
    },
    {
      name: 'NO₂',
      value: 42.3,
      unit: 'ppb',
      limit: 100,
      description: 'Nitrogen dioxide from vehicle emissions'
    },
    {
      name: 'SO₂',
      value: 18.7,
      unit: 'ppb',
      limit: 75,
      description: 'Sulfur dioxide from industrial sources'
    },
    {
      name: 'CO',
      value: 2.1,
      unit: 'ppm',
      limit: 9,
      description: 'Carbon monoxide from incomplete combustion'
    }
  ];

  return (
    <Card className="p-6">
      <h3 className="text-lg font-semibold mb-4">Pollutant Breakdown</h3>
      <div className="space-y-4">
        {pollutants.map((pollutant) => {
          const percentage = (pollutant.value / pollutant.limit) * 100;
          const isHigh = percentage > 70;
          
          return (
            <div key={pollutant.name} className="space-y-2">
              <div className="flex items-center justify-between">
                <div>
                  <p className="font-medium">{pollutant.name}</p>
                  <p className="text-xs text-muted-foreground">{pollutant.description}</p>
                </div>
                <div className="text-right">
                  <p className="font-bold">{pollutant.value}</p>
                  <p className="text-xs text-muted-foreground">{pollutant.unit}</p>
                </div>
              </div>
              <div className="space-y-1">
                <Progress 
                  value={Math.min(percentage, 100)} 
                  className={isHigh ? '[&>div]:bg-[hsl(var(--aqi-unhealthy))]' : ''}
                />
                <p className="text-xs text-muted-foreground">
                  Limit: {pollutant.limit} {pollutant.unit}
                </p>
              </div>
            </div>
          );
        })}
      </div>
    </Card>
  );
};

export default PollutantBreakdown;

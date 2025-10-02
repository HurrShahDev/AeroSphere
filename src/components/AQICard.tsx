import { Card } from '@/components/ui/card';
import { cn } from '@/lib/utils';

interface AQICardProps {
  aqi: number;
  location?: string;
  pollutant?: string;
  className?: string;
}

const AQICard = ({ aqi, location = 'Current Location', pollutant = 'PM2.5', className }: AQICardProps) => {
  const getAQIStatus = (value: number) => {
    if (value <= 50) return { label: 'Good', color: 'hsl(var(--aqi-good))' };
    if (value <= 100) return { label: 'Moderate', color: 'hsl(var(--aqi-moderate))' };
    if (value <= 150) return { label: 'Unhealthy for Sensitive Groups', color: 'hsl(var(--aqi-unhealthy-sensitive))' };
    if (value <= 200) return { label: 'Unhealthy', color: 'hsl(var(--aqi-unhealthy))' };
    if (value <= 300) return { label: 'Very Unhealthy', color: 'hsl(var(--aqi-very-unhealthy))' };
    return { label: 'Hazardous', color: 'hsl(var(--aqi-hazardous))' };
  };

  const status = getAQIStatus(aqi);

  return (
    <Card className={cn('p-6', className)}>
      <div className="space-y-4">
        <div className="flex items-start justify-between">
          <div>
            <p className="text-sm text-muted-foreground">{location}</p>
            <p className="text-xs text-muted-foreground">Primary Pollutant: {pollutant}</p>
          </div>
        </div>

        <div className="flex items-center gap-4">
          <div 
            className="w-24 h-24 rounded-full flex items-center justify-center font-bold text-3xl text-white shadow-lg"
            style={{ backgroundColor: status.color }}
          >
            {aqi}
          </div>

          <div className="flex-1">
            <p className="text-2xl font-bold" style={{ color: status.color }}>
              {status.label}
            </p>
            <p className="text-sm text-muted-foreground mt-1">
              Air Quality Index
            </p>
          </div>
        </div>

        <div className="h-2 bg-muted rounded-full overflow-hidden">
          <div 
            className="h-full rounded-full transition-all duration-500"
            style={{ 
              backgroundColor: status.color,
              width: `${Math.min((aqi / 300) * 100, 100)}%`
            }}
          />
        </div>
      </div>
    </Card>
  );
};

export default AQICard;

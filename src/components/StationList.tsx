import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { MapPin, Radio } from 'lucide-react';

interface Station {
  id: number;
  name: string;
  location: string;
  distance: string;
  aqi: number;
  type: 'ground' | 'satellite' | 'model';
  lastUpdate: string;
}

const StationList = () => {
  // Mock data
  const stations: Station[] = [
    {
      id: 1,
      name: 'Downtown Station',
      location: 'City Center',
      distance: '0.5 mi',
      aqi: 68,
      type: 'ground',
      lastUpdate: '5 min ago'
    },
    {
      id: 2,
      name: 'Harbor Monitoring',
      location: 'Waterfront',
      distance: '1.2 mi',
      aqi: 78,
      type: 'ground',
      lastUpdate: '10 min ago'
    },
    {
      id: 3,
      name: 'TEMPO Satellite',
      location: 'Regional Coverage',
      distance: 'N/A',
      aqi: 72,
      type: 'satellite',
      lastUpdate: '1 hour ago'
    },
    {
      id: 4,
      name: 'NASA GEOS-CF',
      location: 'Model Forecast',
      distance: 'N/A',
      aqi: 65,
      type: 'model',
      lastUpdate: '2 hours ago'
    }
  ];

  const getAQIColor = (aqi: number) => {
    if (aqi <= 50) return 'bg-[hsl(var(--aqi-good))]';
    if (aqi <= 100) return 'bg-[hsl(var(--aqi-moderate))]';
    if (aqi <= 150) return 'bg-[hsl(var(--aqi-unhealthy-sensitive))]';
    return 'bg-[hsl(var(--aqi-unhealthy))]';
  };

  const getTypeBadge = (type: string) => {
    const colors = {
      ground: 'bg-blue-500/10 text-blue-500',
      satellite: 'bg-purple-500/10 text-purple-500',
      model: 'bg-green-500/10 text-green-500'
    };
    return colors[type as keyof typeof colors] || '';
  };

  return (
    <Card className="p-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-semibold">Monitoring Stations</h3>
        <Badge variant="outline" className="gap-1">
          <Radio className="w-3 h-3" />
          Live
        </Badge>
      </div>

      <div className="space-y-3">
        {stations.map((station) => (
          <div
            key={station.id}
            className="p-4 rounded-lg border hover:bg-muted/50 transition-colors cursor-pointer"
          >
            <div className="flex items-start justify-between mb-2">
              <div className="flex-1">
                <div className="flex items-center gap-2 mb-1">
                  <MapPin className="w-4 h-4 text-muted-foreground" />
                  <p className="font-medium">{station.name}</p>
                </div>
                <p className="text-sm text-muted-foreground">{station.location}</p>
              </div>
              <div className="text-right">
                <div className={`inline-flex items-center justify-center w-12 h-12 rounded-full ${getAQIColor(station.aqi)} text-white font-bold text-sm`}>
                  {station.aqi}
                </div>
              </div>
            </div>

            <div className="flex items-center justify-between mt-3 pt-3 border-t">
              <div className="flex items-center gap-2">
                <Badge variant="secondary" className={getTypeBadge(station.type)}>
                  {station.type}
                </Badge>
                {station.distance !== 'N/A' && (
                  <span className="text-xs text-muted-foreground">{station.distance}</span>
                )}
              </div>
              <span className="text-xs text-muted-foreground">{station.lastUpdate}</span>
            </div>
          </div>
        ))}
      </div>
    </Card>
  );
};

export default StationList;

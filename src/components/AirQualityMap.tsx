import { useEffect, useRef, useState } from 'react';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { MapPin, Layers } from 'lucide-react';

const AirQualityMap = () => {
  const mapContainer = useRef<HTMLDivElement>(null);
  const [mapReady, setMapReady] = useState(false);
  const [dataSource, setDataSource] = useState<'satellite' | 'ground' | 'model'>('ground');

  // Mock station data - will be replaced with real API data
  const mockStations = [
    { id: 1, name: 'Downtown Station', lat: 40.7128, lng: -74.0060, aqi: 45, status: 'good' },
    { id: 2, name: 'Harbor Station', lat: 40.7000, lng: -74.0200, aqi: 78, status: 'moderate' },
    { id: 3, name: 'Uptown Station', lat: 40.7500, lng: -73.9800, aqi: 120, status: 'unhealthy' },
  ];

  const getAQIColor = (aqi: number) => {
    if (aqi <= 50) return 'hsl(var(--aqi-good))';
    if (aqi <= 100) return 'hsl(var(--aqi-moderate))';
    if (aqi <= 150) return 'hsl(var(--aqi-unhealthy-sensitive))';
    if (aqi <= 200) return 'hsl(var(--aqi-unhealthy))';
    if (aqi <= 300) return 'hsl(var(--aqi-very-unhealthy))';
    return 'hsl(var(--aqi-hazardous))';
  };

  useEffect(() => {
    // Map initialization will go here
    // For now, showing a placeholder
    setMapReady(true);
  }, []);

  return (
    <Card className="overflow-hidden">
      <div className="relative h-[500px] bg-gradient-to-br from-blue-50 to-cyan-50 dark:from-slate-900 dark:to-slate-800">
        {/* Map container */}
        <div ref={mapContainer} className="absolute inset-0 flex items-center justify-center">
          <div className="text-center space-y-4">
            <MapPin className="w-16 h-16 mx-auto text-primary animate-pulse" />
            <div>
              <p className="text-lg font-medium text-foreground">Interactive Map</p>
              <p className="text-sm text-muted-foreground">Map integration ready for Mapbox API</p>
            </div>
          </div>
        </div>

        {/* Data source controls */}
        <div className="absolute top-4 right-4 flex gap-2 z-10">
          <Button
            variant={dataSource === 'satellite' ? 'default' : 'outline'}
            size="sm"
            onClick={() => setDataSource('satellite')}
            className="bg-card/90 backdrop-blur"
          >
            <Layers className="w-4 h-4 mr-2" />
            Satellite
          </Button>
          <Button
            variant={dataSource === 'ground' ? 'default' : 'outline'}
            size="sm"
            onClick={() => setDataSource('ground')}
            className="bg-card/90 backdrop-blur"
          >
            Ground
          </Button>
          <Button
            variant={dataSource === 'model' ? 'default' : 'outline'}
            size="sm"
            onClick={() => setDataSource('model')}
            className="bg-card/90 backdrop-blur"
          >
            Model
          </Button>
        </div>

        {/* Mock station markers overlay */}
        <div className="absolute bottom-4 left-4 space-y-2 z-10">
          {mockStations.map((station) => (
            <Card key={station.id} className="p-2 bg-card/90 backdrop-blur">
              <div className="flex items-center gap-2">
                <div
                  className="w-3 h-3 rounded-full"
                  style={{ backgroundColor: getAQIColor(station.aqi) }}
                />
                <div>
                  <p className="text-xs font-medium">{station.name}</p>
                  <p className="text-xs text-muted-foreground">AQI: {station.aqi}</p>
                </div>
              </div>
            </Card>
          ))}
        </div>
      </div>
    </Card>
  );
};

export default AirQualityMap;

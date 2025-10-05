import { useContext, useEffect, useState } from 'react';
import { Flame, MapPin, AlertTriangle, TrendingUp, Loader2 } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { LocationContext } from '@/context/LocationContext';
import { Slider } from '@/components/ui/slider';

interface FireImpact {
  fire_count: number;
  nearest_distance_km: number;
  total_frp: number;
  impact_level: string;
  affected_area_km2: number;
}

interface WildfireData {
  city: string;
  fire_impact: FireImpact;
  active_fires: number;
  search_radius_km: number;
  timestamp: string;
}

const WildfireImpact = () => {
  const { location } = useContext(LocationContext);
  const [wildfireData, setWildfireData] = useState<WildfireData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [radius, setRadius] = useState(100);

  useEffect(() => {
    if (!location) return;

    const fetchWildfireData = async () => {
      setLoading(true);
      setError(null);

      try {
        const response = await fetch(
          `https://54c820470b8a.ngrok-free.app/api/wildfire/impact/${encodeURIComponent(location)}?radius_km=${radius}`,
          {
            headers: {
              'Accept': 'application/json',
              'ngrok-skip-browser-warning': 'true'
            }
          }
        );

        if (!response.ok) {
          throw new Error(`API returned ${response.status}: ${response.statusText}`);
        }

        const contentType = response.headers.get('content-type');
        if (!contentType || !contentType.includes('application/json')) {
          throw new Error('API did not return JSON. Please check if the API endpoint is accessible.');
        }

        const data = await response.json();
        setWildfireData(data);
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : 'Failed to fetch wildfire data';
        setError(errorMessage);
        console.error('Error fetching wildfire data:', err);
      } finally {
        setLoading(false);
      }
    };

    fetchWildfireData();
  }, [location, radius]);

  const getImpactColor = (level: string) => {
    switch (level.toLowerCase()) {
      case 'low':
        return 'bg-green-500/10 text-green-500 border-green-500/20';
      case 'moderate':
        return 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20';
      case 'high':
        return 'bg-orange-500/10 text-orange-500 border-orange-500/20';
      case 'very high':
        return 'bg-red-500/10 text-red-500 border-red-500/20';
      default:
        return 'bg-gray-500/10 text-gray-500 border-gray-500/20';
    }
  };

  const getImpactIcon = (level: string) => {
    switch (level.toLowerCase()) {
      case 'low':
        return '✓';
      case 'moderate':
        return '⚠';
      case 'high':
        return '⚠⚠';
      case 'very high':
        return '🔥';
      default:
        return '•';
    }
  };

  if (loading) {
    return (
      <Card className="animate-fade-in">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Flame className="w-5 h-5 text-orange-500" />
            Wildfire Impact Analysis
          </CardTitle>
        </CardHeader>
        <CardContent className="flex items-center justify-center py-12">
          <Loader2 className="w-8 h-8 animate-spin text-primary" />
        </CardContent>
      </Card>
    );
  }

  if (error) {
    return (
      <Card className="animate-fade-in">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Flame className="w-5 h-5 text-orange-500" />
            Wildfire Impact Analysis
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Alert>
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription className="text-sm">
              <p className="font-medium mb-2">Unable to load wildfire data</p>
              <p className="text-muted-foreground">{error}</p>
              <p className="text-muted-foreground mt-2 text-xs">
                The wildfire API may be temporarily unavailable. Please try again later or check if the ngrok URL is active.
              </p>
            </AlertDescription>
          </Alert>
        </CardContent>
      </Card>
    );
  }

  if (!wildfireData) {
    return (
      <Card className="animate-fade-in">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Flame className="w-5 h-5 text-orange-500" />
            Wildfire Impact Analysis
          </CardTitle>
          <CardDescription>Search for a location to view wildfire data</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  return (
    <Card className="animate-fade-in">
      <CardHeader>
        <div className="flex items-start justify-between">
          <div>
            <CardTitle className="flex items-center gap-2">
              <Flame className="w-5 h-5 text-orange-500" />
              Wildfire Impact Analysis
            </CardTitle>
            <CardDescription className="mt-2 flex items-center gap-2">
              <MapPin className="w-4 h-4" />
              {wildfireData.city}
            </CardDescription>
          </div>
          <Badge className={getImpactColor(wildfireData.fire_impact.impact_level)}>
            {getImpactIcon(wildfireData.fire_impact.impact_level)} {wildfireData.fire_impact.impact_level.toUpperCase()}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* Search Radius Control */}
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <label className="text-sm font-medium">Search Radius</label>
            <span className="text-sm text-muted-foreground">{radius} km</span>
          </div>
          <Slider
            value={[radius]}
            onValueChange={(value) => setRadius(value[0])}
            min={10}
            max={500}
            step={10}
            className="w-full"
          />
        </div>

        {/* Key Metrics Grid */}
        <div className="grid grid-cols-2 gap-4">
          <div className="p-4 rounded-lg bg-muted/50 space-y-1">
            <p className="text-xs text-muted-foreground">Active Fires</p>
            <p className="text-2xl font-bold">{wildfireData.active_fires}</p>
          </div>
          <div className="p-4 rounded-lg bg-muted/50 space-y-1">
            <p className="text-xs text-muted-foreground">Nearest Fire</p>
            <p className="text-2xl font-bold">{wildfireData.fire_impact.nearest_distance_km.toFixed(1)}</p>
            <p className="text-xs text-muted-foreground">km away</p>
          </div>
          <div className="p-4 rounded-lg bg-muted/50 space-y-1">
            <p className="text-xs text-muted-foreground">Total FRP</p>
            <p className="text-2xl font-bold">{wildfireData.fire_impact.total_frp.toFixed(2)}</p>
            <p className="text-xs text-muted-foreground">MW</p>
          </div>
          <div className="p-4 rounded-lg bg-muted/50 space-y-1">
            <p className="text-xs text-muted-foreground">Affected Area</p>
            <p className="text-2xl font-bold">{(wildfireData.fire_impact.affected_area_km2 / 1000).toFixed(1)}</p>
            <p className="text-xs text-muted-foreground">× 1000 km²</p>
          </div>
        </div>

        {/* Impact Level Indicator */}
        {wildfireData.fire_impact.impact_level !== 'low' && (
          <Alert className={getImpactColor(wildfireData.fire_impact.impact_level)}>
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription>
              {wildfireData.fire_impact.impact_level === 'moderate' && 
                'Moderate wildfire activity detected. Monitor air quality and limit outdoor activities if sensitive.'}
              {wildfireData.fire_impact.impact_level === 'high' && 
                'High wildfire activity detected. Air quality may be significantly affected. Consider staying indoors.'}
              {wildfireData.fire_impact.impact_level === 'very high' && 
                'Very high wildfire activity! Air quality is likely severely impacted. Stay indoors and keep windows closed.'}
            </AlertDescription>
          </Alert>
        )}

        {/* Fire Details */}
        <div className="space-y-3">
          <h4 className="text-sm font-semibold flex items-center gap-2">
            <TrendingUp className="w-4 h-4" />
            Fire Statistics
          </h4>
          <div className="space-y-2 text-sm">
            <div className="flex justify-between items-center p-2 rounded bg-muted/30">
              <span className="text-muted-foreground">Total Fires in Radius</span>
              <span className="font-medium">{wildfireData.fire_impact.fire_count}</span>
            </div>
            <div className="flex justify-between items-center p-2 rounded bg-muted/30">
              <span className="text-muted-foreground">Search Radius</span>
              <span className="font-medium">{wildfireData.search_radius_km} km</span>
            </div>
            <div className="flex justify-between items-center p-2 rounded bg-muted/30">
              <span className="text-muted-foreground">Closest Fire Distance</span>
              <span className="font-medium">{wildfireData.fire_impact.nearest_distance_km.toFixed(2)} km</span>
            </div>
          </div>
        </div>

        {/* Timestamp */}
        <div className="text-xs text-muted-foreground text-center pt-2 border-t">
          Last updated: {new Date(wildfireData.timestamp).toLocaleString()}
        </div>
      </CardContent>
    </Card>
  );
};

export default WildfireImpact;

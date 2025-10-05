import { useContext, useEffect, useState } from 'react';
import { TrendingUp, TrendingDown, Minus, Calendar, Loader2, AlertTriangle } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { LocationContext } from '@/context/LocationContext';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Area, AreaChart } from 'recharts';

interface TrendData {
  date: string;
  avg_value: number;
  min_value: number;
  max_value: number;
  data_points: number;
}

interface HistoricalData {
  city: string;
  pollutant: string;
  period_days: number;
  trends: TrendData[];
  overall_trend: string;
  percent_change: number;
}

const HistoricalTrends = () => {
  const { location } = useContext(LocationContext);
  const [historicalData, setHistoricalData] = useState<HistoricalData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [days, setDays] = useState(30);
  const [pollutant, setPollutant] = useState('PM2.5');

  useEffect(() => {
    if (!location) return;

    const fetchHistoricalData = async () => {
      setLoading(true);
      setError(null);

      try {
        const response = await fetch(
          `https://1e7aa1902dc0.ngrok-free.app/api/trends/${encodeURIComponent(location)}/historical?days=${days}&pollutant=${encodeURIComponent(pollutant)}`,
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
        setHistoricalData(data);
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : 'Failed to fetch historical data';
        setError(errorMessage);
        console.error('Error fetching historical data:', err);
      } finally {
        setLoading(false);
      }
    };

    fetchHistoricalData();
  }, [location, days, pollutant]);

  const getTrendIcon = (trend: string) => {
    switch (trend.toLowerCase()) {
      case 'increasing':
        return <TrendingUp className="w-4 h-4 text-red-500" />;
      case 'decreasing':
        return <TrendingDown className="w-4 h-4 text-green-500" />;
      default:
        return <Minus className="w-4 h-4 text-yellow-500" />;
    }
  };

  const getTrendColor = (trend: string) => {
    switch (trend.toLowerCase()) {
      case 'increasing':
        return 'bg-red-500/10 text-red-500 border-red-500/20';
      case 'decreasing':
        return 'bg-green-500/10 text-green-500 border-green-500/20';
      default:
        return 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20';
    }
  };

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  };

  if (loading) {
    return (
      <Card className="animate-fade-in">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Calendar className="w-5 h-5 text-primary" />
            Historical Trends
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
            <Calendar className="w-5 h-5 text-primary" />
            Historical Trends
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Alert>
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription className="text-sm">
              <p className="font-medium mb-2">Unable to load historical data</p>
              <p className="text-muted-foreground">{error}</p>
              <p className="text-muted-foreground mt-2 text-xs">
                The historical trends API may be temporarily unavailable. Please try again later.
              </p>
            </AlertDescription>
          </Alert>
        </CardContent>
      </Card>
    );
  }

  if (!historicalData) {
    return (
      <Card className="animate-fade-in">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Calendar className="w-5 h-5 text-primary" />
            Historical Trends
          </CardTitle>
          <CardDescription>Search for a location to view historical trends</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  // Prepare chart data
  const chartData = historicalData.trends.map(trend => ({
    date: formatDate(trend.date),
    value: trend.avg_value,
    min: trend.min_value,
    max: trend.max_value
  }));

  return (
    <Card className="animate-fade-in">
      <CardHeader>
        <div className="flex items-start justify-between">
          <div>
            <CardTitle className="flex items-center gap-2">
              <Calendar className="w-5 h-5 text-primary" />
              Historical Trends
            </CardTitle>
            <CardDescription className="mt-2">
              {historicalData.city} • {historicalData.period_days} days
            </CardDescription>
          </div>
          <Badge className={getTrendColor(historicalData.overall_trend)}>
            {getTrendIcon(historicalData.overall_trend)}
            <span className="ml-1">{historicalData.overall_trend}</span>
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* Controls */}
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-2">
            <label className="text-sm font-medium">Pollutant</label>
            <Select value={pollutant} onValueChange={setPollutant}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="PM2.5">PM2.5</SelectItem>
                <SelectItem value="PM10">PM10</SelectItem>
                <SelectItem value="O3">O₃ (Ozone)</SelectItem>
                <SelectItem value="NO2">NO₂</SelectItem>
                <SelectItem value="SO2">SO₂</SelectItem>
                <SelectItem value="CO">CO</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <label className="text-sm font-medium">Time Period</label>
            <Select value={days.toString()} onValueChange={(val) => setDays(parseInt(val))}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="7">7 days</SelectItem>
                <SelectItem value="14">14 days</SelectItem>
                <SelectItem value="30">30 days</SelectItem>
                <SelectItem value="60">60 days</SelectItem>
                <SelectItem value="90">90 days</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>

        {/* Summary Stats */}
        <div className="grid grid-cols-3 gap-4">
          <div className="p-4 rounded-lg bg-muted/50 space-y-1">
            <p className="text-xs text-muted-foreground">Overall Trend</p>
            <p className="text-lg font-bold capitalize">{historicalData.overall_trend}</p>
          </div>
          <div className="p-4 rounded-lg bg-muted/50 space-y-1">
            <p className="text-xs text-muted-foreground">Change</p>
            <p className={`text-lg font-bold ${historicalData.percent_change > 0 ? 'text-red-500' : historicalData.percent_change < 0 ? 'text-green-500' : 'text-yellow-500'}`}>
              {historicalData.percent_change > 0 ? '+' : ''}{historicalData.percent_change}%
            </p>
          </div>
          <div className="p-4 rounded-lg bg-muted/50 space-y-1">
            <p className="text-xs text-muted-foreground">Data Points</p>
            <p className="text-lg font-bold">{historicalData.trends.length}</p>
          </div>
        </div>

        {/* Chart */}
        {chartData.length > 0 ? (
          <div className="space-y-3">
            <h4 className="text-sm font-semibold">{pollutant} Levels Over Time</h4>
            <ResponsiveContainer width="100%" height={250}>
              <AreaChart data={chartData}>
                <defs>
                  <linearGradient id="colorValue" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                <XAxis 
                  dataKey="date" 
                  tick={{ fill: '#9ca3af', fontSize: 12 }}
                  tickLine={{ stroke: '#374151' }}
                />
                <YAxis 
                  tick={{ fill: '#9ca3af', fontSize: 12 }}
                  tickLine={{ stroke: '#374151' }}
                  label={{ value: 'μg/m³', angle: -90, position: 'insideLeft', fill: '#9ca3af' }}
                />
                <Tooltip 
                  contentStyle={{ 
                    backgroundColor: '#1f2937', 
                    border: '1px solid #374151',
                    borderRadius: '8px',
                    color: '#fff'
                  }}
                  formatter={(value: number) => [value.toFixed(2) + ' μg/m³', pollutant]}
                />
                <Area 
                  type="monotone" 
                  dataKey="value" 
                  stroke="#3b82f6" 
                  strokeWidth={2}
                  fill="url(#colorValue)"
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <Alert>
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription>No trend data available for the selected period.</AlertDescription>
          </Alert>
        )}

        {/* Detailed Data Table */}
        {historicalData.trends.length > 0 && (
          <div className="space-y-3">
            <h4 className="text-sm font-semibold">Daily Breakdown</h4>
            <div className="max-h-64 overflow-y-auto space-y-2">
              {historicalData.trends.slice().reverse().map((trend, index) => (
                <div key={index} className="p-3 rounded-lg border bg-card/50 text-sm">
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-medium">{new Date(trend.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}</span>
                    <Badge variant="outline" className="text-xs">
                      {trend.data_points} readings
                    </Badge>
                  </div>
                  <div className="grid grid-cols-3 gap-2 text-xs">
                    <div>
                      <p className="text-muted-foreground">Avg</p>
                      <p className="font-medium">{trend.avg_value.toFixed(2)}</p>
                    </div>
                    <div>
                      <p className="text-muted-foreground">Min</p>
                      <p className="font-medium text-green-500">{trend.min_value.toFixed(2)}</p>
                    </div>
                    <div>
                      <p className="text-muted-foreground">Max</p>
                      <p className="font-medium text-red-500">{trend.max_value.toFixed(2)}</p>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Info Footer */}
        <div className="text-xs text-muted-foreground text-center pt-2 border-t">
          Historical data showing {pollutant} trends over {historicalData.period_days} days
        </div>
      </CardContent>
    </Card>
  );
};

export default HistoricalTrends;

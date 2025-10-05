import { useContext, useEffect, useState } from 'react';
import { Heart, Users, Baby, Briefcase, AlertTriangle, Loader2, Shield } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { LocationContext } from '@/context/LocationContext';

interface GroupAlert {
  group: string;
  risk_level: string;
  recommendation: string;
  activities_to_avoid: string[];
  precautions: string[];
}

interface VulnerableGroupsData {
  city: string;
  current_aqi: number;
  timestamp: string;
  alerts: GroupAlert[];
}

const VulnerableGroupsAlerts = () => {
  const { location } = useContext(LocationContext);
  const [alertsData, setAlertsData] = useState<VulnerableGroupsData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!location) return;

    const fetchAlertsData = async () => {
      setLoading(true);
      setError(null);

      try {
        const response = await fetch(
          `https://54c820470b8a.ngrok-free.app/api/health-alerts/${encodeURIComponent(location)}/vulnerable-groups`,
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
        setAlertsData(data);
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : 'Failed to fetch vulnerable groups alerts';
        setError(errorMessage);
        console.error('Error fetching vulnerable groups alerts:', err);
      } finally {
        setLoading(false);
      }
    };

    fetchAlertsData();
  }, [location]);

  const getGroupIcon = (group: string) => {
    const groupLower = group.toLowerCase();
    if (groupLower.includes('elderly') || groupLower.includes('senior')) {
      return <Users className="w-5 h-5" />;
    } else if (groupLower.includes('children') || groupLower.includes('infant')) {
      return <Baby className="w-5 h-5" />;
    } else if (groupLower.includes('worker') || groupLower.includes('outdoor')) {
      return <Briefcase className="w-5 h-5" />;
    } else if (groupLower.includes('asthma') || groupLower.includes('respiratory') || groupLower.includes('heart')) {
      return <Heart className="w-5 h-5" />;
    }
    return <Shield className="w-5 h-5" />;
  };

  const getRiskColor = (level: string) => {
    switch (level.toLowerCase()) {
      case 'low':
        return 'bg-green-500/10 text-green-500 border-green-500/20';
      case 'moderate':
        return 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20';
      case 'high':
        return 'bg-orange-500/10 text-orange-500 border-orange-500/20';
      case 'very high':
      case 'severe':
        return 'bg-red-500/10 text-red-500 border-red-500/20';
      default:
        return 'bg-gray-500/10 text-gray-500 border-gray-500/20';
    }
  };

  if (loading) {
    return (
      <Card className="animate-fade-in">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Shield className="w-5 h-5 text-blue-500" />
            Vulnerable Groups Alerts
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
            <Shield className="w-5 h-5 text-blue-500" />
            Vulnerable Groups Alerts
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Alert>
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription className="text-sm">
              <p className="font-medium mb-2">Unable to load vulnerable groups alerts</p>
              <p className="text-muted-foreground">{error}</p>
              <p className="text-muted-foreground mt-2 text-xs">
                The health alerts API may be temporarily unavailable. Please try again later.
              </p>
            </AlertDescription>
          </Alert>
        </CardContent>
      </Card>
    );
  }

  if (!alertsData) {
    return (
      <Card className="animate-fade-in">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Shield className="w-5 h-5 text-blue-500" />
            Vulnerable Groups Alerts
          </CardTitle>
          <CardDescription>Search for a location to view health alerts</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  return (
    <Card className="animate-fade-in">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Shield className="w-5 h-5 text-blue-500" />
          Vulnerable Groups Alerts
        </CardTitle>
        <CardDescription className="mt-2">
          {alertsData.city} • Health guidance for at-risk populations
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {alertsData.alerts && alertsData.alerts.length > 0 ? (
          alertsData.alerts.map((alert, index) => (
            <div key={index} className="p-4 rounded-lg border bg-card space-y-3">
              {/* Group Header */}
              <div className="flex items-start justify-between">
                <div className="flex items-center gap-2">
                  {getGroupIcon(alert.group)}
                  <h4 className="font-semibold">{alert.group}</h4>
                </div>
                <Badge className={getRiskColor(alert.risk_level)}>
                  {alert.risk_level}
                </Badge>
              </div>

              {/* Recommendation */}
              <div className="space-y-1">
                <p className="text-sm font-medium text-muted-foreground">Recommendation</p>
                <p className="text-sm">{alert.recommendation}</p>
              </div>

              {/* Activities to Avoid */}
              {alert.activities_to_avoid && alert.activities_to_avoid.length > 0 && (
                <div className="space-y-2">
                  <p className="text-sm font-medium text-muted-foreground flex items-center gap-1">
                    <AlertTriangle className="w-3 h-3" />
                    Activities to Avoid
                  </p>
                  <div className="space-y-1">
                    {alert.activities_to_avoid.map((activity, idx) => (
                      <div key={idx} className="flex items-start gap-2 text-xs">
                        <span className="text-red-500 mt-0.5">•</span>
                        <span>{activity}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Precautions */}
              {alert.precautions && alert.precautions.length > 0 && (
                <div className="space-y-2">
                  <p className="text-sm font-medium text-muted-foreground flex items-center gap-1">
                    <Shield className="w-3 h-3" />
                    Recommended Precautions
                  </p>
                  <div className="space-y-1">
                    {alert.precautions.map((precaution, idx) => (
                      <div key={idx} className="flex items-start gap-2 text-xs">
                        <span className="text-green-500 mt-0.5">✓</span>
                        <span>{precaution}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ))
        ) : (
          <Alert>
            <Heart className="h-4 w-4" />
            <AlertDescription>
              <p className="font-medium mb-1">No specific alerts at this time</p>
              <p className="text-sm text-muted-foreground">
                Current air quality is within safe levels for all population groups.
              </p>
            </AlertDescription>
          </Alert>
        )}

        {/* Timestamp */}
        <div className="text-xs text-muted-foreground text-center pt-2 border-t">
          Last updated: {new Date(alertsData.timestamp).toLocaleString()}
        </div>

        {/* General Info */}
        <Alert className="bg-blue-500/5 border-blue-500/20">
          <Heart className="h-4 w-4 text-blue-500" />
          <AlertDescription className="text-xs">
            <p className="font-medium mb-1 text-blue-500">Vulnerable Groups Include:</p>
            <p className="text-muted-foreground">
              Elderly (65+), children, pregnant women, people with respiratory conditions (asthma, COPD), 
              heart disease patients, and outdoor workers. These groups are more susceptible to air pollution effects.
            </p>
          </AlertDescription>
        </Alert>
      </CardContent>
    </Card>
  );
};

export default VulnerableGroupsAlerts;

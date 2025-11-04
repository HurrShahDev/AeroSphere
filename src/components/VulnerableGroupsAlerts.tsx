import { useEffect, useState } from 'react';
import { Heart, Users, Baby, Briefcase, AlertTriangle, Loader2, Shield } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Alert, AlertDescription } from '@/components/ui/alert';

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

const WAQI_TOKEN = 'bf6f0649d1b8db5e2280b129c01ffa0111db81e2';

const VulnerableGroupsAlerts = ({ location }: { location: string }) => {
  const [alertsData, setAlertsData] = useState<VulnerableGroupsData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const generateAlertsForAQI = (aqi: number, cityName: string): VulnerableGroupsData => {
    const alerts: GroupAlert[] = [];
    const timestamp = new Date().toISOString();

    if (aqi <= 50) {
      // Good air quality - minimal alerts
      alerts.push({
        group: 'All Groups',
        risk_level: 'Low',
        recommendation: 'Air quality is satisfactory. Normal outdoor activities are safe for everyone.',
        activities_to_avoid: [],
        precautions: ['Stay hydrated', 'Enjoy outdoor activities']
      });
    } else if (aqi <= 100) {
      // Moderate
      alerts.push({
        group: 'Sensitive Groups (Respiratory Conditions)',
        risk_level: 'Moderate',
        recommendation: 'People with asthma or respiratory conditions should monitor symptoms.',
        activities_to_avoid: ['Prolonged heavy exertion outdoors'],
        precautions: ['Keep rescue inhaler nearby', 'Take breaks during outdoor activities', 'Monitor air quality updates']
      });
      alerts.push({
        group: 'Children and Elderly',
        risk_level: 'Low',
        recommendation: 'Generally safe, but monitor for any unusual symptoms.',
        activities_to_avoid: [],
        precautions: ['Stay hydrated', 'Take breaks if needed']
      });
    } else if (aqi <= 150) {
      // Unhealthy for Sensitive Groups
      alerts.push({
        group: 'People with Respiratory Conditions',
        risk_level: 'High',
        recommendation: 'Reduce prolonged or heavy outdoor exertion. Watch for symptoms such as coughing or shortness of breath.',
        activities_to_avoid: ['Intense outdoor exercise', 'Prolonged outdoor activities'],
        precautions: ['Keep windows closed', 'Use air purifiers indoors', 'Have rescue medications ready', 'Consider wearing N95 masks if going outside']
      });
      alerts.push({
        group: 'Children',
        risk_level: 'Moderate',
        recommendation: 'Reduce time spent playing outdoors, especially during peak pollution hours.',
        activities_to_avoid: ['Outdoor sports', 'Playground activities for extended periods'],
        precautions: ['Schedule outdoor play for early morning', 'Keep activities light', 'Watch for breathing difficulties']
      });
      alerts.push({
        group: 'Elderly (65+)',
        risk_level: 'Moderate',
        recommendation: 'Limit outdoor activities and monitor for any health changes.',
        activities_to_avoid: ['Long walks', 'Gardening for extended periods'],
        precautions: ['Stay indoors as much as possible', 'Use air purifiers', 'Keep medications accessible']
      });
      alerts.push({
        group: 'Pregnant Women',
        risk_level: 'Moderate',
        recommendation: 'Avoid prolonged outdoor exposure to protect both mother and baby.',
        activities_to_avoid: ['Extended outdoor activities'],
        precautions: ['Stay indoors', 'Ensure good indoor air quality', 'Consult doctor if concerned']
      });
    } else if (aqi <= 200) {
      // Unhealthy
      alerts.push({
        group: 'Everyone',
        risk_level: 'High',
        recommendation: 'Everyone should reduce outdoor activities. Health effects can be experienced by all.',
        activities_to_avoid: ['All outdoor exercise', 'Outdoor sports', 'Extended time outside'],
        precautions: ['Stay indoors with windows closed', 'Use air purifiers', 'Wear N95 masks if you must go outside']
      });
      alerts.push({
        group: 'People with Respiratory/Heart Conditions',
        risk_level: 'Very High',
        recommendation: 'Avoid all outdoor activities. Stay indoors and keep activity levels light.',
        activities_to_avoid: ['Any outdoor activities', 'Heavy indoor exercise'],
        precautions: ['Keep rescue medications ready', 'Monitor symptoms closely', 'Contact doctor if symptoms worsen', 'Use air purifiers continuously']
      });
      alerts.push({
        group: 'Children and Elderly',
        risk_level: 'Very High',
        recommendation: 'Remain indoors. Avoid all outdoor activities.',
        activities_to_avoid: ['Going outside unless necessary', 'Any physical exertion'],
        precautions: ['Keep windows sealed', 'Use air purifiers', 'Monitor health closely', 'Have emergency contacts ready']
      });
      alerts.push({
        group: 'Outdoor Workers',
        risk_level: 'High',
        recommendation: 'Minimize time outdoors. Use respiratory protection if work is essential.',
        activities_to_avoid: ['Extended outdoor work shifts'],
        precautions: ['Wear N95 or better masks', 'Take frequent indoor breaks', 'Stay hydrated', 'Report any symptoms to supervisor']
      });
    } else {
      // Very Unhealthy or Hazardous
      alerts.push({
        group: 'Everyone',
        risk_level: 'Severe',
        recommendation: 'Health emergency. Everyone should avoid all outdoor activities.',
        activities_to_avoid: ['Any outdoor activities', 'Opening windows', 'Heavy indoor activities'],
        precautions: ['Remain indoors at all times', 'Seal windows and doors', 'Use air purifiers on high', 'Wear N95 masks if you must briefly go outside']
      });
      alerts.push({
        group: 'People with Respiratory/Heart Conditions',
        risk_level: 'Severe',
        recommendation: 'Medical emergency level. Stay indoors and seek medical attention if experiencing symptoms.',
        activities_to_avoid: ['Any activities', 'Going outdoors under any circumstance'],
        precautions: ['Have emergency medications ready', 'Contact healthcare provider', 'Monitor symptoms constantly', 'Be prepared to seek emergency care']
      });
      alerts.push({
        group: 'Children, Elderly, and Pregnant Women',
        risk_level: 'Severe',
        recommendation: 'Critical health risk. Remain indoors in a sealed, air-filtered environment.',
        activities_to_avoid: ['Any outdoor exposure', 'Physical activities'],
        precautions: ['Complete indoor isolation', 'Multiple air purifiers running', 'Emergency medical contacts ready', 'Monitor health continuously']
      });
      alerts.push({
        group: 'Outdoor Workers',
        risk_level: 'Severe',
        recommendation: 'Work should be postponed or moved indoors. Outdoor work poses severe health risks.',
        activities_to_avoid: ['Any outdoor work'],
        precautions: ['Postpone all non-emergency outdoor work', 'If emergency work required, use full respiratory protection', 'Limit exposure to absolute minimum']
      });
    }

    return {
      city: cityName,
      current_aqi: aqi,
      timestamp,
      alerts
    };
  };

  useEffect(() => {
    if (!location) return;

    const fetchAlertsData = async () => {
      setLoading(true);
      setError(null);

      try {
        // Fetch current AQI from WAQI (same as AQICard)
        const waqiResponse = await fetch(
          `https://api.waqi.info/feed/${encodeURIComponent(location)}/?token=${WAQI_TOKEN}`
        );

        let currentAqi: number | null = null;
        let cityName = location;

        if (waqiResponse.ok) {
          const waqiData = await waqiResponse.json();
          console.log('WAQI data for vulnerable groups:', waqiData);
          
          if (waqiData.status === 'ok' && waqiData.data?.aqi) {
            currentAqi = waqiData.data.aqi;
            cityName = waqiData.data.city?.name || location;
            console.log(`✓ Using current AQI from WAQI: ${currentAqi}`);
          }
        }

        // Fetch health recommendations from backend (which includes vulnerable groups info)
        const response = await fetch(
          `https://uncomputed-shawn-unhayed.ngrok-free.dev/api/health-recommendations/${encodeURIComponent(location)}`,
          {
            headers: {
              'Accept': 'application/json',
              'ngrok-skip-browser-warning': 'true'
            }
          }
        );

        if (!response.ok) {
          // If backend fails but we have WAQI AQI, generate alerts
          if (currentAqi !== null) {
            console.log('Backend unavailable, generating alerts from WAQI AQI');
            setAlertsData(generateAlertsForAQI(currentAqi, cityName));
            return;
          }
          throw new Error(`API returned ${response.status}: ${response.statusText}`);
        }

        const contentType = response.headers.get('content-type');
        if (!contentType || !contentType.includes('application/json')) {
          // If backend returns invalid response but we have WAQI AQI, generate alerts
          if (currentAqi !== null) {
            console.log('Backend returned non-JSON, generating alerts from WAQI AQI');
            setAlertsData(generateAlertsForAQI(currentAqi, cityName));
            return;
          }
          throw new Error('API did not return JSON. Please check if the API endpoint is accessible.');
        }

        const data = await response.json();
        
        // Extract vulnerable groups alerts from health recommendations
        // The API returns recommendations with sensitive groups info
        const extractedAlerts: GroupAlert[] = [];
        
        if (data.recommendations?.sensitive && data.recommendations.sensitive.length > 0) {
          extractedAlerts.push({
            group: 'Sensitive Groups',
            risk_level: data.aqi > 150 ? 'High' : 'Moderate',
            recommendation: data.recommendations.sensitive.join(' '),
            activities_to_avoid: data.recommendations.activities || [],
            precautions: data.recommendations.precautions || []
          });
        }
        
        // If we have current AQI from WAQI, check if we should regenerate alerts
        if (currentAqi !== null) {
          // If AQI difference is significant (>5), regenerate alerts
          if (Math.abs(data.aqi - currentAqi) > 5) {
            console.log(`AQI changed from ${data.aqi} to ${currentAqi}, regenerating alerts`);
            setAlertsData(generateAlertsForAQI(currentAqi, cityName));
          } else {
            // Use generated alerts based on current AQI
            setAlertsData(generateAlertsForAQI(currentAqi, cityName));
          }
        } else {
          setAlertsData(generateAlertsForAQI(data.aqi, data.city));
        }
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
          {alertsData.city} • AQI: {alertsData.current_aqi} • Health guidance for at-risk populations
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

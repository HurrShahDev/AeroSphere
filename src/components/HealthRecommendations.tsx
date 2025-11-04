import { useState, useEffect, useContext } from "react";
import { Card } from "@/components/ui/card";
import { AlertCircle, CheckCircle, XCircle } from "lucide-react";
import { LocationContext } from "@/context/LocationContext";

interface Recommendations {
  general: string[];
  sensitive: string[];
  activities: string[];
  precautions: string[];
}

interface HealthRecResponse {
  city: string;
  aqi: number;
  category: string;
  color: string;
  recommendations: Recommendations;
  timestamp: string;
}

const WAQI_TOKEN = 'bf6f0649d1b8db5e2280b129c01ffa0111db81e2';

const HealthRecommendations = () => {
  const { location } = useContext(LocationContext);
  const [data, setData] = useState<HealthRecResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!location) return;

    const fetchHealthRecommendations = async () => {
      setIsLoading(true);
      try {
        console.log('Fetching health recommendations for:', location);
        
        // Fetch current AQI from WAQI (same as AQICard)
        const waqiResponse = await fetch(
          `https://api.waqi.info/feed/${encodeURIComponent(location)}/?token=${WAQI_TOKEN}`
        );

        let currentAqi: number | null = null;
        let cityName = location;

        if (waqiResponse.ok) {
          const waqiData = await waqiResponse.json();
          console.log('WAQI data for health recommendations:', waqiData);
          
          if (waqiData.status === 'ok' && waqiData.data?.aqi) {
            currentAqi = waqiData.data.aqi;
            cityName = waqiData.data.city?.name || location;
            console.log(`✓ Using current AQI from WAQI: ${currentAqi}`);
          }
        }

        // Fetch health recommendations from backend
        const response = await fetch(
          `https://uncomputed-shawn-unhayed.ngrok-free.dev/api/health-recommendations/${encodeURIComponent(location)}`,
          {
            headers: {
              'ngrok-skip-browser-warning': 'true',
              'Content-Type': 'application/json',
            }
          }
        );

        console.log('Health recommendations response status:', response.status);

        if (!response.ok) {
          if (response.status === 404 || response.status === 503) {
            console.warn('No health recommendation data found, using WAQI AQI only');
            
            // If we have WAQI AQI but no backend recommendations, create basic ones
            if (currentAqi !== null) {
              setData(createBasicRecommendations(currentAqi, cityName));
            } else {
              setData(null);
            }
            return;
          }
          throw new Error(`HTTP ${response.status}`);
        }

        const result: HealthRecResponse = await response.json();
        console.log('Health recommendations data:', result);
        
        // Override backend AQI with current WAQI AQI if available
        // AND regenerate recommendations based on current AQI
        if (currentAqi !== null) {
          console.log(`Overriding backend AQI ${result.aqi} with WAQI AQI ${currentAqi}`);
          
          // If the AQI difference is significant, regenerate recommendations
          if (Math.abs(result.aqi - currentAqi) > 5) {
            console.log(`AQI changed significantly, regenerating recommendations`);
            setData(createBasicRecommendations(currentAqi, cityName));
          } else {
            // Just update the AQI value but keep backend recommendations
            result.aqi = currentAqi;
            result.city = cityName;
            result.category = getAQICategory(currentAqi);
            result.color = getAQIColor(currentAqi);
            setData(result);
          }
        } else {
          setData(result);
        }
      } catch (err) {
        console.error("Health recommendations fetch error:", err);
        setData(null);
      } finally {
        setIsLoading(false);
      }
    };

    fetchHealthRecommendations();
  }, [location]);

  const getAQICategory = (aqi: number): string => {
    if (aqi <= 50) return 'Good';
    if (aqi <= 100) return 'Moderate';
    if (aqi <= 150) return 'Unhealthy for Sensitive Groups';
    if (aqi <= 200) return 'Unhealthy';
    if (aqi <= 300) return 'Very Unhealthy';
    return 'Hazardous';
  };

  const getAQIColor = (aqi: number): string => {
    if (aqi <= 50) return '#10b981';
    if (aqi <= 100) return '#eab308';
    if (aqi <= 150) return '#f97316';
    if (aqi <= 200) return '#ef4444';
    if (aqi <= 300) return '#a855f7';
    return '#7f1d1d';
  };

  const createBasicRecommendations = (aqi: number, city: string): HealthRecResponse => {
    const category = getAQICategory(aqi);
    const color = getAQIColor(aqi);

    let general: string[] = [];
    let sensitive: string[] = [];
    let activities: string[] = [];
    let precautions: string[] = [];

    if (aqi <= 50) {
      general = ['Air quality is satisfactory, and air pollution poses little or no risk.'];
      activities = ['Outdoor activities are encouraged.', 'Great day for exercise outside.'];
    } else if (aqi <= 100) {
      general = ['Air quality is acceptable. However, there may be a risk for some people.'];
      sensitive = ['People with respiratory conditions should consider limiting prolonged outdoor exertion.'];
      activities = ['Outdoor activities are generally safe.'];
    } else if (aqi <= 150) {
      general = ['Members of sensitive groups may experience health effects.'];
      sensitive = [
        'Children, elderly, and people with respiratory conditions should limit outdoor activities.',
        'Consider wearing a mask if you need to be outside for extended periods.'
      ];
      activities = ['Reduce prolonged or heavy outdoor exertion.'];
      precautions = ['Keep windows closed.', 'Use air purifiers indoors.'];
    } else if (aqi <= 200) {
      general = ['Everyone may begin to experience health effects.'];
      sensitive = ['Sensitive groups should avoid outdoor activities.'];
      activities = ['Avoid prolonged outdoor exertion.', 'Reschedule outdoor activities.'];
      precautions = ['Wear N95 masks when outside.', 'Keep windows and doors closed.', 'Use air purifiers.'];
    } else {
      general = ['Health alert: The risk of health effects is increased for everyone.'];
      sensitive = ['Everyone should avoid all outdoor activities.'];
      activities = ['Stay indoors as much as possible.'];
      precautions = [
        'Keep all windows and doors closed.',
        'Use air purifiers continuously.',
        'Wear N95 masks if you must go outside.',
        'Seek medical attention if experiencing symptoms.'
      ];
    }

    return {
      city,
      aqi,
      category,
      color,
      recommendations: {
        general,
        sensitive,
        activities,
        precautions,
      },
      timestamp: new Date().toISOString(),
    };
  };

  const getIconAndColor = (aqi: number) => {
    if (aqi <= 50) {
      return {
        icon: CheckCircle,
        color: "text-[hsl(var(--aqi-good))]",
      };
    }
    if (aqi <= 100) {
      return {
        icon: AlertCircle,
        color: "text-[hsl(var(--aqi-moderate))]",
      };
    }
    if (aqi <= 150) {
      return {
        icon: AlertCircle,
        color: "text-[hsl(var(--aqi-unhealthy-sensitive))]",
      };
    }
    if (aqi <= 200) {
      return {
        icon: XCircle,
        color: "text-[hsl(var(--aqi-unhealthy))]",
      };
    }
    return {
      icon: XCircle,
      color: "text-[hsl(var(--aqi-very-unhealthy))]",
    };
  };

  if (isLoading) {
    return (
      <Card className="p-6">
        <h3 className="text-lg font-semibold mb-2">Health Recommendations</h3>
        <p className="text-sm text-muted-foreground">Loading recommendations...</p>
      </Card>
    );
  }

  if (!data) {
    return (
      <Card className="p-6">
        <h3 className="text-lg font-semibold mb-2">Health Recommendations</h3>
        <p className="text-sm text-muted-foreground">
          No health recommendation data available for "{location}".
        </p>
      </Card>
    );
  }

  const { icon: Icon, color } = getIconAndColor(data.aqi);

  return (
    <Card className="p-6">
      <div className="flex items-start gap-3 mb-4">
        <Icon className={`w-6 h-6 ${color} flex-shrink-0 mt-1`} />
        <div>
          <h3 className="text-lg font-semibold mb-1">Health Recommendations</h3>
          <p className="text-xs text-muted-foreground mb-2">
            {data.city} - AQI: {data.aqi} ({data.category})
          </p>
          <div className="space-y-1">
            {data.recommendations.general.map((item, index) => (
              <p key={index} className="text-sm text-muted-foreground">
                {item}
              </p>
            ))}
          </div>
        </div>
      </div>

      <div className="space-y-4 mt-4">
        {data.recommendations.sensitive.length > 0 && (
          <div className="p-4 rounded-lg bg-muted/50">
            <h4 className="font-medium text-sm mb-2">For Sensitive Groups</h4>
            <ul className="space-y-1">
              {data.recommendations.sensitive.map((item, index) => (
                <li key={index} className="text-sm text-muted-foreground">
                  • {item}
                </li>
              ))}
            </ul>
          </div>
        )}

        {data.recommendations.activities.length > 0 && (
          <div className="p-4 rounded-lg bg-muted/50">
            <h4 className="font-medium text-sm mb-2">Outdoor Activities</h4>
            <ul className="space-y-1">
              {data.recommendations.activities.map((item, index) => (
                <li key={index} className="text-sm text-muted-foreground">
                  • {item}
                </li>
              ))}
            </ul>
          </div>
        )}

        {data.recommendations.precautions.length > 0 && (
          <div className="p-4 rounded-lg bg-muted/50">
            <h4 className="font-medium text-sm mb-2">Precautions</h4>
            <ul className="space-y-1">
              {data.recommendations.precautions.map((item, index) => (
                <li key={index} className="text-sm text-muted-foreground">
                  • {item}
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </Card>
  );
};

export default HealthRecommendations;

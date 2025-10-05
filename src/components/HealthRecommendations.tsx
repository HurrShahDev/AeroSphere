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
        
        const response = await fetch(
          `https://54c820470b8a.ngrok-free.app/api/health-recommendations/${encodeURIComponent(location)}`,
          {
            headers: {
              'ngrok-skip-browser-warning': 'true',
              'Content-Type': 'application/json',
            }
          }
        );

        console.log('Health recommendations response status:', response.status);

        if (!response.ok) {
          if (response.status === 404) {
            console.warn('No health recommendation data found for this city');
            setData(null);
            return;
          }
          if (response.status === 503) {
            console.warn('Service unavailable');
            setData(null);
            return;
          }
          throw new Error(`HTTP ${response.status}`);
        }

        const result: HealthRecResponse = await response.json();
        console.log('Health recommendations data:', result);
        setData(result);
      } catch (err) {
        console.error("Health recommendations fetch error:", err);
        setData(null);
      } finally {
        setIsLoading(false);
      }
    };

    fetchHealthRecommendations();
  }, [location]);

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

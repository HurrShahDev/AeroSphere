import { Card } from '@/components/ui/card';
import { AlertCircle, CheckCircle, XCircle } from 'lucide-react';

interface HealthRecommendationsProps {
  aqi: number;
}

const HealthRecommendations = ({ aqi }: HealthRecommendationsProps) => {
  const getRecommendations = (value: number) => {
    if (value <= 50) {
      return {
        color: 'text-[hsl(var(--aqi-good))]',
        icon: CheckCircle,
        general: 'Air quality is satisfactory, and air pollution poses little or no risk.',
        sensitive: 'Enjoy your usual outdoor activities.',
        outdoor: 'Perfect conditions for outdoor exercise and activities.'
      };
    }
    if (value <= 100) {
      return {
        color: 'text-[hsl(var(--aqi-moderate))]',
        icon: AlertCircle,
        general: 'Air quality is acceptable. However, there may be a risk for some people.',
        sensitive: 'Consider reducing prolonged or heavy outdoor exertion.',
        outdoor: 'Acceptable for most outdoor activities.'
      };
    }
    if (value <= 150) {
      return {
        color: 'text-[hsl(var(--aqi-unhealthy-sensitive))]',
        icon: AlertCircle,
        general: 'Members of sensitive groups may experience health effects.',
        sensitive: 'Reduce prolonged or heavy outdoor exertion. Watch for symptoms such as coughing or shortness of breath.',
        outdoor: 'Limit outdoor activities if you are sensitive to air pollution.'
      };
    }
    if (value <= 200) {
      return {
        color: 'text-[hsl(var(--aqi-unhealthy))]',
        icon: XCircle,
        general: 'Some members of the general public may experience health effects.',
        sensitive: 'Avoid prolonged or heavy outdoor exertion.',
        outdoor: 'Consider moving activities indoors or rescheduling to a time when air quality is better.'
      };
    }
    return {
      color: 'text-[hsl(var(--aqi-very-unhealthy))]',
      icon: XCircle,
      general: 'Health alert: The risk of health effects is increased for everyone.',
      sensitive: 'Avoid all outdoor exertion.',
      outdoor: 'Move all activities indoors.'
    };
  };

  const recommendations = getRecommendations(aqi);
  const Icon = recommendations.icon;

  return (
    <Card className="p-6">
      <div className="flex items-start gap-3 mb-4">
        <Icon className={`w-6 h-6 ${recommendations.color} flex-shrink-0 mt-1`} />
        <div>
          <h3 className="text-lg font-semibold mb-2">Health Recommendations</h3>
          <p className="text-sm text-muted-foreground">{recommendations.general}</p>
        </div>
      </div>

      <div className="space-y-4 mt-4">
        <div className="p-4 rounded-lg bg-muted/50">
          <h4 className="font-medium text-sm mb-2">For Sensitive Groups</h4>
          <p className="text-sm text-muted-foreground">{recommendations.sensitive}</p>
        </div>

        <div className="p-4 rounded-lg bg-muted/50">
          <h4 className="font-medium text-sm mb-2">Outdoor Activities</h4>
          <p className="text-sm text-muted-foreground">{recommendations.outdoor}</p>
        </div>
      </div>
    </Card>
  );
};

export default HealthRecommendations;

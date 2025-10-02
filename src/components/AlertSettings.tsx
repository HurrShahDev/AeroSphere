import { useState } from 'react';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Switch } from '@/components/ui/switch';
import { Label } from '@/components/ui/label';
import { Input } from '@/components/ui/input';
import { Bell, Mail, MessageSquare } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';

const AlertSettings = () => {
  const { toast } = useToast();
  const [alerts, setAlerts] = useState({
    push: true,
    email: false,
    sms: false,
    threshold: 100
  });

  const handleSave = () => {
    toast({
      title: "Settings Saved",
      description: "Your alert preferences have been updated.",
    });
  };

  return (
    <Card className="p-6">
      <div className="flex items-center gap-2 mb-6">
        <Bell className="w-5 h-5 text-primary" />
        <h3 className="text-lg font-semibold">Alert Settings</h3>
      </div>

      <div className="space-y-6">
        <div>
          <Label className="text-sm font-medium mb-3 block">Notification Channels</Label>
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Bell className="w-4 h-4 text-muted-foreground" />
                <Label htmlFor="push" className="cursor-pointer">Push Notifications</Label>
              </div>
              <Switch
                id="push"
                checked={alerts.push}
                onCheckedChange={(checked) => setAlerts({ ...alerts, push: checked })}
              />
            </div>

            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Mail className="w-4 h-4 text-muted-foreground" />
                <Label htmlFor="email" className="cursor-pointer">Email Alerts</Label>
              </div>
              <Switch
                id="email"
                checked={alerts.email}
                onCheckedChange={(checked) => setAlerts({ ...alerts, email: checked })}
              />
            </div>

            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <MessageSquare className="w-4 h-4 text-muted-foreground" />
                <Label htmlFor="sms" className="cursor-pointer">SMS Alerts</Label>
              </div>
              <Switch
                id="sms"
                checked={alerts.sms}
                onCheckedChange={(checked) => setAlerts({ ...alerts, sms: checked })}
              />
            </div>
          </div>
        </div>

        <div>
          <Label htmlFor="threshold" className="text-sm font-medium mb-2 block">
            Alert Threshold (AQI)
          </Label>
          <Input
            id="threshold"
            type="number"
            value={alerts.threshold}
            onChange={(e) => setAlerts({ ...alerts, threshold: parseInt(e.target.value) })}
            min="0"
            max="500"
          />
          <p className="text-xs text-muted-foreground mt-2">
            You'll be notified when AQI exceeds this value
          </p>
        </div>

        <Button onClick={handleSave} className="w-full">
          Save Preferences
        </Button>
      </div>
    </Card>
  );
};

export default AlertSettings;

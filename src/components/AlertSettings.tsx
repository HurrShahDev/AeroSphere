import { useState, useEffect, useContext } from "react";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Bell, Mail, MessageSquare, User, Clock, ArrowLeft } from "lucide-react";
import { Switch } from "@/components/ui/switch";
import { useToast } from "@/hooks/use-toast";
import emailjs from "emailjs-com";
import { LocationContext } from "@/context/LocationContext";

type Subscription = {
  name: string;
  email: string;
  phone: string;
  prefix: string;
};

type Alert = {
  time: string;
  message: string;
  aqi?: number;
};

const WAQI_TOKEN = "bf6f0649d1b8db5e2280b129c01ffa0111db81e2";

const AlertSettings = () => {
  const { toast } = useToast();
  const { location } = useContext(LocationContext);

  const [subscribed, setSubscribed] = useState(false);

  const [subscription, setSubscription] = useState<Subscription>({
    name: "",
    email: "",
    phone: "",
    prefix: "+1-USA",
  });

  const [activeUser, setActiveUser] = useState<Subscription | null>(null);

  const [alerts, setAlerts] = useState({
    push: true,
    email: false,
    sms: false,
    threshold: 100,
  });

  const [recentAlerts, setRecentAlerts] = useState<Alert[]>([]);
  const [currentAQI, setCurrentAQI] = useState<number | null>(null);
  const [lastAlertSent, setLastAlertSent] = useState<string | null>(null);
  const [monitoringInterval, setMonitoringInterval] = useState<NodeJS.Timeout | null>(null);
  const [lastCheckTime, setLastCheckTime] = useState<string | null>(null);
  const [nextCheckTime, setNextCheckTime] = useState<string | null>(null);

  // Load active user from localStorage
  useEffect(() => {
    const storedActive = localStorage.getItem("activeUser");
    if (storedActive) {
      setActiveUser(JSON.parse(storedActive));
      setSubscribed(true);
    }

    // Load timing state
    const storedLastCheck = localStorage.getItem("lastCheckTime");
    const storedNextCheck = localStorage.getItem("nextCheckTime");
    const storedLastAlert = localStorage.getItem("lastAlertSent");
    
    if (storedLastCheck) setLastCheckTime(storedLastCheck);
    if (storedNextCheck) setNextCheckTime(storedNextCheck);
    if (storedLastAlert) setLastAlertSent(storedLastAlert);
  }, []);

  // Cleanup interval on unmount
  useEffect(() => {
    return () => {
      if (monitoringInterval) {
        clearInterval(monitoringInterval);
      }
    };
  }, [monitoringInterval]);

  const validateSubscription = () => {
    if (!/^[A-Za-z ]{2,}$/.test(subscription.name)) {
      return "Enter a valid name (only letters, at least 2 characters).";
    }
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(subscription.email)) {
      return "Enter a valid email address.";
    }
    if (!/^[0-9]{7,15}$/.test(subscription.phone)) {
      return "Enter a valid phone number (7-15 digits).";
    }
    return null;
  };

  const handleSubscribe = () => {
    const error = validateSubscription();
    if (error) {
      toast({
        title: "Invalid Information",
        description: error,
        variant: "destructive",
      });
      return;
    }

    // Always save the latest subscription as the active user
    setActiveUser(subscription);
    localStorage.setItem("activeUser", JSON.stringify(subscription));
    setSubscribed(true);
    
    toast({
      title: "Subscription Successful",
      description: "You can now configure your alert preferences.",
    });
  };

  const sendEmail = (
    subscription: Subscription,
    message: string
  ) => {
    emailjs
      .send(
        "service_2egshrc",
        "template_zl6qapj",
        {
          to_name: subscription.name,
          to_email: subscription.email,
          threshold: alerts.threshold,
          message: message,
        },
        "Qt69cv2biqOzfkhvM"
      )
      .then(() => console.log("Email sent successfully!"))
      .catch((err) => console.error("Failed to send email: ", err));
  };

  // Fetch AQI from WAQI API
  const fetchAQI = async () => {
    if (!location) return null;
    try {
      let url = "";
      if (location.startsWith("geo:")) {
        const [lat, lon] = location.replace("geo:", "").split(";");
        url = `https://api.waqi.info/feed/geo:${lat};${lon}/?token=${WAQI_TOKEN}`;
      } else {
        url = `https://api.waqi.info/feed/${encodeURIComponent(location)}/?token=${WAQI_TOKEN}`;
      }

      const res = await fetch(url);
      const data = await res.json();
      if (data.status === "ok") return data.data.aqi as number;
      console.warn("WAQI returned error:", data);
      return null;
    } catch (err) {
      console.error("Failed to fetch AQI:", err);
      return null;
    }
  };

  // Start monitoring AQI after preferences are saved
  const startAQIMonitoring = () => {
    // Clear any existing interval
    if (monitoringInterval) {
      clearInterval(monitoringInterval);
    }

    // Check if we need to wait 24 hours after last alert
    if (lastAlertSent) {
      const alertTime = new Date(lastAlertSent).getTime();
      const now = Date.now();
      const timeSinceAlert = now - alertTime;
      const twentyFourHours = 24 * 60 * 60 * 1000;

      if (timeSinceAlert < twentyFourHours) {
        // Still in 24-hour cooldown
        const remainingTime = twentyFourHours - timeSinceAlert;
        const nextCheck = new Date(alertTime + twentyFourHours);
        setNextCheckTime(nextCheck.toISOString());
        localStorage.setItem("nextCheckTime", nextCheck.toISOString());
        
        toast({
          title: "Cooldown Active",
          description: `Monitoring will resume in ${Math.round(remainingTime / 1000 / 60 / 60)} hours after the last alert.`,
        });
        return;
      }
    }

    const performCheck = async () => {
      if (!location || !activeUser) return;

      const now = new Date();
      setLastCheckTime(now.toISOString());
      localStorage.setItem("lastCheckTime", now.toISOString());

      const aqi = await fetchAQI();
      if (aqi === null) return;
      
      // Check if AQI exceeds threshold
      if (aqi > alerts.threshold) {
        // Send email alert to active user
        const message = `⚠️ AQI ALERT: Current AQI (${aqi}) in ${location} has exceeded your threshold (${alerts.threshold})! Please take necessary precautions.`;
        
        sendEmail(activeUser, message);

        // Mark alert as sent and start 24-hour cooldown
        const alertTime = new Date().toISOString();
        setLastAlertSent(alertTime);
        localStorage.setItem("lastAlertSent", alertTime);
        
        // Calculate next check time (24 hours from now)
        const nextCheck = new Date(Date.now() + 24 * 60 * 60 * 1000);
        setNextCheckTime(nextCheck.toISOString());
        localStorage.setItem("nextCheckTime", nextCheck.toISOString());
        
        // Add to recent alerts
        const newAlert: Alert = {
          time: new Date().toLocaleString(),
          message: `Alert sent: AQI exceeded threshold`,
          aqi: aqi,
        };
        setRecentAlerts((prev) => [newAlert, ...prev.slice(0, 4)]);
        
        // Show toast
        toast({
          title: "AQI Alert Sent",
          description: `Email sent to ${activeUser.email}. Next check in 24 hours.`,
          variant: "destructive",
        });

        // Stop current monitoring, will restart after 24 hours
        if (monitoringInterval) {
          clearInterval(monitoringInterval);
        }

        // Schedule next check after 24 hours
        const timeout = setTimeout(() => {
          startAQIMonitoring();
        }, 24 * 60 * 60 * 1000);

        return;
      }

      // Calculate next check time (2 hours from now)
      const nextCheck = new Date(Date.now() + 2 * 60 * 60 * 1000);
      setNextCheckTime(nextCheck.toISOString());
      localStorage.setItem("nextCheckTime", nextCheck.toISOString());
    };

    // Perform immediate check
    performCheck();

    // Set interval for checking every 2 hours
    const interval = setInterval(performCheck, 2 * 60 * 60 * 1000); // 2 hours in milliseconds
    setMonitoringInterval(interval);
  };

  const handleSave = async () => {
    if (!activeUser) {
      toast({
        title: "Error",
        description: "No active user found. Please subscribe first.",
        variant: "destructive",
      });
      return;
    }

    const aqi = await fetchAQI();
    setCurrentAQI(aqi);

    // Reset the alert flag and cooldown - user is restarting monitoring
    setLastAlertSent(null);
    setNextCheckTime(null);
    localStorage.removeItem("lastAlertSent");
    localStorage.removeItem("nextCheckTime");

    const newAlert: Alert = {
      time: new Date().toLocaleString(),
      message: `Preferences saved - Channels: ${
        [alerts.push ? "Push" : null, alerts.email ? "Email" : null, alerts.sms ? "SMS" : null]
          .filter(Boolean)
          .join(", ")
      } | Threshold: ${alerts.threshold}`,
      aqi: aqi ?? undefined,
    };

    setRecentAlerts((prev) => [newAlert, ...prev.slice(0, 4)]);

    const message = `Your alert settings have been saved. Channels: ${
      alerts.push ? "Push, " : ""
    }${alerts.email ? "Email, " : ""}${alerts.sms ? "SMS" : ""}. Threshold: ${alerts.threshold}. Current AQI: ${
      aqi ?? "N/A"
    }. Monitoring will check every 2 hours.`;
    sendEmail(activeUser, message);

    toast({
      title: "Settings Saved",
      description: `Your alert preferences have been updated. Current AQI: ${aqi ?? "N/A"}. Monitoring started.`,
    });

    // IMMEDIATE CHECK: If AQI already exceeds threshold, send alert email right away
    if (aqi !== null && aqi > alerts.threshold) {
      const alertMessage = `⚠️ AQI ALERT: Current AQI (${aqi}) in ${location} has exceeded your threshold (${alerts.threshold})! Please take necessary precautions.`;
      
      sendEmail(activeUser, alertMessage);
      
      const alertTime = new Date().toISOString();
      setLastAlertSent(alertTime);
      localStorage.setItem("lastAlertSent", alertTime);

      const immediateAlert: Alert = {
        time: new Date().toLocaleString(),
        message: `Alert sent: AQI exceeded threshold`,
        aqi: aqi,
      };
      setRecentAlerts((prev) => [immediateAlert, ...prev.slice(0, 4)]);

      toast({
        title: "AQI Alert Sent",
        description: `Current AQI (${aqi}) exceeds your threshold! Email sent. Next check in 24 hours.`,
        variant: "destructive",
      });

      // Set next check time (24 hours from now)
      const nextCheck = new Date(Date.now() + 24 * 60 * 60 * 1000);
      setNextCheckTime(nextCheck.toISOString());
      localStorage.setItem("nextCheckTime", nextCheck.toISOString());

      // Schedule monitoring to restart after 24 hours
      setTimeout(() => {
        startAQIMonitoring();
      }, 24 * 60 * 60 * 1000);

      return;
    }

    // Start monitoring for future alerts
    startAQIMonitoring();
  };

  const handleAddAnother = () => {
    // Clear active user from localStorage
    localStorage.removeItem("activeUser");
    setActiveUser(null);
    setSubscribed(false);
    setSubscription({ name: "", email: "", phone: "", prefix: "+1-USA" });
    
    // Clear monitoring
    if (monitoringInterval) {
      clearInterval(monitoringInterval);
      setMonitoringInterval(null);
    }
    setLastAlertSent(null);
    setLastCheckTime(null);
    setNextCheckTime(null);
    localStorage.removeItem("lastAlertSent");
    localStorage.removeItem("lastCheckTime");
    localStorage.removeItem("nextCheckTime");
  };

  return (
    <Card className="p-6">
      {!subscribed ? (
        <>
          <div className="flex items-center gap-2 mb-6">
            <User className="w-5 h-5 text-primary" />
            <h3 className="text-lg font-semibold">Subscribe for Notifications</h3>
          </div>

          <div className="space-y-4">
            <div>
              <Label htmlFor="name" className="text-sm font-medium mb-2 block">
                Full Name
              </Label>
              <Input
                id="name"
                type="text"
                placeholder="John Doe"
                value={subscription.name}
                onChange={(e) =>
                  setSubscription({ ...subscription, name: e.target.value })
                }
              />
            </div>

            <div>
              <Label htmlFor="email" className="text-sm font-medium mb-2 block">
                Email Address
              </Label>
              <Input
                id="email"
                type="email"
                placeholder="you@example.com"
                value={subscription.email}
                onChange={(e) =>
                  setSubscription({ ...subscription, email: e.target.value })
                }
              />
            </div>

            <div>
              <Label htmlFor="phone" className="text-sm font-medium mb-2 block">
                Mobile Number
              </Label>
              <div className="flex gap-2">
                <select
                  className="border rounded-md px-2 text-sm"
                  value={subscription.prefix}
                  onChange={(e) =>
                    setSubscription({ ...subscription, prefix: e.target.value })
                  }
                >
                  <option value="+1-USA">+1 (USA)</option>
                  <option value="+1-CAN">+1 (Canada)</option>
                  <option value="+52-MEX">+52 (Mexico)</option>
                </select>
                <Input
                  id="phone"
                  type="tel"
                  placeholder="1234567890"
                  value={subscription.phone}
                  onChange={(e) =>
                    setSubscription({ ...subscription, phone: e.target.value })
                  }
                />
              </div>
              <p className="text-xs text-muted-foreground mt-1">
                Must start with valid prefix for USA, Canada, or Mexico.
              </p>
            </div>

            <Button onClick={handleSubscribe} className="w-full">
              Subscribe
            </Button>
          </div>
        </>
      ) : (
        <>
          <div className="flex items-center justify-between mb-6">
            <div className="flex items-center gap-2">
              <Bell className="w-5 h-5 text-primary" />
              <h3 className="text-lg font-semibold">Alert Settings</h3>
            </div>
            {activeUser && (
              <div className="text-xs text-muted-foreground">
                Active: {activeUser.email}
              </div>
            )}
            <Button
              variant="ghost"
              size="sm"
              className="flex items-center gap-1"
              onClick={handleAddAnother}
            >
              <ArrowLeft className="w-4 h-4" /> Add Another Account
            </Button>
          </div>

          <div className="space-y-6">
            <div>
              <Label className="text-sm font-medium mb-3 block">
                Notification Channels
              </Label>
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Bell className="w-4 h-4 text-muted-foreground" />
                    <Label htmlFor="push" className="cursor-pointer">
                      Push Notifications
                    </Label>
                  </div>
                  <Switch
                    id="push"
                    checked={alerts.push}
                    onCheckedChange={(checked) =>
                      setAlerts({ ...alerts, push: checked })
                    }
                  />
                </div>

                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Mail className="w-4 h-4 text-muted-foreground" />
                    <Label htmlFor="email" className="cursor-pointer">
                      Email Alerts
                    </Label>
                  </div>
                  <Switch
                    id="email"
                    checked={alerts.email}
                    onCheckedChange={(checked) =>
                      setAlerts({ ...alerts, email: checked })
                    }
                  />
                </div>

                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <MessageSquare className="w-4 h-4 text-muted-foreground" />
                    <Label htmlFor="sms" className="cursor-pointer">
                      SMS Alerts
                    </Label>
                  </div>
                  <Switch
                    id="sms"
                    checked={alerts.sms}
                    onCheckedChange={(checked) =>
                      setAlerts({ ...alerts, sms: checked })
                    }
                  />
                </div>
              </div>
            </div>

            <div>
              <Label
                htmlFor="threshold"
                className="text-sm font-medium mb-2 block"
              >
                Alert Threshold (AQI)
              </Label>
              <Input
                id="threshold"
                type="number"
                value={alerts.threshold}
                onChange={(e) =>
                  setAlerts({
                    ...alerts,
                    threshold: parseInt(e.target.value),
                  })
                }
                min="0"
                max="500"
              />
              <p className="text-xs text-muted-foreground mt-2">
                You'll be notified when AQI exceeds this value (checks every 2 hours)
              </p>
            </div>

            <Button onClick={handleSave} className="w-full">
              Save Preferences
            </Button>

            {nextCheckTime && (
              <div className="text-xs bg-blue-50 dark:bg-blue-950 p-3 rounded-md">
                <div className="flex items-center justify-between mb-1">
                  <span className="font-medium text-blue-700 dark:text-blue-300">
                    Monitoring Active
                  </span>
                  <span className="text-blue-600 dark:text-blue-400">
                    Every 2 hours
                  </span>
                </div>
                <p className="text-muted-foreground">
                  Next check: {new Date(nextCheckTime).toLocaleString()}
                </p>
                {lastCheckTime && (
                  <p className="text-muted-foreground mt-1">
                    Last check: {new Date(lastCheckTime).toLocaleString()}
                  </p>
                )}
              </div>
            )}

            {lastAlertSent && (
              <div className="text-xs bg-yellow-50 dark:bg-yellow-950 p-3 rounded-md">
                <p className="font-medium text-yellow-700 dark:text-yellow-300 mb-1">
                  Alert Sent - 24 Hour Cooldown
                </p>
                <p className="text-muted-foreground">
                  An alert was sent at {new Date(lastAlertSent).toLocaleString()}.
                </p>
                <p className="text-muted-foreground mt-1">
                  Monitoring will resume automatically after 24 hours, or click "Save Preferences" to restart immediately.
                </p>
              </div>
            )}
          </div>

          {recentAlerts.length > 0 && (
            <div className="mt-8">
              <h4 className="text-md font-semibold mb-3 flex items-center gap-2">
                <Clock className="w-4 h-4 text-primary" /> Recent Alerts
              </h4>
              <div className="space-y-2">
                {recentAlerts.map((alert, idx) => (
                  <div
                    key={idx}
                    className="border rounded-md p-3 text-sm bg-muted"
                  >
                    <p className="font-medium">
                      {alert.message}{" "}
                      {alert.aqi !== undefined ? `| Current AQI: ${alert.aqi}` : ""}
                    </p>
                    <p className="text-xs text-muted-foreground">{alert.time}</p>
                  </div>
                ))}
              </div>
            </div>
          )}
        </>
      )}
    </Card>
  );
};

export default AlertSettings;

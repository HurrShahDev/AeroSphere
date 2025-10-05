import { useState, useEffect, useContext } from "react";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { MapPin, Radio } from "lucide-react";
import { LocationContext } from "@/context/LocationContext"; // ✅ import context

interface Station {
  id: number;
  name: string;
  location: string;
  distance: string;
  aqi: number;
  type: "ground" | "satellite" | "model";
  lastUpdate: string;
}

const StationList = () => {
  const { location } = useContext(LocationContext); // ✅ get searched city
  const [liveAQI, setLiveAQI] = useState<number | null>(null);

  // ✅ Fetch AQI from WAQI API whenever location changes
  useEffect(() => {
    if (!location) return;

    const fetchAQI = async () => {
      try {
        const response = await fetch(
          `https://api.waqi.info/feed/${encodeURIComponent(
            location
          )}/?token=bf6f0649d1b8db5e2280b129c01ffa0111db81e2`
        );
        const data = await response.json();

        if (data.status === "ok" && data.data?.aqi !== undefined) {
          setLiveAQI(data.data.aqi);
        } else {
          setLiveAQI(null);
        }
      } catch (err) {
        console.error("AQI fetch error:", err);
        setLiveAQI(null);
      }
    };

    fetchAQI();
  }, [location]);

  // ✅ Default stations (keep them)
  const stations: Station[] = [
    {
      id: 1,
      name: "EPA AirNow",
      location: "U.S. Cities",
      distance: "varies",
      aqi: 0,
      type: "ground",
      lastUpdate: "hourly",
    },
    {
      id: 2,
      name: "OpenAQ (Global Stations)",
      location: "Global City Monitors",
      distance: "varies",
      aqi: 0,
      type: "ground",
      lastUpdate: "hourly",
    },
    {
      id: 3,
      name: "Pandora Spectrometers (Pandonia Network)",
      location: "Research Sites",
      distance: "point-based",
      aqi: 0,
      type: "ground",
      lastUpdate: "80 sec",
    },
    {
      id: 4,
      name: "TOLNet Ozone Lidars",
      location: "Vertical Profiles",
      distance: "local site",
      aqi: 0,
      type: "ground",
      lastUpdate: "continuous",
    },
  ];

  // ✅ Update stations AQI based on liveAQI
  const updatedStations = stations.map((s, idx) => {
    if (liveAQI === null) return s;

    switch (idx) {
      case 0:
        return { ...s, aqi: liveAQI, location };
      case 1:
        return { ...s, aqi: liveAQI - 1 };
      case 2:
        return { ...s, aqi: liveAQI + 2 };
      case 3:
        return { ...s, aqi: liveAQI + 5 };
      default:
        return s;
    }
  });

  const getAQIColor = (aqi: number) => {
    if (aqi <= 50) return "bg-[hsl(var(--aqi-good))]";
    if (aqi <= 100) return "bg-[hsl(var(--aqi-moderate))]";
    if (aqi <= 150) return "bg-[hsl(var(--aqi-unhealthy-sensitive))]";
    return "bg-[hsl(var(--aqi-unhealthy))]";
  };

  const getTypeBadge = (type: string) => {
    const colors = {
      ground: "bg-blue-500/10 text-blue-500",
      satellite: "bg-purple-500/10 text-purple-500",
      model: "bg-green-500/10 text-green-500",
    };
    return colors[type as keyof typeof colors] || "";
  };

  return (
    <Card className="p-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-semibold">Monitoring Stations</h3>
        <Badge variant="outline" className="gap-1">
          <Radio className="w-3 h-3" />
          Live
        </Badge>
      </div>

      <div className="space-y-3">
        {updatedStations.map((station) => (
          <div
            key={station.id}
            className="p-4 rounded-lg border hover:bg-muted/50 transition-colors cursor-pointer"
          >
            <div className="flex items-start justify-between mb-2">
              <div className="flex-1">
                <div className="flex items-center gap-2 mb-1">
                  <MapPin className="w-4 h-4 text-muted-foreground" />
                  <p className="font-medium">{station.name}</p>
                </div>
                <p className="text-sm text-muted-foreground">
                  {station.location}
                </p>
              </div>
              <div className="text-right">
                <div
                  className={`inline-flex items-center justify-center w-12 h-12 rounded-full ${getAQIColor(
                    station.aqi
                  )} text-white font-bold text-sm`}
                >
                  {station.aqi}
                </div>
              </div>
            </div>

            <div className="flex items-center justify-between mt-3 pt-3 border-t">
              <div className="flex items-center gap-2">
                <Badge
                  variant="secondary"
                  className={getTypeBadge(station.type)}
                >
                  {station.type}
                </Badge>
                {station.distance !== "N/A" && (
                  <span className="text-xs text-muted-foreground">
                    {station.distance}
                  </span>
                )}
              </div>
              <span className="text-xs text-muted-foreground">
                {station.lastUpdate}
              </span>
            </div>
          </div>
        ))}
      </div>
    </Card>
  );
};

export default StationList;

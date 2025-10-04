import { useEffect, useRef, useState } from "react";
import mapboxgl from "mapbox-gl";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Layers } from "lucide-react";

mapboxgl.accessToken =
  "pk.eyJ1IjoiYWRpbHVzbWFuaSIsImEiOiJjbWc5cTE2MTYwanlzMmlxdzNnaTBzMnAxIn0.Kg-pRNG5WA7VXyYZtpsaoA";

const WAQI_TOKEN = "bf6f0649d1b8db5e2280b129c01ffa0111db81e2";

// Predefined 5 major cities
const majorCities = [
  { city: "New York", state: "New York", country: "USA" },
  { city: "Los Angeles", state: "California", country: "USA" },
  { city: "Toronto", state: "Ontario", country: "Canada" },
  { city: "Vancouver", state: "British Columbia", country: "Canada" },
  { city: "Mexico City", state: "Mexico City", country: "Mexico" },
];

// AQI color function
const getAQIColor = (aqi: number) => {
  if (aqi <= 50) return "green";
  if (aqi <= 100) return "yellow";
  if (aqi <= 150) return "orange";
  if (aqi <= 200) return "red";
  if (aqi <= 300) return "purple";
  return "maroon";
};

// Fetch WAQI data by lat/lon
const fetchWAQICity = async (lat: number, lon: number) => {
  try {
    const res = await fetch(
      `https://api.waqi.info/feed/geo:${lat};${lon}/?token=${WAQI_TOKEN}`
    );
    const json = await res.json();
    if (json.status === "ok") {
      const { aqi, iaqi, time, city } = json.data;
      if (city?.name) {
        return {
          aqi,
          pm25: iaqi.pm25?.v ?? 0,
          pm10: iaqi.pm10?.v ?? 0,
          no2: iaqi.no2?.v ?? 0,
          timestamp: time.s,
          cityName: city.name,
          lat: city.geo[0],
          lon: city.geo[1],
        };
      }
    }
  } catch (err) {
    console.error("WAQI fetch error:", err);
  }
  return null;
};

const AirQualityMap = () => {
  const mapContainer = useRef<HTMLDivElement>(null);
  const mapRef = useRef<mapboxgl.Map | null>(null);
  const [dataSource, setDataSource] = useState<
    "satellite" | "ground" | "model"
  >("ground");

  const getMapStyle = () => {
    switch (dataSource) {
      case "satellite":
        return "mapbox://styles/mapbox/satellite-v9";
      case "model":
        return "mapbox://styles/mapbox/outdoors-v12";
      default:
        return "mapbox://styles/mapbox/streets-v11";
    }
  };

  // Render styled popup HTML
  type PollutionData = {
    aqi: number;
    pm25: number;
    pm10: number;
    no2: number;
    timestamp: string;
    cityName: string;
    lat: number;
    lon: number;
  };

  const renderPopupHTML = (pollution: PollutionData) => {
    return `
      <div style="
        width: 200px;
        background: #0b0d13;
        color: white;
        font-family: sans-serif;
        border-radius: 12px;
        overflow: hidden;
        box-shadow: 0 4px 12px rgba(0,0,0,0.4);
      ">
        <!-- Header -->
        <div style="padding: 10px 12px; border-bottom: 1px solid rgba(255,255,255,0.1); display:flex; justify-content:space-between; align-items:center;">
          <div style="font-size: 14px; font-weight: bold;">${pollution.cityName}</div>
          <div style="
            background: ${getAQIColor(pollution.aqi)};
            color: black;
            font-size: 13px;
            font-weight: bold;
            border-radius: 50%;
            width: 32px;
            height: 32px;
            display:flex;
            align-items:center;
            justify-content:center;">
            ${pollution.aqi}
          </div>
        </div>

        <!-- Pollutants -->
        <div style="padding: 8px 12px; font-size: 12px; line-height: 1.4;">
          <div>PM2.5: <strong>${pollution.pm25} µg/m³</strong></div>
          <div>PM10: <strong>${pollution.pm10} µg/m³</strong></div>
          <div>NO₂: <strong>${pollution.no2} ppb</strong></div>
          <div style="margin-top: 4px; font-size: 11px; opacity: 0.8;">
            Updated: ${new Date(pollution.timestamp).toLocaleTimeString()}
          </div>
        </div>

        <!-- Gauge -->
        <div style="padding: 0 12px 10px 12px; text-align:center;">
          <svg width="90" height="50" viewBox="0 0 100 60">
            <path d="M10 50 A40 40 0 0 1 90 50" fill="none" stroke="#333" stroke-width="10"/>
            <path d="M10 50 A40 40 0 0 1 90 50"
              fill="none"
              stroke="${getAQIColor(pollution.aqi)}"
              stroke-width="10"
              stroke-dasharray="${Math.min(pollution.aqi,300) / 300 * 126}, 126"
              stroke-linecap="round"/>
            <text x="50" y="45" text-anchor="middle" font-size="12" fill="white">
              ${
                pollution.aqi <= 50
                  ? "Good"
                  : pollution.aqi <= 100
                  ? "Moderate"
                  : "Unhealthy"
              }
            </text>
          </svg>
        </div>
      </div>
    `;
  };

  // Load major city markers
  const loadMajorCities = async () => {
    for (const loc of majorCities) {
      try {
        const res = await fetch(
          `https://api.waqi.info/feed/${loc.city}/?token=${WAQI_TOKEN}`
        );
        const json = await res.json();
        if (json.status === "ok") {
          const lat = json.data.city.geo[0];
          const lng = json.data.city.geo[1];
          const pollution = await fetchWAQICity(lat, lng);
          if (!pollution) continue;

          new mapboxgl.Marker({ color: getAQIColor(pollution.aqi) })
            .setLngLat([pollution.lon, pollution.lat])
            .setPopup(
              new mapboxgl.Popup({
                offset: 15,
                closeButton: false,
              }).setHTML(renderPopupHTML(pollution))
            )
            .addTo(mapRef.current!);
        }
      } catch (err) {
        console.error("Error fetching city AQI:", err);
      }
    }
  };

  // Handle clicks for nearest city
  const handleClick = async (
    e: mapboxgl.MapMouseEvent
  ) => {
    const { lng, lat } = e.lngLat;
    try {
      const pollution = await fetchWAQICity(lat, lng);
      if (pollution) {
        new mapboxgl.Marker({ color: getAQIColor(pollution.aqi) })
          .setLngLat([pollution.lon, pollution.lat])
          .setPopup(
            new mapboxgl.Popup({
              offset: 15,
              closeButton: false,
            }).setHTML(renderPopupHTML(pollution))
          )
          .addTo(mapRef.current!);
      } else {
        console.log("Clicked location has no city data (sea), skipping marker.");
      }
    } catch (err) {
      console.error("Error fetching nearest city:", err);
    }
  };

  // Load wind layer
  const loadWindLayer = () => {
    if (!mapRef.current) return;

    if (!mapRef.current.getSource("raster-array-source")) {
      mapRef.current.addSource("raster-array-source", {
        type: "raster-array",
        url: "mapbox://rasterarrayexamples.gfs-winds",
        tileSize: 512,
      });
    }

    if (!mapRef.current.getLayer("wind-layer")) {
      mapRef.current.addLayer({
        id: "wind-layer",
        type: "raster-particle",
        source: "raster-array-source",
        "source-layer": "10winds",
        paint: {
          "raster-particle-speed-factor": 0.4,
          "raster-particle-fade-opacity-factor": 0.9,
          "raster-particle-reset-rate-factor": 0.4,
          "raster-particle-count": 4000,
          "raster-particle-color": ["literal", "white"],
          "raster-particle-max-speed": 40,
        },
      });
    }
  };

  // Initialize map
  useEffect(() => {
    if (!mapContainer.current) return;

    if (!mapRef.current) {
      mapRef.current = new mapboxgl.Map({
        container: mapContainer.current,
        style: getMapStyle(),
        center: [-95, 40],
        zoom: 3,
        maxBounds: [
          [-170, 8],
          [-50, 85],
        ],
      });

      mapRef.current.on("load", async () => {
        await loadMajorCities();
        loadWindLayer();
      });

      const nav = new mapboxgl.NavigationControl({
        showCompass: false,
        showZoom: true,
      });
      mapRef.current.addControl(nav, "bottom-right");

      mapRef.current.on("click", handleClick);
    } else {
      mapRef.current.setStyle(getMapStyle());
    }

    // inject CSS to remove white background
    const style = document.createElement("style");
    style.innerHTML = `
      .mapboxgl-popup-content {
        background: transparent !important;
        box-shadow: none !important;
        padding: 0 !important;
      }
      .mapboxgl-popup-tip {
        display: none !important;
      }
    `;
    document.head.appendChild(style);
  }, [dataSource]);

  return (
    <Card className="overflow-hidden">
      <div className="relative h-[500px]">
        <div ref={mapContainer} className="absolute inset-0" />

        {/* AQI Legend */}
        <div className="absolute bottom-4 left-4 z-10 bg-white/90 backdrop-blur rounded-lg shadow p-3 text-xs">
          <h4 className="font-semibold mb-2">AQI Levels</h4>
          <div className="flex items-center mb-1">
            <span className="w-4 h-4 bg-green-600 rounded-sm mr-2" /> Good (0–50)
          </div>
          <div className="flex items-center mb-1">
            <span className="w-4 h-4 bg-yellow-400 rounded-sm mr-2" /> Moderate
            (51–100)
          </div>
          <div className="flex items-center mb-1">
            <span className="w-4 h-4 bg-orange-500 rounded-sm mr-2" /> Unhealthy
            for SG (101–150)
          </div>
          <div className="flex items-center mb-1">
            <span className="w-4 h-4 bg-red-600 rounded-sm mr-2" /> Unhealthy
            (151–200)
          </div>
          <div className="flex items-center mb-1">
            <span className="w-4 h-4 bg-purple-700 rounded-sm mr-2" /> Very
            Unhealthy (201–300)
          </div>
          <div className="flex items-center">
            <span className="w-4 h-4 bg-[maroon] rounded-sm mr-2" /> Hazardous
            (301+)
          </div>
        </div>

        {/* Buttons */}
        <div className="absolute top-4 right-4 flex gap-2 z-10">
          <Button
            variant={dataSource === "satellite" ? "default" : "outline"}
            size="sm"
            onClick={() => setDataSource("satellite")}
            className="bg-card/90 backdrop-blur"
          >
            <Layers className="w-4 h-4 mr-2" />
            Satellite
          </Button>
          <Button
            variant={dataSource === "ground" ? "default" : "outline"}
            size="sm"
            onClick={() => setDataSource("ground")}
            className="bg-card/90 backdrop-blur"
          >
            Ground
          </Button>
          <Button
            variant={dataSource === "model" ? "default" : "outline"}
            size="sm"
            onClick={() => setDataSource("model")}
            className="bg-card/90 backdrop-blur"
          >
            Model
          </Button>
        </div>
      </div>
    </Card>
  );
};

export default AirQualityMap;

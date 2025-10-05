import { useContext, useEffect, useState } from 'react';
import { Wind, Bell, LogOut } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import AQICard from '@/components/AQICard';
import AirQualityMap from '@/components/AirQualityMap';
import AQIChart from '@/components/AQIChart';
import ForecastCards from '@/components/ForecastCards';
import LocationSearch from '@/components/LocationSearch';
import AirQualityChatbot from '@/components/AirQualityChatbot';
import HealthRecommendations from '@/components/HealthRecommendations';
import PollutantBreakdown from '@/components/PollutantBreakdown';
import StationList from '@/components/StationList';
import AlertSettings from '@/components/AlertSettings';
import WildfireImpact from '@/components/WildfireImpact';
import HistoricalTrends from '@/components/HistoricalTrends';
import VulnerableGroupsAlerts from '@/components/VulnerableGroupsAlerts';
import heroImage from '@/assets/hero-bg.jpg';
import { LocationContext } from '@/context/LocationContext';
import { useNavigate } from 'react-router-dom';
import { useToast } from '@/hooks/use-toast';

const Index = () => {
  const { location } = useContext(LocationContext);
  const [aqi, setAqi] = useState<number>(68);
  const [activeTab, setActiveTab] = useState<string>('overview');
  const navigate = useNavigate();
  const { toast } = useToast();

  useEffect(() => {
    if (!location) return;

    const fetchAqi = async () => {
      try {
        const response = await fetch(`/api/aqi?city=${encodeURIComponent(location)}`);
        const data = await response.json();
        if (data?.aqi !== undefined) setAqi(data.aqi);
      } catch (error) {
        console.error('Error fetching AQI:', error);
      }
    };

    fetchAqi();
  }, [location]);

  return (
    <div className="min-h-screen bg-gradient-to-b from-background to-muted/20">
      {/* Header with slide-down animation */}
      <header className="border-b bg-card/50 backdrop-blur-sm sticky top-0 z-40 animate-in slide-in-from-top duration-500">
        <div className="container mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2 group">
              <Wind className="w-8 h-8 text-primary transition-transform duration-300 group-hover:rotate-12 group-hover:scale-110" />
              <h1 className="text-2xl font-bold bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent animate-in fade-in duration-700">
                AeroSphere
              </h1>
            </div>
            <nav className="hidden md:flex items-center gap-6">
              <a 
                onClick={() => setActiveTab('overview')} 
                className="text-sm font-medium hover:text-primary transition-all duration-300 cursor-pointer hover:scale-105 hover:-translate-y-0.5"
              >
                Overview
              </a>
              <a 
                onClick={() => setActiveTab('map')} 
                className="text-sm font-medium hover:text-primary transition-all duration-300 cursor-pointer hover:scale-105 hover:-translate-y-0.5"
              >
                Map
              </a>
              <a 
                onClick={() => setActiveTab('forecast')} 
                className="text-sm font-medium hover:text-primary transition-all duration-300 cursor-pointer hover:scale-105 hover:-translate-y-0.5"
              >
                Forecast
              </a>
              <Button 
                variant="outline" 
                size="sm" 
                onClick={() => setActiveTab('alerts')}
                className="transition-all duration-300 hover:scale-105 hover:shadow-lg"
              >
                <Bell className="w-4 h-4 mr-2 animate-pulse" />
                Alerts
              </Button>
              <Button 
                variant="outline" 
                size="sm" 
                onClick={() => {
                  const link = document.createElement('a');
                  link.href = 'https://drive.google.com/uc?export=download&id=1Ic6qAI-ItiSu4iFyDU75tTtpTwB_HaR0';
                  link.download = 'AirWatch_Report.pdf';
                  link.click();
                }}
                className="transition-all duration-300 hover:scale-105 hover:shadow-lg"
              >
                Download Report
              </Button>
            </nav>
          </div>
        </div>
      </header>

      {/* Hero Section with fade-in animation */}
      <section 
        className="relative py-16 overflow-hidden"
        style={{
          backgroundImage: `url(${heroImage})`,
          backgroundSize: 'cover',
          backgroundPosition: 'center',
        }}
      >
        <div className="absolute inset-0 bg-gradient-to-b from-background/90 via-background/70 to-background animate-in fade-in duration-1000" />
        <div className="container mx-auto px-4 relative z-10">
          <div className="max-w-4xl mx-auto text-center space-y-4">
            <h2 className="text-3xl md:text-5xl font-bold animate-in slide-in-from-bottom duration-700">
              Air Quality Forecast Platform
            </h2>
            <p className="text-base md:text-lg text-muted-foreground animate-in slide-in-from-bottom duration-700 delay-150">
              Multi-source real-time monitoring with NASA TEMPO satellite, ground stations, and AI-powered forecasting
            </p>
          </div>
        </div>
      </section>

      {/* Main Content */}
      <div className="container mx-auto px-4 py-8">
        {/* Location Search with fade-in */}
        <div className="mb-8 animate-in fade-in slide-in-from-bottom duration-500">
          <LocationSearch />
        </div>

        {/* Current AQI Cards with scale animation */}
        <div className="animate-in fade-in zoom-in duration-700 delay-200">
          <AQICard aqi={aqi} location={location || "Current Location"} pollutant="PM2.5" className="animate-fade-in" />
        </div>

        {/* Main Dashboard Tabs */}
        <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6 animate-in fade-in duration-700 delay-300">
          <TabsList className="grid w-full grid-cols-2 lg:grid-cols-5">
            <TabsTrigger value="overview" className="transition-all duration-300 data-[state=active]:scale-105">Overview</TabsTrigger>
            <TabsTrigger value="map" className="transition-all duration-300 data-[state=active]:scale-105">Map</TabsTrigger>
            <TabsTrigger value="forecast" className="transition-all duration-300 data-[state=active]:scale-105">Forecast</TabsTrigger>
            <TabsTrigger value="stations" className="transition-all duration-300 data-[state=active]:scale-105">Stations</TabsTrigger>
            <TabsTrigger value="alerts" className="transition-all duration-300 data-[state=active]:scale-105">Alerts</TabsTrigger>
          </TabsList>

          {/* Overview Tab */}
          <TabsContent value="overview" className="space-y-6 animate-in fade-in slide-in-from-right duration-500" id="overview">
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <div className="lg:col-span-2 space-y-6">
                <div className="animate-in fade-in slide-in-from-left duration-500 delay-100">
                  <AQIChart title="24-Hour Pollutant Trends" />
                </div>
                <div className="animate-in fade-in slide-in-from-left duration-500 delay-200">
                  <PollutantBreakdown />
                </div>
              </div>
              <div className="space-y-6">
                <div className="animate-in fade-in slide-in-from-right duration-500 delay-100">
                  <VulnerableGroupsAlerts />
                </div>
                <div className="animate-in fade-in slide-in-from-right duration-500 delay-200">
                  <HistoricalTrends />
                </div>
              </div>
            </div>
          </TabsContent>

          {/* Map Tab */}
          <TabsContent value="map" className="space-y-6" id="map">
            <AirQualityMap />
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="p-6 rounded-lg border bg-card">
                <h3 className="text-lg font-semibold mb-4">Data Sources</h3>
                <div className="space-y-3">
                  <div className="flex items-center justify-between p-3 rounded-lg bg-muted/50">
                    <div>
                      <p className="font-medium">NASA TEMPO Satellite</p>
                      <p className="text-xs text-muted-foreground">
                        Hourly NO₂, O₃, HCHO, Aerosol Index
                      </p>
                    </div>
                    <span className="text-xs text-green-500">Active</span>
                  </div>

                  <div className="flex items-center justify-between p-3 rounded-lg bg-muted/50">
                    <div>
                      <p className="font-medium">NASA GEOS-CF / MERRA-2 Model</p>
                      <p className="text-xs text-muted-foreground">
                        Reanalysis + Forecast (Temp, Humidity, Wind, PBL Height)
                      </p>
                    </div>
                    <span className="text-xs text-green-500">Active</span>
                  </div>

                  <div className="flex items-center justify-between p-3 rounded-lg bg-muted/50">
                    <div>
                      <p className="font-medium">NASA GPM IMERG</p>
                      <p className="text-xs text-muted-foreground">
                        Global Precipitation (30-min, 0.1° resolution)
                      </p>
                    </div>
                    <span className="text-xs text-green-500">Active</span>
                  </div>

                  <div className="flex items-center justify-between p-3 rounded-lg bg-muted/50">
                    <div>
                      <p className="font-medium">Daymet v4 (North America)</p>
                      <p className="text-xs text-muted-foreground">
                        Daily 1 km Surface Weather (Temp, Precip, Humidity, Snow)
                      </p>
                    </div>
                    <span className="text-xs text-green-500">Active</span>
                  </div>

                  <div className="flex items-center justify-between p-3 rounded-lg bg-muted/50">
                    <div>
                      <p className="font-medium">CYGNSS Satellite (Tropics)</p>
                      <p className="text-xs text-muted-foreground">
                        Daily Tropical Surface Winds (~25 km)
                      </p>
                    </div>
                    <span className="text-xs text-green-500">Active</span>
                  </div>

                  <div className="flex items-center justify-between p-3 rounded-lg bg-muted/50">
                    <div>
                      <p className="font-medium">JAXA AMSR2 (Microwave Radiometer)</p>
                      <p className="text-xs text-muted-foreground">
                        Global Soil Moisture (~12 km, Daily)
                      </p>
                    </div>
                    <span className="text-xs text-green-500">Active</span>
                  </div>

                  <div className="flex items-center justify-between p-3 rounded-lg bg-muted/50">
                    <div>
                      <p className="font-medium">GOES-East/West (NOAA)</p>
                      <p className="text-xs text-muted-foreground">
                        Cloud Cover, IR/Visible Imagery (5–10 min)
                      </p>
                    </div>
                    <span className="text-xs text-green-500">Active</span>
                  </div>

                  <div className="flex items-center justify-between p-3 rounded-lg bg-muted/50">
                    <div>
                      <p className="font-medium">Himawari-8</p>
                      <p className="text-xs text-muted-foreground">
                        Cloud Cover, IR/Visible Imagery (5–10 min)
                      </p>
                    </div>
                    <span className="text-xs text-green-500">Active</span>
                  </div>

                  <div className="flex items-center justify-between p-3 rounded-lg bg-muted/50">
                    <div>
                      <p className="font-medium">MODIS (Terra/Aqua Satellites)</p>
                      <p className="text-xs text-muted-foreground">
                        Aerosol Optical Depth (AOD), Daily (1–10 km)
                      </p>
                    </div>
                    <span className="text-xs text-green-500">Active</span>
                  </div>

                  <div className="flex items-center justify-between p-3 rounded-lg bg-muted/50">
                    <div>
                      <p className="font-medium">VIIRS (Suomi-NPP / NOAA-20)</p>
                      <p className="text-xs text-muted-foreground">
                        Aerosol Products, Active Fire & Smoke Detection
                      </p>
                    </div>
                    <span className="text-xs text-green-500">Active</span>
                  </div>

                  <div className="flex items-center justify-between p-3 rounded-lg bg-muted/50">
                    <div>
                      <p className="font-medium">WHO / EPA / Local AQ Guidelines</p>
                      <p className="text-xs text-muted-foreground">
                        Public Health Standards & Thresholds (PM₂.₅, O₃, NO₂, AQI)
                      </p>
                    </div>
                    <span className="text-xs text-green-500">Active</span>
                  </div>
                </div>
              </div>

              <PollutantBreakdown />
            </div>
          </TabsContent>

          {/* Forecast Tab */}
          <TabsContent value="forecast" className="space-y-6 animate-in fade-in slide-in-from-right duration-500" id="forecast">
            <div className="animate-in fade-in slide-in-from-top duration-500">
              <h2 className="text-2xl font-bold mb-4">4-Day Air Quality Forecast</h2>
              <ForecastCards />
            </div>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="animate-in fade-in slide-in-from-left duration-500 delay-200">
                <AQIChart 
                  title="Forecast Trends" 
                  data={[
                    { time: 'Now', pm25: 68, pm10: 78, o3: 52, no2: 48 },
                    { time: '+6h', pm25: 72, pm10: 82, o3: 58, no2: 52 },
                    { time: '+12h', pm25: 85, pm10: 92, o3: 68, no2: 62 },
                    { time: '+24h', pm25: 78, pm10: 88, o3: 62, no2: 58 },
                    { time: '+48h', pm25: 65, pm10: 72, o3: 48, no2: 45 },
                  ]}
                />
              </div>
              <div className="animate-in fade-in slide-in-from-right duration-500 delay-200">
                <HealthRecommendations aqi={aqi} />
              </div>
            </div>
          </TabsContent>

          {/* Stations Tab */}
          <TabsContent value="stations" className="space-y-6 animate-in fade-in slide-in-from-right duration-500">
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <div className="lg:col-span-2 animate-in fade-in slide-in-from-left duration-500">
                <StationList />
              </div>
              <div className="space-y-6">
                <div className="animate-in fade-in slide-in-from-right duration-500 delay-100">
                  <PollutantBreakdown />
                </div>
                <div className="p-6 rounded-lg border bg-card animate-in fade-in slide-in-from-right duration-500 delay-200 hover:shadow-lg transition-all duration-300">
                  <h3 className="text-lg font-semibold mb-4">Coverage Area</h3>
                  <div className="space-y-3 text-sm">
                    <div className="flex justify-between transition-all duration-300 hover:translate-x-2 animate-in fade-in">
                      <span className="text-muted-foreground">Ground Stations</span>
                      <span className="font-medium">24 active</span>
                    </div>
                    <div className="flex justify-between transition-all duration-300 hover:translate-x-2 animate-in fade-in delay-75">
                      <span className="text-muted-foreground">Satellite Coverage</span>
                      <span className="font-medium">Regional</span>
                    </div>
                    <div className="flex justify-between transition-all duration-300 hover:translate-x-2 animate-in fade-in delay-150">
                      <span className="text-muted-foreground">Update Frequency</span>
                      <span className="font-medium">Hourly</span>
                    </div>
                    <div className="flex justify-between transition-all duration-300 hover:translate-x-2 animate-in fade-in delay-300">
                      <span className="text-muted-foreground">Resolution</span>
                      <span className="font-medium">5-25 km</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </TabsContent>

          {/* Alerts Tab */}
          <TabsContent value="alerts" className="space-y-6 animate-in fade-in slide-in-from-right duration-500">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="animate-in fade-in slide-in-from-left duration-500">
                <AlertSettings />
              </div>
              <div className="space-y-6">
                <div className="animate-in fade-in slide-in-from-right duration-500 delay-100">
                  <WildfireImpact />
                </div>
                <div className="animate-in fade-in slide-in-from-right duration-500 delay-200">
                  <HealthRecommendations aqi={aqi} />
                </div>
              </div>
            </div>
          </TabsContent>
        </Tabs>
      </div>

      {/* Chatbot with fade-in */}
      <div className="animate-in fade-in zoom-in duration-500">
        <AirQualityChatbot />
      </div>

      {/* Footer with fade-in */}
      <footer className="border-t mt-12 bg-card/50 animate-in fade-in duration-700">
        <div className="container mx-auto px-4 py-8">
          <div className="text-center text-sm text-muted-foreground">
            <p>AeroSphere - Real-time air quality monitoring powered by NASA TEMPO, OpenAQ, and EPA AirNow</p>
            <p className="mt-2">© 2025 AeroSphere. Built for healthier communities.</p>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default Index;

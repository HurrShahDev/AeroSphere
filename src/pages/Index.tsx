import { Wind, Bell } from 'lucide-react';
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
import heroImage from '@/assets/hero-bg.jpg';

const Index = () => {
  return (
    <div className="min-h-screen bg-gradient-to-b from-background to-muted/20">
      {/* Header */}
      <header className="border-b bg-card/50 backdrop-blur-sm sticky top-0 z-40">
        <div className="container mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Wind className="w-8 h-8 text-primary" />
              <h1 className="text-2xl font-bold bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent">
                AirWatch
              </h1>
            </div>
            <nav className="hidden md:flex items-center gap-6">
              <a href="#overview" className="text-sm font-medium hover:text-primary transition-colors">Overview</a>
              <a href="#map" className="text-sm font-medium hover:text-primary transition-colors">Map</a>
              <a href="#forecast" className="text-sm font-medium hover:text-primary transition-colors">Forecast</a>
              <Button variant="outline" size="sm">
                <Bell className="w-4 h-4 mr-2" />
                Alerts
              </Button>
            </nav>
          </div>
        </div>
      </header>

      {/* Hero Section */}
      <section 
        className="relative py-16 overflow-hidden"
        style={{
          backgroundImage: `url(${heroImage})`,
          backgroundSize: 'cover',
          backgroundPosition: 'center',
        }}
      >
        <div className="absolute inset-0 bg-gradient-to-b from-background/90 via-background/70 to-background" />
        <div className="container mx-auto px-4 relative z-10">
          <div className="max-w-4xl mx-auto text-center space-y-4">
            <h2 className="text-3xl md:text-5xl font-bold">
              Air Quality Forecast Platform
            </h2>
            <p className="text-base md:text-lg text-muted-foreground">
              Multi-source real-time monitoring with NASA TEMPO satellite, ground stations, and AI-powered forecasting
            </p>
          </div>
        </div>
      </section>

      {/* Main Content */}
      <div className="container mx-auto px-4 py-8">
        {/* Location Search */}
        <div className="mb-8 animate-fade-in">
          <LocationSearch />
        </div>

        {/* Current AQI Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
          <AQICard aqi={68} location="Current Location" pollutant="PM2.5" className="animate-fade-in" />
          <AQICard aqi={45} location="Nearby Station" pollutant="O₃" className="animate-fade-in" />
          <AQICard aqi={82} location="Regional Average" pollutant="NO₂" className="animate-fade-in" />
        </div>

        {/* Main Dashboard Tabs */}
        <Tabs defaultValue="overview" className="space-y-6">
          <TabsList className="grid w-full grid-cols-2 lg:grid-cols-5">
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="map">Map</TabsTrigger>
            <TabsTrigger value="forecast">Forecast</TabsTrigger>
            <TabsTrigger value="stations">Stations</TabsTrigger>
            <TabsTrigger value="alerts">Alerts</TabsTrigger>
          </TabsList>

          {/* Overview Tab */}
          <TabsContent value="overview" className="space-y-6" id="overview">
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <div className="lg:col-span-2 space-y-6">
                <AQIChart title="24-Hour Pollutant Trends" />
                <PollutantBreakdown />
              </div>
              <div className="space-y-6">
                <HealthRecommendations aqi={68} />
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
                      <p className="text-xs text-muted-foreground">Hourly NO₂, O₃, HCHO</p>
                    </div>
                    <span className="text-xs text-green-500">Active</span>
                  </div>
                  <div className="flex items-center justify-between p-3 rounded-lg bg-muted/50">
                    <div>
                      <p className="font-medium">Ground Stations</p>
                      <p className="text-xs text-muted-foreground">EPA AirNow, OpenAQ</p>
                    </div>
                    <span className="text-xs text-green-500">Active</span>
                  </div>
                  <div className="flex items-center justify-between p-3 rounded-lg bg-muted/50">
                    <div>
                      <p className="font-medium">NASA GEOS-CF Model</p>
                      <p className="text-xs text-muted-foreground">5-Day Forecast</p>
                    </div>
                    <span className="text-xs text-green-500">Active</span>
                  </div>
                </div>
              </div>
              <PollutantBreakdown />
            </div>
          </TabsContent>

          {/* Forecast Tab */}
          <TabsContent value="forecast" className="space-y-6" id="forecast">
            <div>
              <h2 className="text-2xl font-bold mb-4">4-Day Air Quality Forecast</h2>
              <ForecastCards />
            </div>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
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
              <HealthRecommendations aqi={68} />
            </div>
          </TabsContent>

          {/* Stations Tab */}
          <TabsContent value="stations" className="space-y-6">
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <div className="lg:col-span-2">
                <StationList />
              </div>
              <div className="space-y-6">
                <PollutantBreakdown />
                <div className="p-6 rounded-lg border bg-card">
                  <h3 className="text-lg font-semibold mb-4">Coverage Area</h3>
                  <div className="space-y-3 text-sm">
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Ground Stations</span>
                      <span className="font-medium">24 active</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Satellite Coverage</span>
                      <span className="font-medium">Regional</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Update Frequency</span>
                      <span className="font-medium">Hourly</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Resolution</span>
                      <span className="font-medium">5-25 km</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </TabsContent>

          {/* Alerts Tab */}
          <TabsContent value="alerts" className="space-y-6">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <AlertSettings />
              <div className="space-y-6">
                <div className="p-6 rounded-lg border bg-card">
                  <h3 className="text-lg font-semibold mb-4">Recent Alerts</h3>
                  <div className="space-y-3">
                    <div className="p-4 rounded-lg bg-orange-500/10 border border-orange-500/20">
                      <p className="font-medium text-sm mb-1">High Ozone Alert</p>
                      <p className="text-xs text-muted-foreground">AQI reached 125 at 2:00 PM</p>
                      <p className="text-xs text-muted-foreground mt-2">2 hours ago</p>
                    </div>
                    <div className="p-4 rounded-lg bg-yellow-500/10 border border-yellow-500/20">
                      <p className="font-medium text-sm mb-1">Moderate PM2.5</p>
                      <p className="text-xs text-muted-foreground">Levels increased to 85 μg/m³</p>
                      <p className="text-xs text-muted-foreground mt-2">5 hours ago</p>
                    </div>
                  </div>
                </div>
                <HealthRecommendations aqi={68} />
              </div>
            </div>
          </TabsContent>
        </Tabs>
      </div>

      {/* Chatbot */}
      <AirQualityChatbot />

      {/* Footer */}
      <footer className="border-t mt-12 bg-card/50">
        <div className="container mx-auto px-4 py-8">
          <div className="text-center text-sm text-muted-foreground">
            <p>AirWatch - Real-time air quality monitoring powered by NASA TEMPO, OpenAQ, and EPA AirNow</p>
            <p className="mt-2">© 2025 AirWatch. Built for healthier communities.</p>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default Index;

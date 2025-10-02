import { Wind, Activity, TrendingUp, Bell } from 'lucide-react';
import { Button } from '@/components/ui/button';
import AQICard from '@/components/AQICard';
import AirQualityMap from '@/components/AirQualityMap';
import AQIChart from '@/components/AQIChart';
import ForecastCards from '@/components/ForecastCards';
import LocationSearch from '@/components/LocationSearch';
import AirQualityChatbot from '@/components/AirQualityChatbot';
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
              <a href="#map" className="text-sm font-medium hover:text-primary transition-colors">Map</a>
              <a href="#data" className="text-sm font-medium hover:text-primary transition-colors">Data</a>
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
        className="relative py-20 overflow-hidden"
        style={{
          backgroundImage: `url(${heroImage})`,
          backgroundSize: 'cover',
          backgroundPosition: 'center',
        }}
      >
        <div className="absolute inset-0 bg-gradient-to-b from-background/80 via-background/60 to-background" />
        <div className="container mx-auto px-4 relative z-10">
          <div className="max-w-4xl mx-auto text-center space-y-6">
            <h2 className="text-5xl md:text-6xl font-bold">
              Breathe Easy with
              <span className="block bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent">
                Real-Time Air Quality
              </span>
            </h2>
            <p className="text-xl text-muted-foreground">
              Monitor pollutants, track forecasts, and make informed decisions about your outdoor activities
            </p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center pt-4">
              <Button size="lg" className="text-lg">
                <Activity className="w-5 h-5 mr-2" />
                View Live Data
              </Button>
              <Button size="lg" variant="outline" className="text-lg">
                <TrendingUp className="w-5 h-5 mr-2" />
                See Forecasts
              </Button>
            </div>
          </div>
        </div>
      </section>

      <div className="container mx-auto px-4 py-12 space-y-12">
        {/* Location Search */}
        <section>
          <LocationSearch />
        </section>

        {/* Current AQI */}
        <section>
          <h2 className="text-3xl font-bold mb-6">Current Air Quality</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <AQICard aqi={68} location="New York, NY" pollutant="PM2.5" />
            <AQICard aqi={42} location="San Francisco, CA" pollutant="O₃" />
          </div>
        </section>

        {/* Forecast */}
        <section id="forecast">
          <h2 className="text-3xl font-bold mb-6">7-Day Forecast</h2>
          <ForecastCards />
        </section>

        {/* Map */}
        <section id="map">
          <h2 className="text-3xl font-bold mb-6">Interactive Map</h2>
          <AirQualityMap />
        </section>

        {/* Charts */}
        <section id="data">
          <h2 className="text-3xl font-bold mb-6">Pollutant Trends</h2>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <AQIChart title="24-Hour Trends" />
            <AQIChart title="Weekly Average" />
          </div>
        </section>
      </div>

      {/* Chatbot */}
      <AirQualityChatbot />

      {/* Footer */}
      <footer className="border-t mt-20 bg-card/50">
        <div className="container mx-auto px-4 py-8">
          <div className="text-center text-sm text-muted-foreground">
            <p>AirWatch - Real-time air quality monitoring powered by satellite, ground, and model data</p>
            <p className="mt-2">© 2025 AirWatch. Built for healthier communities.</p>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default Index;

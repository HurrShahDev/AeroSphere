import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card } from '@/components/ui/card';
import { Search, MapPin, Loader2 } from 'lucide-react';

const LocationSearch = () => {
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [suggestions, setSuggestions] = useState<string[]>([]);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    // API call will go here
    setTimeout(() => {
      setLoading(false);
    }, 500);
  };

  const handleGeolocation = () => {
    setLoading(true);
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(
        (position) => {
          console.log('Location:', position.coords);
          setLoading(false);
          // Handle location data
        },
        (error) => {
          console.error('Geolocation error:', error);
          setLoading(false);
        }
      );
    }
  };

  return (
    <Card className="p-4">
      <form onSubmit={handleSearch} className="flex gap-2">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
          <Input
            type="text"
            placeholder="Search location..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="pl-10"
          />
        </div>
        <Button type="submit" disabled={loading}>
          {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Search'}
        </Button>
        <Button 
          type="button" 
          variant="outline"
          onClick={handleGeolocation}
          disabled={loading}
        >
          <MapPin className="w-4 h-4" />
        </Button>
      </form>

      {suggestions.length > 0 && (
        <div className="mt-2 space-y-1">
          {suggestions.map((suggestion, i) => (
            <button
              key={i}
              className="w-full text-left px-3 py-2 hover:bg-muted rounded-md text-sm"
              onClick={() => setQuery(suggestion)}
            >
              {suggestion}
            </button>
          ))}
        </div>
      )}
    </Card>
  );
};

export default LocationSearch;

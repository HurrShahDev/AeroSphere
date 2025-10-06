import { useState, useEffect, useRef, useContext } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card } from '@/components/ui/card';
import { Search, MapPin, Loader2 } from 'lucide-react';
import { LocationContext } from '@/context/LocationContext'; // ✅ import context

const MAPBOX_TOKEN =
  "pk.eyJ1IjoiYWRpbHVzbWFuaSIsImEiOiJjbWc5cTE2MTYwanlzMmlxdzNnaTBzMnAxIn0.Kg-pRNG5WA7VXyYZtpsaoA";

type Suggestion = {
  name: string;
  coordinates: [number, number]; // [lng, lat]
};

const LocationSearch = () => {
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [suggestions, setSuggestions] = useState<Suggestion[]>([]);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [showSuggestions, setShowSuggestions] = useState(false);

  const inputRef = useRef<HTMLInputElement>(null);

  const { setLocation } = useContext(LocationContext); // ✅ get setLocation from context

  // 🔹 Fetch suggestions
  useEffect(() => {
    if (!query.trim() || !showSuggestions) {
      setSuggestions([]);
      return;
    }

    const delayDebounce = setTimeout(async () => {
      setLoading(true);
      try {
        const response = await fetch(
          `https://api.mapbox.com/geocoding/v5/mapbox.places/${encodeURIComponent(
            query
          )}.json?access_token=${MAPBOX_TOKEN}&limit=5&country=us,ca,mx&types=place`
        );
        const data = await response.json();

        type Feature = {
          place_name: string;
          geometry: {
            coordinates: [number, number];
          };
        };

        setSuggestions(
          (data.features as Feature[]).map((item) => ({
            name: item.place_name,
            coordinates: item.geometry.coordinates,
          }))
        );
        setActiveIndex(-1);
      } catch (error) {
        console.error('Search error:', error);
      }
      setLoading(false);
    }, 400);

    return () => clearTimeout(delayDebounce);
  }, [query, showSuggestions]);

  // 🔹 Select suggestion
  const selectSuggestion = (s: Suggestion) => {
    setQuery(s.name);
    setSuggestions([]);
    setActiveIndex(-1);

    // ✅ Only pass city name to WAQI (first part before comma)
    const city = s.name.split(',')[0];
    setLocation(city);

    // Keep caret at end
    setTimeout(() => {
      setShowSuggestions(false);
      if (inputRef.current) {
        inputRef.current.focus();
        inputRef.current.setSelectionRange(s.name.length, s.name.length);
      }
    }, 0);

    console.log('Searching for:', city, s.coordinates);
  };

  // 🔹 Handle Enter & arrows
  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();

      if (!query.trim()) {
        return;
      }

      if (activeIndex >= 0 && activeIndex < suggestions.length) {
        selectSuggestion(suggestions[activeIndex]);
      } else {
        setSuggestions([]);
        setShowSuggestions(false);

        // ✅ Only pass first part of query if possible
        const city = query.split(',')[0];
        setLocation(city);

        console.log('Searching for:', city);
      }
    } else if (e.key === 'ArrowDown' && suggestions.length > 0) {
      e.preventDefault();
      setActiveIndex((prev) => (prev + 1) % suggestions.length);
    } else if (e.key === 'ArrowUp' && suggestions.length > 0) {
      e.preventDefault();
      setActiveIndex((prev) =>
        prev <= 0 ? suggestions.length - 1 : prev - 1
      );
    }
  };

  // 🔹 Geolocation
  const handleGeolocation = () => {
    setLoading(true);
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(
        (position) => {
          setLoading(false);

          // ✅ Format for WAQI: geo::LAT;LON
          const geoLocation = `geo:${position.coords.latitude};${position.coords.longitude}`;
          const name = `My Location: ${position.coords.latitude}, ${position.coords.longitude}`;

          setQuery(name);
          setSuggestions([]);
          setShowSuggestions(false);

          setLocation(geoLocation); // ✅ update context with WAQI-friendly geo format

          setTimeout(() => {
            if (inputRef.current) {
              inputRef.current.focus();
              inputRef.current.setSelectionRange(name.length, name.length);
            }
          }, 0);

          console.log('My Location:', geoLocation);
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
      <div className="flex gap-2 relative">
        {/* Input */}
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
          <Input
            ref={inputRef}
            type="text"
            placeholder="Search city in USA, Canada, or Mexico..."
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
              setShowSuggestions(true);
            }}
            onFocus={() => setShowSuggestions(true)}
            onKeyDown={handleKeyDown}
            className="pl-10"
          />

          {/* Suggestions */}
          {showSuggestions && query && suggestions.length > 0 && (
            <div className="absolute mt-1 w-full bg-background border rounded-md shadow-md z-10 max-h-60 overflow-y-auto">
              {suggestions.map((s, i) => (
                <button
                  key={i}
                  className={`w-full text-left px-3 py-2 rounded-md text-sm ${
                    i === activeIndex
                      ? 'bg-muted font-medium'
                      : 'hover:bg-muted'
                  }`}
                  onMouseDown={(e) => {
                    e.preventDefault(); // Prevent input blur
                    selectSuggestion(s);
                  }}
                >
                  {s.name}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Geolocation */}
        <Button
          type="button"
          variant="outline"
          onClick={handleGeolocation}
          disabled={loading}
        >
          {loading ? (
            <Loader2 className="w-4 h-4 animate-spin" />
          ) : (
            <MapPin className="w-4 h-4" />
          )}
        </Button>
      </div>
    </Card>
  );
};

export default LocationSearch;

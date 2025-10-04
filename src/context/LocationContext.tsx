import { createContext, useState, ReactNode } from 'react';

interface LocationContextProps {
  location: string;
  setLocation: (loc: string) => void;
}

export const LocationContext = createContext<LocationContextProps>({
  location: '',
  setLocation: () => {},
});

export const LocationProvider = ({ children }: { children: ReactNode }) => {
  const [location, setLocation] = useState('');
  return (
    <LocationContext.Provider value={{ location, setLocation }}>
      {children}
    </LocationContext.Provider>
  );
};

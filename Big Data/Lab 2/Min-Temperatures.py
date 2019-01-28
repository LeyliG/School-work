from mrjob.job import MRJob

class MRMinTemperature(MRJob):

    # convert the temperature to Fahrenheit
    def MakeFahrenheit(self, tenthsOfCelsius):
        celsius = float(tenthsOfCelsius) / 10.0
        fahrenheit = celsius * 1.8 + 32.0
        return fahrenheit

    # Objective: For entire year, find minimum temperature for
    # each weather station.
    
    # Key: Weather Station
    # Value: Temperature
    
    # The Mapper is extracting and organizing the data that we 
    # care about (moving forward) into KeyValue pairs.
    
    def mapper(self, _, line):  # line -> read by line
        # identifying the columns in a csv file 
        (location, date, type, data, x, y, z) = line.split(',')
        if (type == 'TMIN'):
            # calls the temperature conversion function only if it's a TMIN
            temperature = self.MakeFahrenheit(data)   
            yield location, temperature  #keep only the lines that have TMIN

    # Between the Mapper and the Reducer, take list of key-value 
    # pairs (from Mapper) and sorts by keys and groups together 
    # all values for that key
    
    # 
    def reducer(self, location, temps):
        yield location, min(temps)  # gives the minimum for each location


if __name__ == '__main__':
    MRMinTemperature.run()
    
# File title is Min-Temperatures.py
# Save as a txt file
# !python Min-Temperatures.py 1800weather.csv > mintemps.txt


# Output:
# "EZE00100082"	7.699999999999999
# "ITE00100554"	5.359999999999999
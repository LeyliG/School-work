from mrjob.job import MRJob

class MRMaxTemperature(MRJob):
    
    def MakeFahrenheit(self, tenthsOfCelsius):
        celsius = float(tenthsOfCelsius) / 10.0
        fahrenheit = celsius * 1.8 + 32.0
        return fahrenheit

    def mapper(self, _, line):
        (location, date, type, data, x, y, z) = line.split(',')
        if (type == 'TMAX'):
            temperature = self.MakeFahrenheit(data)
            yield location, temperature

    def reducer(self, location, temps):
        yield location, max(temps)


if __name__ == '__main__':
    MRMaxTemperature.run()
    
# Max-Temperatures.py
# !python Max-Temperatures.py 1800weather.csv > maxtemps.txt

# Outputs:
# "EZE00100082"	90.13999999999999
# "ITE00100554"	90.13999999999999
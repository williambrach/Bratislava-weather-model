import requests
import urllib.request
import time
import csv
from bs4 import BeautifulSoup
import sched, time

SECONDS = 60 * 30
header = ['time', 'station', 'temperature', 'wind_dir', 'wind_speed', "wind_impact", "clouds", "weather"]
with open('weather.csv', 'w', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(header)
s = sched.scheduler(time.time, time.sleep)

# get data func
def getData(sc):
    url = 'http://www.shmu.sk/sk/?page=59'
    response = requests.get(url)
    
    soup = BeautifulSoup(response.text, "html.parser")
    
    time = soup.findAll('h3')[0].text.strip()
    
    with open('weather.csv','a',encoding='utf-8') as fd:
        writer = csv.writer(fd)
        for row in soup.findAll('table')[0].find_all('tr'):
            columns = row.find_all("td")
            if columns[0].text not in ("Bratislava - Mlynská Dolina", "Bratislava Ivanka", "Bratislava Koliba"):
                continue
            else:
                station = columns[0].text.strip()
                temperature = columns[1].text.strip()
                wind_dir = columns[2].text.strip()
                wind_speed = columns[3].text.strip()
                wind_impact = columns[4].text.strip()
                clouds = columns[5].text.strip()
                weather = columns[6].text.strip()
                writer.writerow([time,station, temperature, wind_dir, wind_speed, wind_impact, clouds, weather])
    
    
    s.enter(SECONDS, 1, getData, (sc,))

# main
s.enter(SECONDS, 1, getData, (s,))
s.run()
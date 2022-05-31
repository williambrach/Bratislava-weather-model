import requests
import urllib.request
import time
import csv
from bs4 import BeautifulSoup
import sched, time
from datetime import datetime
from yoo_telegram import Notifier
import pygsheets
from config import BOT_TOKEN, TELEGRAM_USER


client = Notifier(BOT_TOKEN)
gc = pygsheets.authorize(
    service_file="keys/bratislava-weather-trends-b9b47037fa19.json"
)
SECONDS = 60 * 60


def getData(sc):
    try:
        sh = gc.open("weather-data")
        wks = sh.sheet1

        url = "http://www.shmu.sk/sk/?page=59"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        timeNow = datetime.now().strftime("%Y/%m/%d %H:%M")
        for row in soup.findAll("table")[0].find_all("tr"):
            columns = row.find_all("td")
            if columns[0].text not in (
                "Bratislava - Mlynská Dolina",
                "Bratislava Koliba",
            ):
                continue
            else:
                station = columns[0].text.strip()
                temperature = columns[1].text.strip().split(" ")[0]
                wind_dir = columns[2].text.strip()
                wind_speed = columns[3].text.strip().split(" ")[0]
                wind_gusts = columns[4].text.strip()
                pressure = columns[5].text.strip().split(" ")[0]
                clouds = columns[6].text.strip()
                weather = columns[7].text.strip()

            wks.insert_rows(
                1,
                number=1,
                values=[
                    timeNow,
                    station,
                    temperature,
                    wind_dir,
                    wind_speed,
                    wind_gusts,
                    pressure,
                    clouds,
                    weather,
                ],
                inherit=False,
            )
        sc.enter(SECONDS, 1, getData, (sc,))
    except Exception as e:
        timeErr = datetime.now().strftime("%Y/%m/%d %H:%M")
        e_msg = f"{timeErr} - Bratislava weather - {str(e)}"
        client.sendMessage(TELEGRAM_USER, e_msg)
        sc.enter(SECONDS, 1, getData, (sc,))


def main():
    s = sched.scheduler(time.time, time.sleep)
    try:
        s.enter(SECONDS, 1, getData, (s,))
        s.run()
    except Exception as e:
        timeErr = datetime.now().strftime("%Y/%m/%d %H:%M")
        e_msg = f"{timeErr} - Bratislava weather - {str(e)}"
        client.sendMessage(TELEGRAM_USER, e_msg)
        

if __name__ == "__main__":
    main()

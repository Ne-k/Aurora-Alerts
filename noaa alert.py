import os
import re
from datetime import datetime

import pytz
import requests
from dotenv import load_dotenv


class NOAAForecast:
    def __init__(self):
        self.url = "https://services.swpc.noaa.gov/text/3-day-forecast.txt"
        load_dotenv()
        self.discord_webhook = os.getenv('DISCORD_WEBHOOK')

    def fetch_forecast(self):
        response = requests.get(self.url)
        response.raise_for_status()
        return response.text

    def post_to_discord(self, message, file_content, tonight_forecast_content=None, tomorrow_forecast_content=None):
        data = {"content": message}
        files = {
            'file': ('forecast.txt', file_content)
        }
        if tonight_forecast_content:
            files['tonight_forecast.png'] = ('tonight_forecast.png', tonight_forecast_content)
        if tomorrow_forecast_content:
            files['tomorrow_forecast.png'] = ('tomorrow_forecast.png', tomorrow_forecast_content)

        response = requests.post(self.discord_webhook, data=data, files=files)
        response.raise_for_status()

    def check_kp_levels(self, forecast_text):
        kp_section_pattern = re.compile(r'NOAA Kp index breakdown.*?(?=Rationale:)', re.DOTALL)
        kp_section = kp_section_pattern.search(forecast_text)
        if not kp_section:
            return False

        times_pattern = re.compile(r'(\d+-\d+UT)')
        kp_values_pattern = re.compile(r'(\d+\.\d+)')
        times = times_pattern.findall(kp_section.group())
        kp_levels = kp_values_pattern.findall(kp_section.group())
        kp_levels = [float(kp) for kp in kp_levels]

        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        months_pattern = "|".join(months)
        days_pattern = re.compile(rf'\b(?:{months_pattern}) \d{{1,2}}\b')
        days = days_pattern.findall(forecast_text)

        if len(days) < 3 or len(times) < 8:
            return False

        above_6_info = []
        for i, kp in enumerate(kp_levels):
            if kp >= 7:
                day = days[i // 8]
                time = times[i % 8]
                above_6_info.append((day, time, kp))

        if above_6_info:
            tonight_forecast_url = "https://services.swpc.noaa.gov/experimental/images/aurora_dashboard/tonights_static_viewline_forecast.png"
            tomorrow_forecast_url = "https://services.swpc.noaa.gov/experimental/images/aurora_dashboard/tomorrow_nights_static_viewline_forecast.png"

            tonight_forecast = requests.get(tonight_forecast_url)
            tomorrow_forecast = requests.get(tomorrow_forecast_url)
            message = "[Aurora Dashboard](https://www.swpc.noaa.gov/communities/aurora-dashboard-experimental)\n"
            message += "```Aurora kp levels above or equal to 7 detected on:\n"
            message += "╔═══════════════════════════════════════════════════╗\n"
            for info in above_6_info:
                day, time, kp = info
                start_hour = int(time.split('-')[0])
                end_hour = int(time.split('-')[1][:2])
                utc = pytz.utc
                pst = pytz.timezone('US/Pacific')
                start_time_utc = utc.localize(datetime.strptime(f"{day} {start_hour}", "%b %d %H"))
                end_time_utc = utc.localize(datetime.strptime(f"{day} {end_hour}", "%b %d %H"))
                start_time_pst = start_time_utc.astimezone(pst)
                end_time_pst = end_time_utc.astimezone(pst)
                message += f"║ Day: {day}, Time: {start_time_pst.strftime('%I:%M %p')} - {end_time_pst.strftime('%I:%M %p')} PST, Kp level: {kp:.2f} ║\n"
            message += "╚═══════════════════════════════════════════════════╝\n"
            # message += "\nClick on the image to see the actual forecast) [Tonight's Aurora Forecast](https://services.swpc.noaa.gov/experimental/images/aurora_dashboard/tonights_static_viewline_forecast.png)"
            # message += "\n(Click on the image to see the actual forecast) [Tomorrow Night's Aurora Forecast](https://services.swpc.noaa.gov/experimental/images/aurora_dashboard/tomorrow_nights_static_viewline_forecast.png)"

            self.post_to_discord(message, forecast_text, tonight_forecast.content, tomorrow_forecast.content)
            return True
        else:
            return False

    def main(self):
        forecast_text = self.fetch_forecast()
        if self.check_kp_levels(forecast_text):
            print(f"{datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}: Kp levels above 6 detected!")
        else:
            print(f"{datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}: Kp levels are normal.")


if __name__ == "__main__":
    forecast = NOAAForecast()
    forecast.main()
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
        if not self.discord_webhook:
            print(f"{datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}: No Discord webhook URL configured!")
            return
            
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
            if kp >= 6.5:
                day = days[i // 8]
                time = times[i % 8]
                above_6_info.append((day, time, kp))

        if above_6_info:
            tonight_forecast_url = "https://services.swpc.noaa.gov/experimental/images/aurora_dashboard/tonights_static_viewline_forecast.png"
            tomorrow_forecast_url = "https://services.swpc.noaa.gov/experimental/images/aurora_dashboard/tomorrow_nights_static_viewline_forecast.png"

            tonight_forecast = requests.get(tonight_forecast_url)
            tomorrow_forecast = requests.get(tomorrow_forecast_url)
            
            # Get current timestamp for Discord formatting
            current_timestamp = int(datetime.now().timestamp())
            
            message = "🌌 **AURORA ALERT**\n\n"
            message += f"Alert detected at: <t:{current_timestamp}:F>\n"
            message += f"Time: <t:{current_timestamp}:R>\n\n"
            message += "[Aurora Dashboard](https://www.swpc.noaa.gov/communities/aurora-dashboard-experimental)\n\n"
            message += "**Aurora kp levels above or equal to 6.5 detected on:**\n"
            
            # Collect all aurora info lines first to determine max width
            aurora_lines = []
            for info in above_6_info:
                day, time, kp = info
                start_hour = int(time.split('-')[0])
                end_hour = int(time.split('-')[1][:2])
                utc = pytz.utc
                
                # Create datetime with current year for proper timestamp calculation
                current_year = datetime.now().year
                try:
                    start_time_utc = utc.localize(datetime.strptime(f"{current_year} {day} {start_hour}", "%Y %b %d %H"))
                    end_time_utc = utc.localize(datetime.strptime(f"{current_year} {day} {end_hour}", "%Y %b %d %H"))
                    
                    # Convert to Discord timestamps
                    start_discord_timestamp = int(start_time_utc.timestamp())
                    end_discord_timestamp = int(end_time_utc.timestamp())
                    
                    # Create line without Discord formatting for width calculation
                    display_line = f"Day: {day}, Time: <t:{start_discord_timestamp}:R> to <t:{end_discord_timestamp}:R> UTC, Kp level: {kp:.2f}"
                    # Estimate display width (Discord timestamps show as text)
                    estimated_line = f"Day: {day}, Time: in X hours to in X hours UTC, Kp level: {kp:.2f}"
                    aurora_lines.append((display_line, len(estimated_line)))
                except ValueError:
                    # Fallback to original format if timestamp parsing fails
                    display_line = f"Day: {day}, Time: {start_hour:02d}:00 - {end_hour:02d}:00 UTC, Kp level: {kp:.2f}"
                    aurora_lines.append((display_line, len(display_line)))
            
            # Find the maximum width needed (use a more accurate estimate)
            max_content_width = 0
            for line, estimated_len in aurora_lines:
                # Better estimate for Discord timestamp display
                clean_line = line.replace('<t:', '').replace(':R>', '')
                # Count timestamp placeholders and estimate their rendered length
                timestamp_count = line.count('<t:')
                # Each timestamp roughly displays as "X time ago" (about 15-20 chars average)
                estimated_display_length = len(clean_line) + (timestamp_count * 15)
                max_content_width = max(max_content_width, estimated_display_length)
            
            # Add padding for the border characters and some extra space
            max_width = max_content_width + 4
            
            # Create dynamic border
            top_border = "╔" + "═" * max_width + "╗"
            bottom_border = "╚" + "═" * max_width + "╝"
            
            message += f"{top_border}\n"
            for line, _ in aurora_lines:
                # Calculate padding more accurately
                clean_line = line.replace('<t:', '').replace(':R>', '')
                timestamp_count = line.count('<t:')
                estimated_length = len(clean_line) + (timestamp_count * 15)
                padding = max(0, max_width - estimated_length - 2)  # -2 for the border characters
                message += f"║ {line}" + " " * padding + " ║\n"
            message += f"{bottom_border}\n"
            # message += "\nClick on the image to see the actual forecast) [Tonight's Aurora Forecast](https://services.swpc.noaa.gov/experimental/images/aurora_dashboard/tonights_static_viewline_forecast.png)"
            # message += "\n(Click on the image to see the actual forecast) [Tomorrow Night's Aurora Forecast](https://services.swpc.noaa.gov/experimental/images/aurora_dashboard/tomorrow_nights_static_viewline_forecast.png)"

            self.post_to_discord(message, forecast_text, tonight_forecast.content, tomorrow_forecast.content)
            return True
        else:
            return False

    def send_test_message(self):
        """
        Send a test message using the forecastExample.txt data with Discord timestamps.
        """
        if not self.discord_webhook:
            print(f"{datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}: No Discord webhook URL configured!")
            return
            
        try:
            # Read the example forecast data
            with open('forecastExample.txt', 'r') as f:
                forecast_text = f.read()
            
            # Get current timestamp for Discord formatting
            current_timestamp = int(datetime.now().timestamp())
            
            # Create test message with same format as actual alert
            test_message = "🧪 **TEST AURORA ALERT** 🧪\n\n"
            test_message += f"Alert detected at: <t:{current_timestamp}:F>\n"
            test_message += f"Time: <t:{current_timestamp}:R>\n\n"
            test_message += "**This is a test using forecastExample.txt data**\n"
            test_message += "[Aurora Dashboard](https://www.swpc.noaa.gov/communities/aurora-dashboard-experimental)\n"
            
            # Check if the example data has high Kp levels and format accordingly
            if self.check_kp_levels(forecast_text):
                # The check_kp_levels method will handle sending the full alert message
                print(f"{datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}: Test message sent with alert data!")
            else:
                test_message += "**No high Kp levels detected in test data**\n"
                test_message += "✅ Test completed - system is operational\n"
                # Send a simple test message without attachments
                data = {"content": test_message}
                response = requests.post(self.discord_webhook, json=data)
                response.raise_for_status()
                print(f"{datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}: Test message sent successfully!")
                
        except FileNotFoundError:
            print(f"{datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}: forecastExample.txt not found!")
        except Exception as e:
            print(f"{datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}: Error sending test message: {e}")

    def main(self, test_mode=False):
        if test_mode:
            self.send_test_message()
            return
            
        forecast_text = self.fetch_forecast()
        if self.check_kp_levels(forecast_text):
            print(f"{datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}: Kp levels above 6 detected!")
        else:
            print(f"{datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}: Kp levels are normal.")


if __name__ == "__main__":
    import sys
    
    # Check for test flag
    test_mode = "--test" in sys.argv
    
    forecast = NOAAForecast()
    forecast.main(test_mode=test_mode)
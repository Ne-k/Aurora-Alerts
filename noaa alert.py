import os
import re
from datetime import datetime, timedelta, timezone
import json

import pytz
import requests
from dotenv import load_dotenv


class NOAAForecast:
    def __init__(self):
        self.url = "https://services.swpc.noaa.gov/text/3-day-forecast.txt"
        load_dotenv()
        self.discord_webhook = os.getenv('DISCORD_WEBHOOK')
        # Load configurable Kp threshold from environment (default to 6.5)
        try:
            self.kp_threshold = float(os.getenv('KP_THRESHOLD', '6.5'))
        except ValueError:
            print(f"Warning: Invalid KP_THRESHOLD value, using default 6.5")
            self.kp_threshold = 6.5

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

    def _state_path(self) -> str:
        # Persisted state to avoid duplicate alerts within the same 3-day window
        return os.path.join(os.path.dirname(__file__), 'alert_state.json')

    def _load_state(self):
        try:
            with open(self._state_path(), 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        except Exception:
            return {}

    def _save_state(self, state: dict):
        try:
            with open(self._state_path(), 'w') as f:
                json.dump(state, f)
        except Exception as e:
            print(f"[WARN] Failed to write state file: {e}")

    def check_kp_levels(self, forecast_text, debug: bool = False, record_state: bool = True):
        # Capture the entire Kp table block until the next section header
        kp_section_pattern = re.compile(
            r'NOAA Kp index breakdown[\s\S]*?(?=Rationale:|B\. NOAA|C\. NOAA|$)',
            re.DOTALL
        )
        kp_section_match = kp_section_pattern.search(forecast_text)
        if not kp_section_match:
            if debug:
                print("[DEBUG] Kp section not found in forecast text")
            return False

        kp_section = kp_section_match.group()

        # Extract the day headers from the Kp table header line
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        months_pattern = "|".join(months)
        # Prefer a strict header row with exactly three columns
        header_match = re.search(
            rf'^\s*((?:{months_pattern})\s+\d{{1,2}})\s+((?:{months_pattern})\s+\d{{1,2}})\s+((?:{months_pattern})\s+\d{{1,2}})\s*$',
            kp_section,
            flags=re.M
        )
        if header_match:
            days = [header_match.group(1), header_match.group(2), header_match.group(3)]
        else:
            # Fallback: scan lines and take the first with 3 tokens
            header_line = None
            for line in kp_section.splitlines():
                found = re.findall(rf'(?:{months_pattern})\s+\d{{1,2}}', line)
                if len(found) >= 3:
                    header_line = line
                    days = found[:3]
                    break
            if not header_line:
                if debug:
                    print("[DEBUG] Could not find day header line with three dates")
                return False
        # Safety trim
        days = [d.strip() for d in days][:3]
        if len(days) < 3:
            if debug:
                print(f"[DEBUG] Less than three day headers detected: {days}")
            return False

        # Determine issued year for accurate date construction
        issued_year_match = re.search(r':Issued:\s+(\d{4})', forecast_text)
        try:
            issued_year = int(issued_year_match.group(1)) if issued_year_match else datetime.now(timezone.utc).year
        except Exception:
            issued_year = datetime.now(timezone.utc).year

        # Build concrete date objects for each day column using issued year
        day_labels = days
        try:
            day_dates = [datetime.strptime(f"{issued_year} {d}", "%Y %b %d").date() for d in day_labels]
        except ValueError:
            # Fallback to current year if issued year parse fails unexpectedly
            fallback_year = datetime.now(timezone.utc).year
            day_dates = [datetime.strptime(f"{fallback_year} {d}", "%Y %b %d").date() for d in day_labels]

        # Extract each UT time row and the corresponding 3 values
        time_rows = re.findall(r'^\s*(\d{2}-\d{2})UT\s+([^\n]+)$', kp_section, flags=re.M)
        if len(time_rows) < 8:
            if debug:
                print(f"[DEBUG] Expected 8 UT rows, found {len(time_rows)}")
            return False

        above_6_info = []
        if debug:
            print(f"[DEBUG] Day headers: {days}")
        for interval, rest in time_rows:
            # Remove any annotation like (G1), (G2), (G3) to avoid column misalignment
            rest_clean = re.sub(r'\s*\(G\d+\)', '', rest)
            # Capture numeric values per column
            values = re.findall(r'(\d+(?:\.\d+)?)', rest_clean)
            if len(values) < 3:
                # Not enough columns, skip the row
                if debug:
                    print(f"[DEBUG] Skipping row {interval}UT, parsed values: {values}")
                continue
            if debug:
                print(f"[DEBUG] {interval}UT -> {values}")
            for col_idx, day_label in enumerate(day_labels):
                try:
                    kp = float(values[col_idx])
                except ValueError:
                    continue
                if kp >= self.kp_threshold:
                    above_6_info.append((day_label, day_dates[col_idx], f"{interval}UT", kp))

        if above_6_info:
            # Create a 3-day window ID (min to max dates) including threshold for dedupe
            window_id = f"{day_dates[0].isoformat()}_to_{day_dates[-1].isoformat()}_kp>={self.kp_threshold}"
            state = self._load_state()
            last_window_id = state.get('last_window_id')
            if last_window_id == window_id and record_state:
                if debug:
                    print(f"[DEBUG] Already alerted for window {window_id}; skipping post.")
                return False

            tonight_forecast_url = "https://services.swpc.noaa.gov/experimental/images/aurora_dashboard/tonights_static_viewline_forecast.png"
            tomorrow_forecast_url = "https://services.swpc.noaa.gov/experimental/images/aurora_dashboard/tomorrow_nights_static_viewline_forecast.png"

            tonight_forecast = requests.get(tonight_forecast_url)
            tomorrow_forecast = requests.get(tomorrow_forecast_url)
            
            # Prepare local timezone (PST/PDT via America/Los_Angeles) and format current time
            local_tz = pytz.timezone('America/Los_Angeles')
            now_local = datetime.now(local_tz)
            
            message = "🌌 **AURORA ALERT**\n\n"
            # Human-readable local time plus dynamic relative timestamp
            detected_ts = int(datetime.now(timezone.utc).timestamp())
            message += f"Alert detected at: <t:{detected_ts}:F> (\u2192 <t:{detected_ts}:R>)\n\n"
            message += "[Aurora Dashboard](https://www.swpc.noaa.gov/communities/aurora-dashboard-experimental)\n\n"
            message += f"**Aurora kp levels above or equal to {self.kp_threshold} detected on:**\n"
            
            # Sort detections by UTC day then interval start hour
            try:
                above_6_info.sort(key=lambda x: (x[1], int(x[2].split('-')[0])))
            except Exception:
                pass

            # Collect all aurora info lines, grouped by local date
            aurora_lines = []
            grouped: dict[str, list[tuple[str, int]]] = {}
            for info in above_6_info:
                day_label, day_date, time, kp = info
                start_hour = int(time.split('-')[0])
                end_hour = int(time.split('-')[1][:2])
                utc = pytz.utc
                
                # Create datetime with current year for proper timestamp calculation
                current_year = day_date.year
                try:
                    start_time_utc = utc.localize(datetime(current_year, day_date.month, day_date.day, start_hour, 0))
                    end_time_utc = utc.localize(datetime(current_year, day_date.month, day_date.day, end_hour, 0))
                    # Handle wrap-around intervals like 21-00UT (end hour <= start hour means next day)
                    if end_hour <= start_hour:
                        end_time_utc = end_time_utc + timedelta(days=1)
                    
                    # Convert to local timezone (America/Los_Angeles)
                    local_tz = pytz.timezone('America/Los_Angeles')
                    start_local = start_time_utc.astimezone(local_tz)
                    end_local = end_time_utc.astimezone(local_tz)

                    # Discord dynamic timestamps for start/end times, plus local short label
                    tz_abbr = start_local.strftime('%Z') or 'PT'
                    start_ts = int(start_time_utc.timestamp())
                    end_ts = int(end_time_utc.timestamp())
                    # Use Discord dynamic date for local date header key
                    local_date_label = f"<t:{start_ts}:D>"
                    # Example: <t:...:D> (PDT) • 00-03 UT → 5:00 PM – 8:00 PM • Kp 6.67
                    ut_block_label = (time[:-2] + " UT") if time.endswith("UT") else time
                    bullet = f"• {ut_block_label} → <t:{start_ts}:t> - <t:{end_ts}:t> • Kp {kp:.2f}"
                    # Estimate display width considering timestamp rendering (~12 chars per <t:...>)
                    timestamp_count = bullet.count('<t:')
                    clean_line = bullet.replace('<t:', '').replace(':t>', '').replace(':R>', '').replace(':F>', '')
                    estimated_len = len(clean_line) + timestamp_count * 12
                    grouped.setdefault(local_date_label, []).append((bullet, estimated_len))
                except ValueError:
                    # Fallback to original format if timestamp parsing fails
                    local_date_label = f"{day_label}"
                    bullet = f"• {start_hour:02d}:00 - {end_hour:02d}:00 UTC • Kp {kp:.2f}"
                    grouped.setdefault(local_date_label, []).append((bullet, len(bullet)))
            
            # Output grouped by local date with bold headers
            for date_label in sorted(grouped.keys()):
                message += f"**{date_label}**\n"
                for bullet, _ in grouped[date_label]:
                    message += f"{bullet}\n"
            # message += "\nClick on the image to see the actual forecast) [Tonight's Aurora Forecast](https://services.swpc.noaa.gov/experimental/images/aurora_dashboard/tonights_static_viewline_forecast.png)"
            # message += "\n(Click on the image to see the actual forecast) [Tomorrow Night's Aurora Forecast](https://services.swpc.noaa.gov/experimental/images/aurora_dashboard/tomorrow_nights_static_viewline_forecast.png)"

            # Only attempt posting if webhook is configured; method will no-op otherwise
            self.post_to_discord(message, forecast_text, tonight_forecast.content, tomorrow_forecast.content)
            if record_state:
                # Save dedupe marker
                state['last_window_id'] = window_id
                state['last_alert_ts'] = int(datetime.now(timezone.utc).timestamp())
                self._save_state(state)
            if debug:
                print("[DEBUG] High Kp detections:")
                for info in above_6_info:
                    print(f"[DEBUG] Day {info[0]} {info[2]} -> Kp {info[3]}")
            return True
        else:
            return False

    def send_test_message(self):
        """
        Send a test message using the forecastExample.txt data with Discord timestamps.
        """
        has_webhook = bool(self.discord_webhook)
        if not has_webhook:
            print(f"{datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}: No Discord webhook URL configured! Proceeding with console-only test.")
            
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
            if self.check_kp_levels(forecast_text, debug=True, record_state=False):
                # The check_kp_levels method will handle sending the full alert message
                print(f"{datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}: Test message parsed with alert data!")
            else:
                test_message += "**No high Kp levels detected in test data**\n"
                test_message += "✅ Test completed - system is operational\n"
                # Send a simple test message without attachments
                if has_webhook:
                    data = {"content": test_message}
                    assert self.discord_webhook is not None
                    response = requests.post(self.discord_webhook, json=data)
                    response.raise_for_status()
                    print(f"{datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}: Test message sent successfully!")
                else:
                    print(test_message)
                
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
            print(f"{datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}: Kp levels above {self.kp_threshold} detected!")
        else:
            print(f"{datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}: Kp levels are normal.")


if __name__ == "__main__":
    import sys
    
    # Check for test flag
    test_mode = "--test" in sys.argv
    
    forecast = NOAAForecast()
    forecast.main(test_mode=test_mode)
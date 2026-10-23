import requests
import pathlib
import time
import datetime
import logging
import json

# -------------------------------------------------------------------------
# LOGGING CONFIGURATION
# -------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------
# RATE LIMITING HELPER
# -------------------------------------------------------------------------
class RateLimiter:
    def __init__(self):
        self.requests = []

    def wait_if_needed(self):
        now = time.time()
        self.requests = [t for t in self.requests if now - t < 60]
        
        if len(self.requests) >= 30:
            sleep_time = 60 - (now - self.requests[0])
            if sleep_time > 0:
                logger.info(f"Minute rate limit hit. Sleeping for {sleep_time:.2f} seconds.")
                time.sleep(sleep_time)
                now = time.time()

        recent_sec = [t for t in self.requests if now - t < 1.0]
        if len(recent_sec) >= 3:
            sleep_time = 1.0 - (now - recent_sec[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
                now = time.time()

        self.requests.append(now)

limiter = RateLimiter()

# -------------------------------------------------------------------------
# API SETUP & DIRECTORY LAYOUTS
# -------------------------------------------------------------------------
session = requests.Session()

ENDPOINT_LAYOUTS = {
    "meetings": ["year"],
    "sessions": ["year"],
    "session_result": ["year", "session_key"],
    "laps": ["year", "session_key"],
    "stints": ["year", "session_key"],
    "pit": ["year", "session_key"],
    "weather": ["year", "session_key"],
    "race_control": ["year", "session_key"]
}


def fetch_openf1_data(endpoint_name: str, config_params: dict, max_retries: int = 3):
    base_url = f"https://api.openf1.org/v1/{endpoint_name.strip('/')}"
    
    api_params = {k: v for k, v in config_params.items() if k != "endpoint"}
    api_params = {k: str(v).lower() if isinstance(v, bool) else str(v) for k, v in api_params.items()}
    
    if "session_key" in config_params and "year" in api_params:
        del api_params["year"]

    base_wait = 2 

    for attempt in range(max_retries + 1):
        try:
            limiter.wait_if_needed()
            response = session.get(base_url, params=api_params, timeout=5)
            
            # 429 Too Many Requests Handling
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_time = int(retry_after) if retry_after else (base_wait * (2 ** attempt))
                
                if attempt < max_retries:
                    logger.warning(f"429 Too Many Requests on {base_url}. Retrying in {wait_time}s (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Max retries ({max_retries}) exhausted for 429 Too Many Requests.")
                    response.raise_for_status()

            # raise_for_status() is called BEFORE logging success
            response.raise_for_status()
            logger.info(f"Successfully fetched data from: {response.url}")
            
            return response.json(), response.url
            
        # Explicitly catch HTTP errors to retry 5xx, but fail fast on 4xx
        except requests.exceptions.HTTPError as e:
            if attempt < max_retries and e.response.status_code >= 500:
                wait_time = base_wait * (2 ** attempt)
                logger.warning(f"Server error {e.response.status_code}. Retrying in {wait_time}s (Attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                if attempt >= max_retries:
                    logger.error(f"API Request failed after {max_retries} attempts: {e}")
                else:
                    logger.error(f"Fatal HTTP Error (4xx): {e}")
                raise
                
        # Catch standard network drops (Timeouts, Connection Refused)
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                wait_time = base_wait * (2 ** attempt)
                logger.warning(f"Network error ({e}). Retrying in {wait_time}s (Attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                logger.error(f"API Request failed after {max_retries} attempts: {e}")
                raise


def save_raw_response(raw_data, endpoint_name, config_params, source_url):
    if not raw_data:
        logger.warning(f"Skipping save for {endpoint_name}. No records returned.")
        return

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    ingest_date = now_utc.strftime("%Y-%m-%d")
    timestamp_str = now_utc.strftime("%Y%m%dT%H%M%SZ")
    
    layout = ENDPOINT_LAYOUTS.get(endpoint_name)
    if not layout:
        raise ValueError(f"No partition layout defined for endpoint: {endpoint_name}")
        
    path_parts = [endpoint_name]
    for key in layout:
        if key not in config_params:
            raise KeyError(f"Missing required partition key '{key}' for endpoint '{endpoint_name}'.")
        path_parts.append(f"{key}={config_params[key]}")
        
    if "session_key" not in layout:
        path_parts.append(f"ingest_date={ingest_date}")

    current_dir = pathlib.Path(__file__).parent.resolve()
    landing_dir = current_dir.parent / "landing" / "openf1"
    
    for part in path_parts:
        landing_dir = landing_dir / str(part)
        
    landing_dir.mkdir(parents=True, exist_ok=True)
    
    base_filename = f"{endpoint_name}_{timestamp_str}"
    temp_file = landing_dir / f"{base_filename}.tmp"
    final_file = landing_dir / f"{base_filename}.jsonl"

    logger.info(f"Writing {len(raw_data)} records to temporary file: {temp_file.name}")
    
    with open(temp_file, "w", encoding="utf-8") as f:
        for item in raw_data:
            item["_ingest"] = {
                "fetched_at": now_utc.isoformat(),
                "source_url": source_url
            }
            f.write(json.dumps(item) + "\n")
            
    temp_file.rename(final_file)
    logger.info(f"Successfully committed data to {final_file}")


def main():
    logger.info("Starting dynamic OpenF1 ingestion pipeline")
    
    target_year = 2025
    sessions_config = {"endpoint": "sessions", "year": target_year}
    
    sessions_data, sessions_url = fetch_openf1_data("sessions", sessions_config)
    save_raw_response(sessions_data, "sessions", sessions_config, sessions_url)
    
    race_sessions = [
        s for s in sessions_data 
        if s.get("session_name") == "Race" 
        and not s.get("is_cancelled", False)
    ]
    
    if not race_sessions:
        logger.warning(f"No valid race sessions found for year {target_year}.")
        return

    test_race = race_sessions[0]
    
    race_key = test_race["session_key"]
    race_year = test_race.get("year", target_year)
    
    logger.info(f"Targeting test race: session_key={race_key}, year={race_year} ({test_race.get('location', 'Unknown')})")

    strategy_endpoints = ["laps", "stints", "pit", "weather", "race_control"]
    dynamic_jobs = []
    
    for ep in strategy_endpoints:
        dynamic_jobs.append({
            "endpoint": ep,
            "year": race_year,
            "session_key": race_key
        })

    for config in dynamic_jobs:
        endpoint_name = config["endpoint"]
        data, source_url = fetch_openf1_data(endpoint_name, config)
        save_raw_response(data, endpoint_name, config, source_url)
        
    logger.info("Pipeline execution completed successfully.")


if __name__ == '__main__':
    main()
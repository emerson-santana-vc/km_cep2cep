"""Service for integrating IBGE city code data."""

from functools import lru_cache
from typing import Optional

import requests


IBGE_API_BASE = "https://servicodados.ibge.gov.br/api/v1"


@lru_cache(maxsize=1024)
def get_city_from_ibge_code(ibge_code: str) -> Optional[dict[str, object]]:
    """
    Fetch city information from IBGE API by city code.
    Returns dict with: code, name, state, latitude, longitude or None if not found.
    """
    if not ibge_code or not ibge_code.isdigit():
        return None
    
    try:
        # IBGE API endpoint: /localidades/municipios/{código}
        url = f"{IBGE_API_BASE}/localidades/municipios/{ibge_code}"
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        
        # Extract state code from the response structure
        state_code = data.get("microrregiao", {}).get("mesorregiao", {}).get("estado", {}).get("sigla", "")
        
        return {
            "ibge_code": ibge_code,
            "city_name": data.get("nome", ""),
            "state_code": state_code,
            "latitude": None,  # IBGE API doesn't provide coordinates directly
            "longitude": None,
        }
    except (requests.RequestException, KeyError, ValueError):
        return None


def fetch_all_brazilian_cities() -> list[dict[str, object]]:
    """
    Fetch all Brazilian cities from IBGE API.
    Returns list of dicts with: code, name, state_code, latitude, longitude
    """
    cities = []
    
    try:
        # Get all states first
        url = f"{IBGE_API_BASE}/localidades/estados"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        states = response.json()
        
        # For each state, get all municipalities
        for state in states:
            state_code = state.get("sigla", "")
            state_id = state.get("id",  0)
            
            try:
                url = f"{IBGE_API_BASE}/localidades/estados/{state_id}/municipios"
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                municipalities = response.json()
                
                for city in municipalities:
                    cities.append({
                        "ibge_code": str(city.get("id", "")),
                        "city_name": city.get("nome", ""),
                        "state_code": state_code,
                        "latitude": None,  # IBGE API doesn't provide coordinates
                        "longitude": None,
                    })
            except requests.RequestException:
                continue
    
    except requests.RequestException:
        return []
    
    return cities


def validate_city_against_ibge_code(city_name: str, state_code: str, ibge_code: Optional[str]) -> bool:
    """
    Validate if a city name and state match the provided IBGE code.
    Returns True if valid, False otherwise.
    """
    if not ibge_code:
        return True  # No IBGE code to validate against
    
    city_info = get_city_from_ibge_code(ibge_code)
    if not city_info:
        return False  # Invalid IBGE code
    
    # Normalize and compare
    city_clean = (city_name or "").strip().upper().replace(" ", "").replace("-", "").replace("Á", "A").replace("À", "A").replace("Ã", "A")
    ibge_city_clean = (city_info.get("city_name", "") or "").strip().upper().replace(" ", "").replace("-", "").replace("Á", "A").replace("À", "A").replace("Ã", "A")
    state_clean = (state_code or "").strip().upper()
    ibge_state_clean = (city_info.get("state_code", "") or "").strip().upper()
    
    # Check if city name and state match
    return city_clean == ibge_city_clean and state_clean == ibge_state_clean

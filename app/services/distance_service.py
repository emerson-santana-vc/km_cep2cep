import math
import os
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from typing import Optional, Tuple

import requests
from geopy.geocoders import Nominatim


class DistanceMode(str, Enum):
    ROUTE = "Rota viária aproximada"
    HAVERSINE = "Distância em linha reta (Haversine)"


@dataclass
class DistanceResult:
    origin_lat: Optional[float]
    origin_lng: Optional[float]
    destination_lat: Optional[float]
    destination_lng: Optional[float]
    distance_km: Optional[float]
    status: str
    error_message: Optional[str] = None


@lru_cache(maxsize=1024)
def _geocode_address(address: str) -> Optional[Tuple[float, float]]:
    user_agent = os.getenv("GEOCODER_USER_AGENT", "km-cep2cep-app")
    geolocator = Nominatim(user_agent=user_agent, timeout=10)
    location = geolocator.geocode(address)
    if not location:
        return None
    return float(location.latitude), float(location.longitude)


def _haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def _route_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> Optional[float]:
    osrm_base_url = os.getenv("OSRM_BASE_URL")
    if not osrm_base_url:
        return _haversine_distance_km(lat1, lon1, lat2, lon2)

    url = f"{osrm_base_url.rstrip('/')}/route/v1/driving/{lon1},{lat1};{lon2},{lat2}"
    params = {"overview": "false"}
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        routes = data.get("routes") or []
        if not routes:
            return None
        meters = routes[0].get("distance")
        if meters is None:
            return None
        return float(meters) / 1000.0
    except Exception:
        return _haversine_distance_km(lat1, lon1, lat2, lon2)


def calculate_distance_single(origin_address: str, destination_address: str, mode: DistanceMode) -> DistanceResult:
    try:
        origin_coords = _geocode_address(origin_address)
        destination_coords = _geocode_address(destination_address)
    except Exception as exc:
        return DistanceResult(
            origin_lat=None,
            origin_lng=None,
            destination_lat=None,
            destination_lng=None,
            distance_km=None,
            status="error",
            error_message=str(exc),
        )

    if not origin_coords or not destination_coords:
        return DistanceResult(
            origin_lat=origin_coords[0] if origin_coords else None,
            origin_lng=origin_coords[1] if origin_coords else None,
            destination_lat=destination_coords[0] if destination_coords else None,
            destination_lng=destination_coords[1] if destination_coords else None,
            distance_km=None,
            status="error",
            error_message="Falha na geocodificação de um ou ambos os endereços.",
        )

    origin_lat, origin_lng = origin_coords
    destination_lat, destination_lng = destination_coords

    if mode == DistanceMode.ROUTE:
        distance_km = _route_distance_km(origin_lat, origin_lng, destination_lat, destination_lng)
    else:
        distance_km = _haversine_distance_km(origin_lat, origin_lng, destination_lat, destination_lng)

    if distance_km is None:
        status = "error"
        error_message = "Não foi possível calcular a distância."
    else:
        status = "ok"
        error_message = None

    return DistanceResult(
        origin_lat=origin_lat,
        origin_lng=origin_lng,
        destination_lat=destination_lat,
        destination_lng=destination_lng,
        distance_km=distance_km,
        status=status,
        error_message=error_message,
    )


def calculate_distance_batch(pairs: list[tuple[str, str]], mode: DistanceMode) -> list[DistanceResult]:
    results: list[DistanceResult] = []
    for origin, destination in pairs:
        results.append(calculate_distance_single(origin, destination, mode))
    return results


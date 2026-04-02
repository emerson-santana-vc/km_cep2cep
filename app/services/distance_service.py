import math
import os
import re
import time
import unicodedata
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from typing import Optional, Tuple

import requests
from geopy.geocoders import Nominatim


class DistanceMode(str, Enum):
    ROUTE = "Rota viária aproximada"
    HAVERSINE = "Distância em linha reta (Haversine)"


class GeocodingProvider(str, Enum):
    AUTO = "Auto"
    NOMINATIM = "Nominatim"
    GOOGLE = "Google Maps"
    OPENROUTESERVICE = "OpenRouteService"


class RoutingProvider(str, Enum):
    AUTO = "Auto"
    OSRM = "OSRM"
    GOOGLE = "Google Maps"
    OPENROUTESERVICE = "OpenRouteService"


@dataclass
class DistanceResult:
    origin_lat: Optional[float]
    origin_lng: Optional[float]
    destination_lat: Optional[float]
    destination_lng: Optional[float]
    distance_km: Optional[float]
    status: str
    error_message: Optional[str] = None
    geocoding_provider_used: Optional[str] = None
    routing_provider_used: Optional[str] = None
    fallback_used: bool = False


@dataclass
class GeocodeMatch:
    lat: float
    lng: float
    locality: Optional[str] = None
    uf: Optional[str] = None
    postal_code: Optional[str] = None


CEP_PATTERN = re.compile(r"\b\d{5}-?\d{3}\b")
UF_PATTERN = re.compile(r"\b(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)\b")

STATE_NAME_TO_UF = {
    "ACRE": "AC",
    "ALAGOAS": "AL",
    "AMAPA": "AP",
    "AMAZONAS": "AM",
    "BAHIA": "BA",
    "CEARA": "CE",
    "DISTRITO FEDERAL": "DF",
    "ESPIRITO SANTO": "ES",
    "GOIAS": "GO",
    "MARANHAO": "MA",
    "MATO GROSSO": "MT",
    "MATO GROSSO DO SUL": "MS",
    "MINAS GERAIS": "MG",
    "PARA": "PA",
    "PARAIBA": "PB",
    "PARANA": "PR",
    "PERNAMBUCO": "PE",
    "PIAUI": "PI",
    "RIO DE JANEIRO": "RJ",
    "RIO GRANDE DO NORTE": "RN",
    "RIO GRANDE DO SUL": "RS",
    "RONDONIA": "RO",
    "RORAIMA": "RR",
    "SANTA CATARINA": "SC",
    "SAO PAULO": "SP",
    "SERGIPE": "SE",
    "TOCANTINS": "TO",
}

STATE_APPROX_CENTROIDS = {
    "AC": (-8.77, -70.55),
    "AL": (-9.65, -36.73),
    "AP": (1.41, -51.77),
    "AM": (-3.07, -61.66),
    "BA": (-12.96, -38.51),
    "CE": (-3.72, -38.54),
    "DF": (-15.79, -47.88),
    "ES": (-20.32, -40.34),
    "GO": (-16.64, -49.31),
    "MA": (-2.53, -44.30),
    "MT": (-15.60, -56.10),
    "MS": (-20.47, -54.62),
    "MG": (-18.10, -44.38),
    "PA": (-5.53, -52.29),
    "PB": (-7.06, -35.55),
    "PR": (-24.89, -51.55),
    "PE": (-8.28, -35.07),
    "PI": (-7.06, -42.77),
    "RJ": (-22.84, -43.15),
    "RN": (-5.79, -36.52),
    "RS": (-30.03, -51.23),
    "RO": (-10.83, -63.34),
    "RR": (1.99, -61.33),
    "SC": (-27.33, -49.44),
    "SP": (-23.55, -46.63),
    "SE": (-10.57, -37.45),
    "TO": (-10.18, -48.33),
}


def _request_json_with_retries(method: str, url: str, *, attempts: int = 2, **kwargs) -> dict:
    last_exc: Optional[Exception] = None
    for attempt in range(attempts):
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < attempts - 1:
                time.sleep(0.4)
            else:
                raise
    if last_exc:
        raise last_exc
    return {}


@lru_cache(maxsize=1024)
def _geocode_nominatim(address: str) -> Optional[GeocodeMatch]:
    user_agent = os.getenv("GEOCODER_USER_AGENT", "km-cep2cep-app")
    geolocator = Nominatim(user_agent=user_agent, timeout=10)
    location = geolocator.geocode(address)
    if not location:
        return None
    raw_address = location.raw.get("address") if isinstance(location.raw, dict) else {}
    locality = raw_address.get("city") or raw_address.get("town") or raw_address.get("village") or raw_address.get("municipality")
    uf = raw_address.get("state_code") or raw_address.get("state")
    postal_code = raw_address.get("postcode")
    return GeocodeMatch(float(location.latitude), float(location.longitude), locality=locality, uf=uf, postal_code=postal_code)


@lru_cache(maxsize=1024)
def _geocode_google(address: str) -> Optional[GeocodeMatch]:
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        return None

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    data = _request_json_with_retries(
        "GET",
        url,
        attempts=2,
        params={
            "address": address,
            "key": api_key,
            "region": "br",
            "language": "pt-BR",
            "components": "country:BR",
        },
        timeout=10,
    )
    if data.get("status") != "OK":
        return None

    results = data.get("results") or []
    if not results:
        return None

    first_result = results[0] or {}
    location = ((first_result.get("geometry") or {}).get("location") or {})
    lat = location.get("lat")
    lng = location.get("lng")
    if lat is None or lng is None:
        return None
    locality = _extract_google_component(first_result, "locality") or _extract_google_component(first_result, "administrative_area_level_2")
    uf = _extract_google_component(first_result, "administrative_area_level_1")
    postal_code = _extract_google_component(first_result, "postal_code")
    return GeocodeMatch(float(lat), float(lng), locality=locality, uf=uf, postal_code=postal_code)


@lru_cache(maxsize=1024)
def _geocode_openrouteservice(address: str) -> Optional[GeocodeMatch]:
    api_key = os.getenv("OPENROUTESERVICE_API_KEY")
    if not api_key:
        return None

    base_url = os.getenv("OPENROUTESERVICE_BASE_URL", "https://api.openrouteservice.org")
    url = f"{base_url.rstrip('/')}/geocode/search"
    data = _request_json_with_retries(
        "GET",
        url,
        attempts=2,
        params={"api_key": api_key, "text": address, "size": 1, "boundary.country": "BRA"},
        timeout=10,
    )
    features = data.get("features") or []
    if not features:
        return None

    first_feature = features[0] or {}
    coords = ((first_feature.get("geometry") or {}).get("coordinates") or [])
    if len(coords) < 2:
        return None

    lon, lat = coords[0], coords[1]
    properties = first_feature.get("properties") or {}
    locality = properties.get("locality") or properties.get("county") or properties.get("region")
    uf = properties.get("region_a") or properties.get("state") or properties.get("macroregion")
    postal_code = properties.get("postalcode")
    return GeocodeMatch(float(lat), float(lon), locality=locality, uf=uf, postal_code=postal_code)


def _provider_chain_geocoding(preferred: GeocodingProvider) -> list[GeocodingProvider]:
    if preferred == GeocodingProvider.GOOGLE:
        return [GeocodingProvider.GOOGLE, GeocodingProvider.OPENROUTESERVICE, GeocodingProvider.NOMINATIM]
    if preferred == GeocodingProvider.OPENROUTESERVICE:
        return [GeocodingProvider.OPENROUTESERVICE, GeocodingProvider.GOOGLE, GeocodingProvider.NOMINATIM]
    if preferred == GeocodingProvider.NOMINATIM:
        return [GeocodingProvider.NOMINATIM, GeocodingProvider.OPENROUTESERVICE, GeocodingProvider.GOOGLE]
    return [GeocodingProvider.OPENROUTESERVICE, GeocodingProvider.GOOGLE, GeocodingProvider.NOMINATIM]


def _provider_chain_routing(preferred: RoutingProvider) -> list[RoutingProvider]:
    if preferred == RoutingProvider.GOOGLE:
        return [RoutingProvider.GOOGLE, RoutingProvider.OPENROUTESERVICE, RoutingProvider.OSRM]
    if preferred == RoutingProvider.OPENROUTESERVICE:
        return [RoutingProvider.OPENROUTESERVICE, RoutingProvider.GOOGLE, RoutingProvider.OSRM]
    if preferred == RoutingProvider.OSRM:
        return [RoutingProvider.OSRM, RoutingProvider.OPENROUTESERVICE, RoutingProvider.GOOGLE]
    return [RoutingProvider.OPENROUTESERVICE, RoutingProvider.GOOGLE, RoutingProvider.OSRM]


def _find_cep(value: str) -> Optional[str]:
    match = CEP_PATTERN.search(value)
    if not match:
        return None
    return re.sub(r"\D", "", match.group(0))


@lru_cache(maxsize=1024)
def _lookup_cep(cep_digits: str) -> Optional[dict]:
    if len(cep_digits) != 8:
        return None

    url = f"https://viacep.com.br/ws/{cep_digits}/json/"
    data = _request_json_with_retries("GET", url, attempts=2, timeout=10)

    if data.get("erro"):
        return None
    return data


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def _clean_address_text(value: str) -> str:
    compact = re.sub(r"\s*,\s*", ", ", value)
    compact = re.sub(r"\s+", " ", compact)
    compact = re.sub(r",\s*,+", ", ", compact)
    return compact.strip(" ,")


def _extract_uf_from_text(value: str) -> Optional[str]:
    if not value:
        return None
    upper = _strip_accents(value).upper()
    tokens = re.split(r"[^A-Z]", upper)
    for token in tokens:
        if UF_PATTERN.fullmatch(token):
            return token
    return None


def _normalize_token(value: Optional[str]) -> str:
    if not value:
        return ""
    return _strip_accents(str(value)).upper().strip()


def _normalize_uf(value: Optional[str]) -> str:
    token = _normalize_token(value)
    if not token:
        return ""
    if UF_PATTERN.fullmatch(token):
        return token
    return STATE_NAME_TO_UF.get(token, token)


def _extract_google_component(result: dict, component_type: str) -> Optional[str]:
    components = result.get("address_components") or []
    for component in components:
        types = component.get("types") or []
        if component_type in types:
            return component.get("long_name") or component.get("short_name")
    return None


def _match_cep_context(match: GeocodeMatch, cep_data: Optional[dict]) -> bool:
    if not cep_data:
        return True

    expected_uf = _normalize_uf(cep_data.get("uf"))
    expected_locality = _normalize_token(cep_data.get("localidade"))
    expected_postal = re.sub(r"\D", "", str(cep_data.get("cep") or ""))

    match_uf = _normalize_uf(match.uf)
    match_locality = _normalize_token(match.locality)
    match_postal = re.sub(r"\D", "", str(match.postal_code or ""))

    if expected_uf and not match_uf:
        return False
    if expected_uf and match_uf != expected_uf:
        return False
    if expected_locality and not match_locality:
        return False
    if expected_locality and match_locality != expected_locality:
        return False
    if expected_postal and match_postal and match_postal != expected_postal:
        return False
    return True


def _distance_km_between(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    return _haversine_distance_km(lat1, lon1, lat2, lon2)


def _is_plausible_geocode(match: GeocodeMatch, cep_data: Optional[dict]) -> bool:
    if not cep_data:
        return True

    uf = _normalize_uf(cep_data.get("uf"))
    expected_city = _normalize_token(cep_data.get("localidade"))

    if not uf:
        return True

    centroid = STATE_APPROX_CENTROIDS.get(uf)
    if not centroid:
        return True

    city = _normalize_token(match.locality)
    if expected_city and city and city != expected_city:
        return False

    distance_to_state_center = _distance_km_between(match.lat, match.lng, centroid[0], centroid[1])
    return distance_to_state_center <= 450.0


def _compose_from_cep(cep_data: dict) -> tuple[str, str, str]:
    cep = re.sub(r"\D", "", str(cep_data.get("cep") or ""))
    formatted_cep = f"{cep[:5]}-{cep[5:]}" if len(cep) == 8 else cep
    uf = str(cep_data.get("uf") or "").strip().upper()
    localidade = str(cep_data.get("localidade") or "").strip()
    logradouro = str(cep_data.get("logradouro") or "").strip()

    cep_only = f"{formatted_cep}, Brasil" if formatted_cep else ""
    cep_uf = f"{formatted_cep}, {uf}, Brasil" if formatted_cep and uf else cep_only
    if formatted_cep and uf and localidade:
        cep_uf = f"{formatted_cep}, {localidade}, {uf}, Brasil"

    cep_uf_logradouro = cep_uf
    if logradouro and formatted_cep and uf and localidade:
        cep_uf_logradouro = f"{formatted_cep}, {localidade}, {uf}, {logradouro}, Brasil"
    elif logradouro and formatted_cep and uf:
        cep_uf_logradouro = f"{formatted_cep}, {uf}, {logradouro}, Brasil"
    return cep_only, cep_uf, cep_uf_logradouro


def _build_search_candidates(address: str) -> tuple[list[str], Optional[str], Optional[dict]]:
    raw = (address or "").strip()
    candidates: list[str] = []
    cep = _find_cep(raw)
    if cep:
        cep_data = _lookup_cep(cep)
        if cep_data:
            input_uf = _extract_uf_from_text(raw)
            cep_uf = str(cep_data.get("uf") or "").strip().upper()
            cep_localidade = _normalize_token(cep_data.get("localidade"))
            if input_uf and cep_uf and input_uf != cep_uf:
                return [], f"CEP {cep} inválido para UF informada ({input_uf}). ViaCEP retornou UF {cep_uf}.", cep_data

            cep_only, cep_plus_uf, cep_plus_uf_logradouro = _compose_from_cep(cep_data)
            ordered_candidates = [
                cep_only,
                cep_plus_uf,
                cep_plus_uf_logradouro,
            ]

            if cep_localidade:
                ordered_candidates.insert(1, f"{_clean_address_text(cep_only.replace(', Brasil', ''))}, {cep_data.get('localidade')}, {cep_uf}, Brasil")

            for candidate in ordered_candidates:
                cleaned = _clean_address_text(candidate)
                if cleaned and cleaned not in candidates:
                    candidates.append(cleaned)
        else:
            formatted_cep = f"{cep[:5]}-{cep[5:]}"
            for candidate in (f"{formatted_cep}, Brasil", f"{cep}, Brasil"):
                cleaned = _clean_address_text(candidate)
                if cleaned and cleaned not in candidates:
                    candidates.append(cleaned)

    cleaned_raw = _clean_address_text(raw)
    for candidate in (raw, cleaned_raw, _strip_accents(cleaned_raw)):
        normalized_candidate = _clean_address_text(candidate)
        if normalized_candidate and "BRASIL" not in _strip_accents(normalized_candidate).upper():
            normalized_candidate = f"{normalized_candidate}, Brasil"
        if normalized_candidate and normalized_candidate not in candidates:
            candidates.append(normalized_candidate)

    return candidates, None, cep_data if cep else None

def _geocode_with_fallback(address: str, preferred: GeocodingProvider) -> tuple[Optional[Tuple[float, float]], Optional[str], bool, Optional[str]]:
    providers = _provider_chain_geocoding(preferred)
    candidates, pre_validation_error, cep_data = _build_search_candidates(address)
    if pre_validation_error:
        return None, None, False, pre_validation_error

    for index, provider in enumerate(providers):
        for candidate in candidates:
            try:
                if provider == GeocodingProvider.NOMINATIM:
                    coords = _geocode_nominatim(candidate)
                elif provider == GeocodingProvider.GOOGLE:
                    coords = _geocode_google(candidate)
                else:
                    coords = _geocode_openrouteservice(candidate)
            except Exception:
                coords = None

            if coords and _match_cep_context(coords, cep_data) and _is_plausible_geocode(coords, cep_data):
                return (coords.lat, coords.lng), provider.name, index > 0, None

    return None, None, False, None


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
        return None

    url = f"{osrm_base_url.rstrip('/')}/route/v1/driving/{lon1},{lat1};{lon2},{lat2}"
    params = {"overview": "false"}
    data = _request_json_with_retries("GET", url, attempts=2, params=params, timeout=10)
    routes = data.get("routes") or []
    if not routes:
        return None

    meters = routes[0].get("distance")
    if meters is None:
        return None
    return float(meters) / 1000.0


def _route_distance_google_km(lat1: float, lon1: float, lat2: float, lon2: float) -> Optional[float]:
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        return None

    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": f"{lat1},{lon1}",
        "destination": f"{lat2},{lon2}",
        "key": api_key,
        "region": "br",
        "language": "pt-BR",
    }
    data = _request_json_with_retries("GET", url, attempts=2, params=params, timeout=10)
    if data.get("status") != "OK":
        return None

    routes = data.get("routes") or []
    if not routes:
        return None

    legs = routes[0].get("legs") or []
    if not legs:
        return None

    meters = ((legs[0].get("distance") or {}).get("value"))
    if meters is None:
        return None
    return float(meters) / 1000.0


def _route_distance_openrouteservice_km(lat1: float, lon1: float, lat2: float, lon2: float) -> Optional[float]:
    api_key = os.getenv("OPENROUTESERVICE_API_KEY")
    if not api_key:
        return None

    base_url = os.getenv("OPENROUTESERVICE_BASE_URL", "https://api.openrouteservice.org")
    url = f"{base_url.rstrip('/')}/v2/directions/driving-car"
    headers = {"Authorization": api_key, "Content-Type": "application/json"}
    payload = {"coordinates": [[lon1, lat1], [lon2, lat2]]}
    data = _request_json_with_retries("POST", url, attempts=2, headers=headers, json=payload, timeout=15)
    routes = data.get("routes") or []
    if not routes:
        return None

    summary = routes[0].get("summary") or {}
    meters = summary.get("distance")
    if meters is None:
        return None
    return float(meters) / 1000.0


def _route_with_fallback(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    preferred: RoutingProvider,
) -> tuple[Optional[float], Optional[str], bool]:
    providers = _provider_chain_routing(preferred)

    for index, provider in enumerate(providers):
        try:
            if provider == RoutingProvider.GOOGLE:
                distance = _route_distance_google_km(lat1, lon1, lat2, lon2)
            elif provider == RoutingProvider.OPENROUTESERVICE:
                distance = _route_distance_openrouteservice_km(lat1, lon1, lat2, lon2)
            else:
                distance = _route_distance_km(lat1, lon1, lat2, lon2)
        except Exception:
            distance = None

        if distance is not None:
            return distance, provider.name, index > 0

    return None, None, False


def calculate_distance_single(
    origin_address: str,
    destination_address: str,
    mode: DistanceMode,
    geocoding_provider: GeocodingProvider = GeocodingProvider.AUTO,
    routing_provider: RoutingProvider = RoutingProvider.AUTO,
) -> DistanceResult:
    try:
        origin_coords, origin_provider, origin_fallback, origin_geo_error = _geocode_with_fallback(origin_address, geocoding_provider)
        destination_coords, destination_provider, destination_fallback, destination_geo_error = _geocode_with_fallback(
            destination_address,
            geocoding_provider,
        )
    except Exception:
        origin_coords = None
        destination_coords = None
        origin_provider = None
        destination_provider = None
        origin_fallback = False
        destination_fallback = False
        origin_geo_error = None
        destination_geo_error = None

    if not origin_coords or not destination_coords:
        error_messages = [msg for msg in [origin_geo_error, destination_geo_error] if msg]
        error_message = " | ".join(error_messages) if error_messages else "Falha na geocodificação de um ou ambos os endereços."
        return DistanceResult(
            origin_lat=origin_coords[0] if origin_coords else None,
            origin_lng=origin_coords[1] if origin_coords else None,
            destination_lat=destination_coords[0] if destination_coords else None,
            destination_lng=destination_coords[1] if destination_coords else None,
            distance_km=None,
            status="error",
            error_message=error_message,
            geocoding_provider_used=origin_provider or destination_provider,
            routing_provider_used=None,
            fallback_used=origin_fallback or destination_fallback,
        )

    origin_lat, origin_lng = origin_coords
    destination_lat, destination_lng = destination_coords

    if mode == DistanceMode.ROUTE:
        distance_km, route_provider_used, route_fallback = _route_with_fallback(
            origin_lat,
            origin_lng,
            destination_lat,
            destination_lng,
            preferred=routing_provider,
        )
        if distance_km is None:
            distance_km = _haversine_distance_km(origin_lat, origin_lng, destination_lat, destination_lng)
            route_provider_used = "HAVERSINE"
            route_fallback = True
    else:
        distance_km = _haversine_distance_km(origin_lat, origin_lng, destination_lat, destination_lng)
        route_provider_used = "HAVERSINE"
        route_fallback = False

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
        geocoding_provider_used=origin_provider or destination_provider,
        routing_provider_used=route_provider_used,
        fallback_used=origin_fallback or destination_fallback or route_fallback,
    )


def calculate_distance_batch(
    pairs: list[tuple[str, str]],
    mode: DistanceMode,
    geocoding_provider: GeocodingProvider = GeocodingProvider.AUTO,
    routing_provider: RoutingProvider = RoutingProvider.AUTO,
) -> list[DistanceResult]:
    results: list[DistanceResult] = []
    for origin, destination in pairs:
        results.append(
            calculate_distance_single(
                origin,
                destination,
                mode,
                geocoding_provider=geocoding_provider,
                routing_provider=routing_provider,
            )
        )
    return results


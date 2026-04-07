#!/usr/bin/env python
"""Test script to verify IBGE integration."""

import sys
import os

# Add app directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from db.repository import (
    init_db,
    create_distance_request,
    save_distance_result,
    get_city_from_ibge_code,
    add_or_update_ibge_city,
)
from services.distance_service import DistanceMode, GeocodingProvider, RoutingProvider


def test_database_initialization():
    """Test database schema creation."""
    print("Testing database initialization...")
    try:
        init_db()
        print("✓ Database initialized successfully")
        return True
    except Exception as exc:
        print(f"✗ Database initialization failed: {exc}")
        return False


def test_ibge_function():
    """Test IBGE city operations."""
    print("\nTesting IBGE functions...")
    try:
        # Add a test city
        city = add_or_update_ibge_city(
            ibge_code="3550308",  # São Paulo
            city_name="São Paulo",
            state_code="SP",
            latitude=-23.5505,
            longitude=-46.6333,
        )
        print(f"✓ Added IBGE city: {city.city_name}")
        
        # Retrieve it
        retrieved = get_city_from_ibge_code("3550308")
        if retrieved and retrieved["city_name"] == "São Paulo":
            print(f"✓ Retrieved IBGE city: {retrieved['city_name']}, {retrieved['state_code']}")
            return True
        else:
            print("✗ Failed to retrieve IBGE city")
            return False
    except Exception as exc:
        print(f"✗ IBGE function test failed: {exc}")
        return False


def test_distance_request_with_ibge():
    """Test creating distance request with IBGE codes."""
    print("\nTesting distance request with IBGE codes...")
    try:
        request = create_distance_request(
            filename="test.csv",
            mode=DistanceMode.ROUTE,
            total_rows=1,
            ibge_codigo_origem="3550308",
            ibge_codigo_destino="3509502",
        )
        print(f"✓ Created distance request #{request.id} with IBGE codes")
        return True
    except Exception as exc:
        print(f"✗ Distance request test failed: {exc}")
        return False


def test_distance_result_with_ibge():
    """Test saving distance result with IBGE codes."""
    print("\nTesting distance result with IBGE codes...")
    try:
        result = save_distance_result(
            request_id=None,
            origin_raw="Avenida Paulista, São Paulo, SP",
            destination_raw="Rua Oscar Freire, São Paulo, SP",
            origin_lat=-23.5505,
            origin_lng=-46.6333,
            destination_lat=-23.5597,
            destination_lng=-46.6756,
            distance_km=5.5,
            mode=DistanceMode.ROUTE,
            geocoding_provider=GeocodingProvider.AUTO,
            routing_provider=RoutingProvider.AUTO,
            status="ok",
            error_message=None,
            geocoding_provider_used="NOMINATIM",
            routing_provider_used="OSRM",
            fallback_used=False,
            origin_ibge_code="3550308",
            destination_ibge_code="3550308",
        )
        print(f"✓ Saved distance result #{result.id} with IBGE codes")
        print(f"  - Origin IBGE: {result.origin_ibge_code}")
        print(f"  - Destination IBGE: {result.destination_ibge_code}")
        return True
    except Exception as exc:
        print(f"✗ Distance result test failed: {exc}")
        return False


def main():
    """Run all tests."""
    print("=" * 60)
    print("IBGE Integration Test Suite")
    print("=" * 60)
    
    results = []
    results.append(test_database_initialization())
    results.append(test_ibge_function())
    results.append(test_distance_request_with_ibge())
    results.append(test_distance_result_with_ibge())
    
    print("\n" + "=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Results: {passed}/{total} tests passed")
    print("=" * 60)
    
    return all(results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

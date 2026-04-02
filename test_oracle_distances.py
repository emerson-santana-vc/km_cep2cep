#!/usr/bin/env python3
"""Test Oracle search with distance processing."""

import sys
import os

# Add app directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from datetime import date, timedelta
import pandas as pd

from services.oracle_service import (
    load_oracle_config,
    execute_oracle_query,
    extract_sql_bind_names,
)
from services.distance_service import (
    DistanceMode,
    GeocodingProvider,
    RoutingProvider,
    calculate_distance_single,
)
from db.repository import (
    get_cached_distance,
    create_distance_request,
    save_distance_result,
    update_distance_request_progress,
)

def test_oracle_with_distance():
    """Test full pipeline: Oracle query + distance calculation."""
    print("=" * 80)
    print("TESTE END-TO-END: ORACLE QUERY + DISTANCE PROCESSING")
    print("=" * 80)
    
    # Step 1: Load Oracle config
    print("\n[1] Carregando configuração Oracle...")
    try:
        config = load_oracle_config()
        print(f"    ✓ Conectado: {config.host}:{config.port}")
    except Exception as e:
        print(f"    ✗ Erro: {e}")
        return False
    
    # Step 2: Execute query (small subset for testing)
    print("\n[2] Executando consulta Oracle (últimos 3 dias)...")
    today = date.today()
    bind_params = {
        "data_inicio": (today - timedelta(days=3)).isoformat(),
        "data_fim": today.isoformat(),
        "uf": None,
        "codigo_filial": None,
        "codigo_cliente": None,
        "cidade_origem": None,
        "cidade_destino": None,
    }
    
    try:
        rows = execute_oracle_query(config.sql_query, bind_params)
        print(f"    ✓ Registros retornados: {len(rows)}")
        if not rows:
            print("    ⚠ Nenhum registro no período. Expandindo para 30 dias...")
            bind_params["data_inicio"] = (today - timedelta(days=30)).isoformat()
            rows = execute_oracle_query(config.sql_query, bind_params)
            print(f"    ✓ Registros retornados: {len(rows)}")
    except Exception as e:
        print(f"    ✗ Erro na query: {e}")
        return False
    
    result_df = pd.DataFrame(rows)
    
    # Step 3: Configuration for distance processing
    print("\n[3] Configurando processamento de distâncias...")
    mode = DistanceMode.ROUTE
    geocoding_provider = GeocodingProvider.GOOGLE
    routing_provider = RoutingProvider.OSRM
    
    print(f"    ✓ Modo: {mode.value}")
    print(f"    ✓ Geocoding: {geocoding_provider.value}")
    print(f"    ✓ Rota: {routing_provider.value}")
    
    # Step 4: Create distance request
    print(f"\n[4] Criando requisição de lote (teste com 5 registros)...")
    
    # Limit to 5 rows for quick testing
    test_df = result_df.head(5).reset_index(drop=True)
    request = create_distance_request(
        filename="oracle_test",
        mode=mode,
        total_rows=len(test_df),
    )
    print(f"    ✓ Request ID: {request.id}")
    
    # Step 5: Process distances
    print(f"\n[5] Processando {len(test_df)} registros...")
    
    distances = []
    statuses = []
    error_messages = []
    
    for idx, row in test_df.iterrows():
        try:
            # Build complete addresses
            origin = f"{row.get('endereco_origem', '')} {row.get('cidade_origem', '')} {row.get('uf', '')}"
            destination = f"{row.get('endereco_destino', '')} {row.get('cidade_destino', '')} {row.get('uf', '')}"
            
            print(f"\n    [{idx + 1}] Processando...")
            print(f"        Origem: {origin[:50]}...")
            print(f"        Destino: {destination[:50]}...")
            
            # Check cache
            cached = get_cached_distance(
                origin,
                destination,
                mode,
                geocoding_provider,
                routing_provider,
            )
            
            if cached is not None:
                distance_km = cached
                status = "ok (cached)"
                error_message = None
                print(f"        ✓ Cache hit: {distance_km} km")
            else:
                # Calculate distance
                result = calculate_distance_single(
                    origin,
                    destination,
                    mode,
                    geocoding_provider=geocoding_provider,
                    routing_provider=routing_provider,
                )
                
                distance_km = result.distance_km
                status = result.status
                error_message = result.error_message
                
                print(f"        ✓ Calculado: {distance_km} km ({status})")
                if error_message:
                    print(f"        ⚠ Aviso: {error_message}")
                
                # Save to database
                try:
                    save_distance_result(
                        request_id=request.id,
                        origin_raw=origin,
                        destination_raw=destination,
                        origin_lat=result.origin_lat,
                        origin_lng=result.origin_lng,
                        destination_lat=result.destination_lat,
                        destination_lng=result.destination_lng,
                        distance_km=result.distance_km,
                        mode=mode,
                        geocoding_provider=geocoding_provider,
                        routing_provider=routing_provider,
                        status=result.status,
                        error_message=result.error_message,
                        geocoding_provider_used=result.geocoding_provider_used,
                        routing_provider_used=result.routing_provider_used,
                        fallback_used=result.fallback_used,
                    )
                    print(f"        ✓ Salvo no banco de dados")
                except Exception as db_err:
                    print(f"        ⚠ Erro ao salvar: {db_err}")
            
            distances.append(distance_km)
            statuses.append(status)
            error_messages.append(error_message)
            
            # Update progress
            update_distance_request_progress(
                request_id=request.id,
                processed_rows=idx + 1,
                status="processing" if idx + 1 < len(test_df) else "finished",
            )
        
        except Exception as e:
            print(f"        ✗ Erro: {e}")
            distances.append(None)
            statuses.append("error")
            error_messages.append(str(e))
    
    # Step 6: Prepare results
    print(f"\n[6] Compilando resultados...")
    test_df["distance_km"] = distances
    test_df["status"] = statuses
    test_df["error_message"] = error_messages
    
    # Step 7: Export results
    print(f"\n[7] Exportando resultados...")
    
    csv_path = "test_oracle_distances.csv"
    test_df.to_csv(csv_path, index=False)
    
    successful = sum(1 for s in statuses if s and s.startswith("ok"))
    print(f"    ✓ Arquivo criado: {csv_path}")
    print(f"    ✓ Sucesso: {successful}/{len(test_df)}")
    print(f"    ✓ Taxa: {(successful/len(test_df)*100):.1f}%")
    
    # Summary statistics
    print(f"\n[8] Estatísticas:")
    valid_distances = [d for d in distances if d is not None and d > 0]
    if valid_distances:
        print(f"    ✓ Distâncias válidas: {len(valid_distances)}")
        print(f"    ✓ Mínima: {min(valid_distances):.2f} km")
        print(f"    ✓ Máxima: {max(valid_distances):.2f} km")
        print(f"    ✓ Média: {sum(valid_distances)/len(valid_distances):.2f} km")
    
    print(f"\n    Primeiras 3 linhas dos resultados:")
    print(test_df[["endereco_origem", "cidade_origem", "cidade_destino", "distance_km", "status"]].head(3).to_string())
    
    return True

if __name__ == "__main__":
    success = test_oracle_with_distance()
    print("\n" + "=" * 80)
    if success:
        print("✓ TESTE CONCLUÍDO COM SUCESSO")
    else:
        print("✗ TESTE FALHOU")
    print("=" * 80)
    sys.exit(0 if success else 1)

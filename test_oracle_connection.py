#!/usr/bin/env python3
"""Quick test for Oracle connectivity and query execution."""

import sys
from datetime import datetime, timedelta
from app.services.oracle_service import load_oracle_config, execute_oracle_query, extract_sql_bind_names

def test_oracle_connection():
    """Test Oracle database connection and query execution."""
    print("=" * 70)
    print("TESTE DE CONECTIVIDADE ORACLE")
    print("=" * 70)
    
    # Step 1: Load configuration
    print("\n[1] Carregando configuração Oracle...")
    try:
        config = load_oracle_config()
        print(f"    ✓ Host: {config.host}")
        print(f"    ✓ Port: {config.port}")
        print(f"    ✓ Service: {config.service_name}")
        print(f"    ✓ User: {config.user}")
        print(f"    ✓ Senha carregada: {'SIM' if config.password else 'NÃO'}")
        print(f"    ✓ SQL Query carregada: {len(config.sql_query)} caracteres")
    except Exception as e:
        print(f"    ✗ ERRO ao carregar config: {e}")
        return False
    
    # Step 2: Extract bind names
    print("\n[2] Analisando bind names na query...")
    try:
        bind_names = extract_sql_bind_names(config.sql_query)
        print(f"    ✓ Binds detectados: {bind_names}")
    except Exception as e:
        print(f"    ✗ ERRO ao extrair binds: {e}")
        return False
    
    # Step 3: Prepare test parameters
    print("\n[3] Preparando parâmetros de teste...")
    today = datetime.now()
    thirty_days_ago = today - timedelta(days=30)
    
    test_params = {
        'data_inicio': thirty_days_ago.strftime('%Y-%m-%d'),
        'data_fim': today.strftime('%Y-%m-%d'),
        'uf': None,
        'codigo_filial': None,
        'codigo_cliente': None,
        'cidade_origem': None,
        'cidade_destino': None,
    }
    
    print(f"    ✓ Data início: {test_params['data_inicio']}")
    print(f"    ✓ Data fim: {test_params['data_fim']}")
    print("    ✓ Filtros gerais: DESATIVADOS (None)")
    
    # Step 4: Execute query
    print("\n[4] Executando query no Oracle...")
    try:
        results = execute_oracle_query(config.sql_query, test_params)
        print(f"    ✓ Conexão bem-sucedida!")
        print(f"    ✓ Linhas retornadas: {len(results)}")
        
        if results:
            print(f"\n    Colunas: {list(results[0].keys())}")
            print(f"\n    Primeiras 3 linhas:")
            for i, row in enumerate(results[:3], 1):
                print(f"      {i}. {row}")
        else:
            print("    ⚠ Nenhuma linha retornada (pode ser normal se sem dados no período)")
        
        return True
    
    except Exception as e:
        print(f"    ✗ ERRO na execução: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_oracle_connection()
    print("\n" + "=" * 70)
    if success:
        print("✓ TESTE CONCLUÍDO COM SUCESSO")
    else:
        print("✗ TESTE FALHOU - Veja erros acima")
    print("=" * 70)
    sys.exit(0 if success else 1)

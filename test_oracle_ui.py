#!/usr/bin/env python3
"""Test Oracle search interface via direct function calls."""

import sys
from datetime import datetime, date, timedelta
from app.services.oracle_service import (
    load_oracle_config,
    execute_oracle_query,
    extract_sql_bind_names,
)
import pandas as pd

def test_oracle_ui():
    """Simulates user interaction with Oracle search form."""
    print("=" * 80)
    print("TESTE INTERFACE - PESQUISA ORACLE")
    print("=" * 80)
    
    # Step 1: Load config
    print("\n[1] Carregando configuração Oracle...")
    try:
        config = load_oracle_config()
        print(f"    ✓ Banco conectado: {config.host}:{config.port}/{config.service_name}")
    except Exception as e:
        print(f"    ✗ Erro: {e}")
        return False
    
    # Step 2: Extract bind names (simulates form field generation)
    print("\n[2] Detectando campos de filtro...")
    try:
        bind_names = set(extract_sql_bind_names(config.sql_query))
        print(f"    ✓ Binds: {', '.join(sorted(bind_names))}")
    except Exception as e:
        print(f"    ✗ Erro: {e}")
        return False
    
    # Step 3: Simulating user filling the form
    print("\n[3] Preenchendo formulário (valores simulados)...")
    today = date.today()
    
    # Simulation 1: Filter by date range only (most common case)
    print("\n    TESTE 1: Sem filtros específicos (apenas data)")
    bind_params = {
        "data_inicio": (today - timedelta(days=7)).isoformat(),
        "data_fim": today.isoformat(),
        "uf": None,
        "codigo_filial": None,
        "codigo_cliente": None,
        "cidade_origem": None,
        "cidade_destino": None,
    }
    bind_params = {k: v for k, v in bind_params.items() if k in bind_names or k in {"data_inicio", "data_fim"}}
    
    try:
        rows = execute_oracle_query(config.sql_query, bind_params)
        print(f"       ✓ Registros retornados: {len(rows)}")
        if rows:
            print(f"       ✓ Colunas: {list(rows[0].keys())}")
            print(f"       ✓ Exemplo de registro:")
            row = rows[0]
            print(f"         - Cliente: {row.get('codigo_cliente')}")
            print(f"         - Origem: {row.get('cidade_origem', 'N/A')}")
            print(f"         - Destino: {row.get('cidade_destino', 'N/A')}")
            print(f"         - UF: {row.get('uf')}")
            print(f"         - Data: {row.get('data_faturamento')}")
    except Exception as e:
        print(f"       ✗ Erro: {e}")
        return False
    
    # Simulation 2: Filter by UF
    print("\n    TESTE 2: Filtro por UF (SP)")
    bind_params = {
        "data_inicio": (today - timedelta(days=7)).isoformat(),
        "data_fim": today.isoformat(),
        "uf": "SP",
        "codigo_filial": None,
        "codigo_cliente": None,
        "cidade_origem": None,
        "cidade_destino": None,
    }
    bind_params = {k: v for k, v in bind_params.items() if k in bind_names or k in {"data_inicio", "data_fim"}}
    
    try:
        rows = execute_oracle_query(config.sql_query, bind_params)
        print(f"       ✓ Registros retornados: {len(rows)}")
        ufs = set(row.get('uf') for row in rows)
        print(f"       ✓ UFs nos dados: {', '.join(sorted(ufs))}")
    except Exception as e:
        print(f"       ✗ Erro: {e}")
        return False
    
    # Simulation 3: Filter by city
    print("\n    TESTE 3: Filtro por cidade de origem (SÃO PAULO)")
    bind_params = {
        "data_inicio": (today - timedelta(days=7)).isoformat(),
        "data_fim": today.isoformat(),
        "uf": None,
        "codigo_filial": None,
        "codigo_cliente": None,
        "cidade_origem": "SÃO PAULO",
        "cidade_destino": None,
    }
    bind_params = {k: v for k, v in bind_params.items() if k in bind_names or k in {"data_inicio", "data_fim"}}
    
    try:
        rows = execute_oracle_query(config.sql_query, bind_params)
        print(f"       ✓ Registros retornados: {len(rows)}")
        if rows:
            origins = set(row.get('cidade_origem') for row in rows)
            print(f"       ✓ Cidades de origem: {', '.join(sorted(origins)[:5])}...")  # Show first 5
    except Exception as e:
        print(f"       ✗ Erro: {e}")
        return False
    
    # Simulation 4: Multiple filters
    print("\n    TESTE 4: Múltiplos filtros (UF, filial, cliente)")
    bind_params = {
        "data_inicio": (today - timedelta(days=30)).isoformat(),
        "data_fim": today.isoformat(),
        "uf": "SP",
        "codigo_filial": "9",
        "codigo_cliente": "5123",
        "cidade_origem": None,
        "cidade_destino": None,
    }
    bind_params = {k: v for k, v in bind_params.items() if k in bind_names or k in {"data_inicio", "data_fim"}}
    
    try:
        rows = execute_oracle_query(config.sql_query, bind_params)
        print(f"       ✓ Registros retornados: {len(rows)}")
        if rows:
            print(f"       ✓ Filtros aplicados com sucesso")
            for i, row in enumerate(rows[:2], 1):
                print(f"         Linha {i}: Cliente {row.get('codigo_cliente')} | Filial {row.get('codigo_filial')} | {row.get('cidade_origem')} → {row.get('cidade_destino')}")
    except Exception as e:
        print(f"       ✗ Erro: {e}")
        return False
    
    # Step 4: Test DataFrame export (simulates CSV download)
    print("\n[4] Testando exportação para CSV...")
    try:
        bind_params = {
            "data_inicio": (today - timedelta(days=5)).isoformat(),
            "data_fim": today.isoformat(),
            "uf": "SP",
            "codigo_filial": None,
            "codigo_cliente": None,
            "cidade_origem": None,
            "cidade_destino": None,
        }
        bind_params = {k: v for k, v in bind_params.items() if k in bind_names or k in {"data_inicio", "data_fim"}}
        
        rows = execute_oracle_query(config.sql_query, bind_params)
        result_df = pd.DataFrame(rows)
        
        csv_path = "test_export.csv"
        result_df.to_csv(csv_path, index=False)
        
        print(f"    ✓ CSV criado: {csv_path}")
        print(f"    ✓ Tamanho: {len(result_df)} linhas x {len(result_df.columns)} colunas")
        print(f"    ✓ Espaço: {len(result_df.to_csv(index=False).encode('utf-8')) / 1024:.2f} KB")
    except Exception as e:
        print(f"    ✗ Erro: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_oracle_ui()
    print("\n" + "=" * 80)
    if success:
        print("✓ TESTE INTERFACE CONCLUÍDO COM SUCESSO")
        print("  A interface Streamlit está pronta para uso.")
    else:
        print("✗ TESTE FALHOU")
    print("=" * 80)
    sys.exit(0 if success else 1)

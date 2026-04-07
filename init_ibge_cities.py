#!/usr/bin/env python
"""Script to initialize IBGE city codes in the application database."""

import sys
import os

# Add app directory to path so imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from db.repository import bulk_add_ibge_cities
from services.ibge_service import fetch_all_brazilian_cities


def main():
    print("Iniciando importação de cidades IBGE...")
    print("Conectando à API IBGE para buscar dados de cidades...")
    
    cities = fetch_all_brazilian_cities()
    
    if not cities:
        print("Erro: Nenhuma cidade foi obtida da API IBGE.")
        print("Verifique a conexão com a internet e a disponibilidade da API IBGE.")
        return False
    
    print(f"Encontradas {len(cities)} cidades no Brasil.")
    print("Salvando dados no banco de dados...")
    
    try:
        bulk_add_ibge_cities(cities)
        print(f"✓ {len(cities)} cidades importadas com sucesso!")
        return True
    except Exception as exc:
        print(f"Erro ao importar cidades: {exc}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

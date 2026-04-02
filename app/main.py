import os
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import streamlit as st

from services.file_importer import read_uploaded_file
from services.distance_service import (
    DistanceMode,
    GeocodingProvider,
    RoutingProvider,
    calculate_distance_single,
)
from db.repository import (
    init_db,
    create_distance_request,
    update_distance_request_progress,
    save_distance_result,
    get_cached_distance,
)
from services.oracle_service import (
    OracleConfigError,
    OracleQueryError,
    execute_oracle_query,
    extract_sql_bind_names,
    load_oracle_config,
)


st.set_page_config(
    page_title="Distância entre endereços",
    layout="wide",
)


@st.cache_resource
def _init_db_once():
    init_db()


def process_single():
    st.subheader("Consulta única")

    origin = st.text_input("Endereço de origem")
    destination = st.text_input("Endereço de destino")

    mode_label = st.radio(
        "Modo de cálculo",
        options=[DistanceMode.ROUTE.value, DistanceMode.HAVERSINE.value],
        index=0,
    )
    mode = DistanceMode(mode_label)

    geocoding_provider_label = st.selectbox(
        "Provedor de geocodificação",
        options=[provider.value for provider in GeocodingProvider],
        index=0,
    )
    geocoding_provider = GeocodingProvider(geocoding_provider_label)

    routing_provider_label = st.selectbox(
        "Provedor de rota",
        options=[provider.value for provider in RoutingProvider],
        index=0,
    )
    routing_provider = RoutingProvider(routing_provider_label)

    if st.button("Calcular distância"):
        if not origin or not destination:
            st.warning("Informe endereço de origem e destino.")
            return

        with st.spinner("Calculando distância..."):
            # tenta cache primeiro
            cached = get_cached_distance(
                origin,
                destination,
                mode,
                geocoding_provider,
                routing_provider,
            )
            if cached is not None:
                distance_km = cached
            else:
                result = calculate_distance_single(
                    origin,
                    destination,
                    mode,
                    geocoding_provider=geocoding_provider,
                    routing_provider=routing_provider,
                )
                distance_km = result.distance_km
                save_distance_result(
                    request_id=None,
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

        if distance_km is not None:
            st.success(f"Distância: {distance_km:.2f} km")
        else:
            st.error("Não foi possível calcular a distância.")


def process_batch():
    st.subheader("Processamento em lote")

    uploaded_file = st.file_uploader(
        "Selecione arquivo CSV ou XLSX",
        type=["csv", "xlsx"],
    )

    if not uploaded_file:
        st.info("Envie um arquivo para iniciar o processamento.")
        return

    sep = st.selectbox("Separador (apenas para CSV)", [",", ";"], index=0)

    df = read_uploaded_file(uploaded_file, sep=sep)
    if df is None or df.empty:
        st.error("Não foi possível ler o arquivo ou ele está vazio.")
        return

    st.write("Pré-visualização dos dados:")
    st.dataframe(df.head())

    col_origin = st.selectbox("Coluna de endereço de origem", df.columns)
    col_destination = st.selectbox("Coluna de endereço de destino", df.columns)

    mode_label = st.radio(
        "Modo de cálculo",
        options=[DistanceMode.ROUTE.value, DistanceMode.HAVERSINE.value],
        index=0,
        key="batch_mode",
    )
    mode = DistanceMode(mode_label)

    geocoding_provider_label = st.selectbox(
        "Provedor de geocodificação",
        options=[provider.value for provider in GeocodingProvider],
        index=0,
        key="batch_geocoding_provider",
    )
    geocoding_provider = GeocodingProvider(geocoding_provider_label)

    routing_provider_label = st.selectbox(
        "Provedor de rota",
        options=[provider.value for provider in RoutingProvider],
        index=0,
        key="batch_routing_provider",
    )
    routing_provider = RoutingProvider(routing_provider_label)

    if st.button("Processar lote"):
        if col_origin == col_destination:
            st.warning("Colunas de origem e destino devem ser diferentes.")
            return

        with st.spinner("Iniciando processamento..."):
            request = create_distance_request(
                filename=uploaded_file.name,
                mode=mode,
                total_rows=len(df),
            )

        progress_bar = st.progress(0, text="Processando distâncias...")

        results_df = df.copy()
        distances = []
        statuses = []
        error_messages = []

        for idx, row in df.iterrows():
            origin = str(row[col_origin])
            destination = str(row[col_destination])

            cached = get_cached_distance(
                origin,
                destination,
                mode,
                geocoding_provider,
                routing_provider,
            )
            if cached is not None:
                distance_km = cached
                status = "ok"
                error_message: Optional[str] = None
            else:
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

            distances.append(distance_km)
            statuses.append(status)
            error_messages.append(error_message)

            processed = idx + 1
            update_distance_request_progress(
                request_id=request.id,
                processed_rows=processed,
                status="processing" if processed < len(df) else "finished",
            )
            progress_bar.progress(
                int((processed / len(df)) * 100),
                text=f"Processando linha {processed} de {len(df)}...",
            )

        results_df["distance_km"] = distances
        results_df["status"] = statuses
        results_df["error_message"] = error_messages

        st.success("Processamento concluído.")
        st.dataframe(results_df)

        csv_bytes = results_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Baixar resultados em CSV",
            data=csv_bytes,
            file_name=f"distancias_{uploaded_file.name}.csv",
            mime="text/csv",
        )


def process_oracle_search():
    st.subheader("Pesquisa Oracle")
    st.caption("A SQL é lida de variáveis de ambiente para evitar exposição em GitHub.")

    try:
        config = load_oracle_config()
        st.success(f"Configuração Oracle carregada")
    except OracleConfigError as exc:
        st.warning(str(exc))
        st.info(
            "Defina ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE_NAME, ORACLE_USER, ORACLE_PASSWORD e ORACLE_SQL_QUERY no ambiente."
        )
        return

    bind_names = set(extract_sql_bind_names(config.sql_query))
    st.caption(f"Binds detectados na SQL: {', '.join(sorted(bind_names)) if bind_names else 'nenhum'}")

    with st.form("oracle_search_form"):
        col1, col2 = st.columns(2)
        with col1:
            data_inicio = st.date_input("Data inicial", value=date.today() - timedelta(days=30))
            uf = st.text_input("UF", value="")
            codigo_cliente = st.text_input("Código do cliente", value="")
        with col2:
            data_fim = st.date_input("Data final", value=date.today())
            codigo_filial = st.text_input("Código da filial", value="")
            cidade_origem = st.text_input("Cidade de origem", value="")
            cidade_destino = st.text_input("Cidade de destino", value="")

        submitted = st.form_submit_button("Executar consulta Oracle")

    if submitted:
        bind_params = {
            "data_inicio": data_inicio.isoformat(),
            "data_fim": data_fim.isoformat(),
            "uf": uf.strip().upper() or None,
            "codigo_filial": codigo_filial.strip() or None,
            "codigo_cliente": codigo_cliente.strip() or None,
            "cidade_origem": cidade_origem.strip() or None,
            "cidade_destino": cidade_destino.strip() or None,
        }

        bind_params = {key: value for key, value in bind_params.items() if key in bind_names or key in {"data_inicio", "data_fim"}}

        try:
            rows = execute_oracle_query(config.sql_query, bind_params)
            if not rows:
                st.info("A consulta não retornou registros.")
                return

            st.success(f"Consulta concluída com {len(rows)} registro(s).")
            result_df = pd.DataFrame(rows)
            
            # Store in session state to persist across button clicks
            st.session_state.oracle_result_df = result_df
            
            st.dataframe(result_df)

            csv_bytes = result_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Baixar resultado Oracle em CSV",
                data=csv_bytes,
                file_name="consulta_oracle.csv",
                mime="text/csv",
            )
        except (OracleConfigError, OracleQueryError) as exc:
            st.error(str(exc))
            return

    # Section: Calculate distances for Oracle results (only if we have data)
    if hasattr(st.session_state, "oracle_result_df") and st.session_state.oracle_result_df is not None:
        result_df = st.session_state.oracle_result_df
        
        st.divider()
        st.subheader("Processamento de Geolocalização e Rota")
        st.caption(f"Processarão {len(result_df)} registros da consulta Oracle")
        
        col1, col2 = st.columns(2)
        with col1:
            mode_label = st.radio(
                "Modo de cálculo",
                options=[DistanceMode.ROUTE.value, DistanceMode.HAVERSINE.value],
                key="oracle_mode",
            )
            mode = DistanceMode(mode_label)
            
            geocoding_provider_label = st.selectbox(
                "Provedor de geocodificação",
                options=[provider.value for provider in GeocodingProvider],
                index=0,
                key="oracle_geocoding_provider",
            )
            geocoding_provider = GeocodingProvider(geocoding_provider_label)
        
        with col2:
            routing_provider_label = st.selectbox(
                "Provedor de rota",
                options=[provider.value for provider in RoutingProvider],
                index=0,
                key="oracle_routing_provider",
            )
            routing_provider = RoutingProvider(routing_provider_label)

        if st.button("Processar Distâncias", key="oracle_process_distances"):
            with st.spinner("Iniciando processamento de distâncias..."):
                request = create_distance_request(
                    filename="consulta_oracle",
                    mode=mode,
                    total_rows=len(result_df),
                )

            progress_bar = st.progress(0, text="Processando distâncias...")

            distances = []
            statuses = []
            error_messages = []
            origins_lat = []
            origins_lng = []
            destinations_lat = []
            destinations_lng = []

            for idx, row in result_df.iterrows():
                # Build complete addresses from Oracle fields
                origin = f"{row.get('endereco_origem', '')} {row.get('cidade_origem', '')} {row.get('uf', '')}"
                destination = f"{row.get('endereco_destino', '')} {row.get('cidade_destino', '')} {row.get('uf', '')}"

                cached = get_cached_distance(
                    origin,
                    destination,
                    mode,
                    geocoding_provider,
                    routing_provider,
                )
                if cached is not None:
                    distance_km = cached
                    status = "ok"
                    error_message: Optional[str] = None
                    origin_lat = None
                    origin_lng = None
                    destination_lat = None
                    destination_lng = None
                else:
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
                    origin_lat = result.origin_lat
                    origin_lng = result.origin_lng
                    destination_lat = result.destination_lat
                    destination_lng = result.destination_lng

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

                distances.append(distance_km)
                statuses.append(status)
                error_messages.append(error_message)
                origins_lat.append(origin_lat)
                origins_lng.append(origin_lng)
                destinations_lat.append(destination_lat)
                destinations_lng.append(destination_lng)

                processed = idx + 1
                update_distance_request_progress(
                    request_id=request.id,
                    processed_rows=processed,
                    status="processing" if processed < len(result_df) else "finished",
                )
                progress_bar.progress(
                    int((processed / len(result_df)) * 100),
                    text=f"Processando linha {processed} de {len(result_df)}...",
                )

            result_df["distance_km"] = distances
            result_df["status"] = statuses
            result_df["error_message"] = error_messages
            result_df["origin_lat"] = origins_lat
            result_df["origin_lng"] = origins_lng
            result_df["destination_lat"] = destinations_lat
            result_df["destination_lng"] = destinations_lng

            st.success("Processamento concluído.")
            st.dataframe(result_df)

            csv_bytes = result_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Baixar resultados com distâncias em CSV",
                data=csv_bytes,
                file_name="consulta_oracle_com_distancias.csv",
                mime="text/csv",
            )


def main():
    _init_db_once()

    st.title("Cálculo de distância entre endereços")

    tab_single, tab_batch, tab_oracle = st.tabs(["Consulta única", "Processamento em lote", "Pesquisa Oracle"])

    with tab_single:
        process_single()
    with tab_batch:
        process_batch()
    with tab_oracle:
        process_oracle_search()


if __name__ == "__main__":
    main()


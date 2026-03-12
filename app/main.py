import os
from typing import Optional

import streamlit as st

from services.file_importer import read_uploaded_file
from services.distance_service import DistanceMode, calculate_distance_single
from db.repository import (
    init_db,
    create_distance_request,
    update_distance_request_progress,
    save_distance_result,
    get_cached_distance,
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

    if st.button("Calcular distância"):
        if not origin or not destination:
            st.warning("Informe endereço de origem e destino.")
            return

        with st.spinner("Calculando distância..."):
            # tenta cache primeiro
            cached = get_cached_distance(origin, destination, mode)
            if cached is not None:
                distance_km = cached
            else:
                result = calculate_distance_single(origin, destination, mode)
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
                    status=result.status,
                    error_message=result.error_message,
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

            cached = get_cached_distance(origin, destination, mode)
            if cached is not None:
                distance_km = cached
                status = "ok"
                error_message: Optional[str] = None
            else:
                result = calculate_distance_single(origin, destination, mode)
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
                    status=result.status,
                    error_message=result.error_message,
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


def main():
    _init_db_once()

    st.title("Cálculo de distância entre endereços")

    tab_single, tab_batch = st.tabs(["Consulta única", "Processamento em lote"])

    with tab_single:
        process_single()
    with tab_batch:
        process_batch()


if __name__ == "__main__":
    main()


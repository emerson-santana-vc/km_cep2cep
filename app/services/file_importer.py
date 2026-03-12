from typing import Optional

import pandas as pd
import streamlit as st


def read_uploaded_file(uploaded_file, sep: str = ",") -> Optional[pd.DataFrame]:
    try:
        name = uploaded_file.name.lower()

        if name.endswith(".csv"):
            df = pd.read_csv(uploaded_file, sep=sep)
        elif name.endswith(".xlsx"):
            df = pd.read_excel(uploaded_file, engine="openpyxl")
        else:
            st.error("Formato de arquivo não suportado. Use CSV ou XLSX.")
            return None

        if df.empty:
            st.error("O arquivo está vazio.")
            return None

        df.columns = [str(col).strip() for col in df.columns]
        return df
    except Exception as exc:
        st.error(f"Erro ao ler o arquivo: {exc}")
        return None


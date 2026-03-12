from io import BytesIO
from typing import Optional

import pandas as pd
from streamlit.runtime.uploaded_file_manager import UploadedFile


def read_uploaded_file(uploaded_file: UploadedFile, sep: str = ",") -> Optional[pd.DataFrame]:
    try:
        name = uploaded_file.name.lower()
        data: BytesIO = uploaded_file.read()
        buffer = BytesIO(data)

        if name.endswith(".csv"):
            df = pd.read_csv(buffer, sep=sep)
        elif name.endswith(".xlsx"):
            df = pd.read_excel(buffer)
        else:
            return None

        df.columns = [str(col).strip() for col in df.columns]
        return df
    except Exception:
        return None


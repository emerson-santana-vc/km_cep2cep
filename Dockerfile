FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY Procfile ./Procfile

ENV PORT=8000

EXPOSE 8000

# Railway define a variável de ambiente PORT em runtime.
# Usamos $PORT (e não ${PORT}) para garantir que o shell faça a expansão corretamente.
CMD ["sh", "-c", "streamlit run app/main.py --server.port=$PORT --server.address=0.0.0.0"]


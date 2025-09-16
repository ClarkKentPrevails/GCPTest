FROM python:3.10-slim

# Install system dependencies for newspaper3k & lxml
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    python3-dev \
    libxml2-dev \
    libxslt1-dev \
    libjpeg-dev \
    zlib1g-dev \
    libffi-dev \
    wget \
    libssl-dev \
    poppler-utils \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Create the custom NLTK data directory
RUN mkdir -p /opt/nltk_data

# Download NLTK data into the custom directory at build time (optional)
RUN python -m nltk.downloader punkt -d /opt/nifi/nltk_data

COPY . .

# Use Gunicorn for production
CMD ["gunicorn", "-b", "0.0.0.0:8080", "main:app"]
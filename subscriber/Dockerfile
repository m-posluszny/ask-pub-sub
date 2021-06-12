#używam obrazu pythona wersji 3
FROM python:3
WORKDIR /usr/src/app

#kopiuje wymagania z folderu i instaluje je
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

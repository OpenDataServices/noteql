FROM python:3
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN apt-get update
RUN apt-get install -y vim git
RUN pip install --no-cache-dir -r requirements.txt
RUN wget https://ftp.postgresql.org/pub/pgadmin/pgadmin4/v2.0/pip/pgadmin4-2.0-py2.py3-none-any.whl
RUN pip install ./pgadmin4-2.0-py2.py3-none-any.whl
COPY config_local.py /usr/local/lib/python3.6/site-packages/pgadmin4/
COPY . .

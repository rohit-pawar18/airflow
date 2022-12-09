FROM apache/airflow:2.4.3
USER 0
RUN apt-get -y update
RUN apt-get -y install git
COPY requirements.txt /requirements.txt
USER airflow
RUN pip install --user --upgrade pip
RUN pip3 install --no-cache-dir --user -r /requirements.txt
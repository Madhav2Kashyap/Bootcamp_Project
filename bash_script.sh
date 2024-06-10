#!/bin/bash

sudo apt-get update
sudo apt-get install postgresql postgresql-contrib
sudo service postgresql status
sudo -i -u postgres
psql
CREATE USER myuser WITH PASSWORD 'mypassword';
ALTER USER myuser CREATEDB;
GRANT CREATE ON SCHEMA public TO myuser;
\q
exit;


sudo apt update -y
sudo apt install -y build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev python3-openssl git


cd /tmp
wget https://www.python.org/ftp/python/3.9.10/Python-3.9.10.tgz
tar xzf Python-3.9.10.tgz
cd Python-3.9.10
./configure --enable-optimizations
make -j 8
sudo make altinstall


/usr/local/bin/python3.9 -m venv ~/airflow_venv

source ~/airflow_venv/bin/activate

pip install --upgrade pip

export AIRFLOW_HOME=~/airflow
export SLUGIFY_USES_TEXT_UNIDECODE=yes

pip install apache-airflow==2.9.1


airflow db init

airflow users create \
    --role Admin \
    --username madhav \
    --password 1234 \
    --email userworkhere@gmail.com \
    --firstname madhav \
    --lastname kashyap
    
pip install apache-airflow
pip install pyspark
pip install boto3
pip install apache-airflow-providers-amazon

FROM ubuntu:16.04
COPY . /app
WORKDIR /app
RUN apt-get update && apt-get -y install python-pip python-lxml python-scipy python-numpy liblzma-dev libxml2-dev libxslt-dev python-setuptools python-dev build-essential 
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN ./test.py
ENTRYPOINT ["python", "/app/run_home_scorer.py"]
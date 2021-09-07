FROM python:3.7.3-stretch
LABEL maintainer="SAUMYA PALLAV<saumyapallav@gmail.com>"
WORKDIR /usr/src/app

COPY Pubsub_Publishing.py .
COPY requirements.txt .
COPY egen-project-dev-private.json .
COPY twitter-api-keys.json .
ENV PORT 8080
RUN pip install -r requirements.txt
CMD ["python", "./Pubsub_Publishing.py"]

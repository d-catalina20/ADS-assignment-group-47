# buster version didn't wor, the apt-get update command was failing
# replaced with bullseye
FROM python:3.9-slim-bullseye
RUN apt-get update
#RUN apt-get install -y "any package you like"
RUN pip3 install rpyc redis numpy matplotlib
WORKDIR /app
CMD [ "/bin/bash", "-c", "while true; do bash -l; done" ]

FROM python:3.9-slim-buster

# Do not cache Python packages
ENV PIP_NO_CACHE_DIR=YES

# keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# set PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/code/"

# Initializing new working directory
WORKDIR /code

#Transferring code and data
COPY src ./src
COPY Pipfile ./Pipfile
COPY Pipfile.lock ./Pipfile.lock

RUN pip install pipenv
RUN pipenv install --ignore-pipfile --system
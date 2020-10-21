# Kafka Setup

 0. Follow this: [Confluent Kafka Docker Quickstart Guide](https://docs.confluent.io/current/quickstart/ce-docker-quickstart.html#ce-docker-quickstart)
 <-- Put the repository into the EXTERNAL directory
 1. copy .env.example to .env and change necessary variables
 2. install dependencies (`poetry install` from root directory)
 3. run setup.py (`poetry run python kafka_setup/setup.py`)
 4. if you need to clean up topics (`poetry run python kafka_setup/teardown.py`) 
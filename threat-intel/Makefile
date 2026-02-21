VENV_ACT = . .venv/bin/activate

up:
	docker compose up -d

down:
	docker compose down

topic-reset:
	- docker compose exec kafka kafka-topics --delete --topic domain-events --bootstrap-server kafka:9092
	docker compose exec kafka kafka-topics --create --topic domain-events --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1
	docker compose exec kafka kafka-topics --list --bootstrap-server kafka:9092

produce:
	$(VENV_ACT) && python simulator/producer.py

stream:
	$(VENV_ACT) && python pipelines/spark_stream.py

stream-fast:
	$(VENV_ACT) && SPARK_TRIGGER_SECS=2 python pipelines/spark_stream.py

stream-fresh:
	rm -rf bronze dlq chk
	$(VENV_ACT) && SPARK_CHECKPOINT_DIR=./chk/dev python pipelines/spark_stream.py

test:
	$(VENV_ACT) && pytest -q

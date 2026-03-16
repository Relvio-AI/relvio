.PHONY: install run reset-db

install:
	pip install -r requirements.txt
	cp -n .env.example .env

run:
	flask run --port=$${PORT:-5000}

reset-db:
	rm -f crm.db

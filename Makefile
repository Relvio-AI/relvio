SHELL := /bin/bash
.PHONY: install run reset-db

install:
	python3 -m pip install -r requirements.txt
	cp -n .env.example .env

run:
	python3 app.py

reset-db:
	rm -f crm.db

init:
    pip install -r requirements.txt

test:
    python -m unittest discover -v

.PHONY: init test
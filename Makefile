
clean:
	rm -rf test_outputs
	rm -f *.pyc
	rm -f pjrdb/*.pyc
	rm -f test/*.pyc

test:
	python -m unittest test.test_csv_format
	python -m unittest test.test_fetcher
	python -m unittest test.test_parse

.PHONY: clean test

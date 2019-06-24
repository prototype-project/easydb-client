
build_package:
	rm -rf build dist easydb*.egg*
	python3 setup.py sdist bdist_wheel


upload_package:
	python -m twine upload dist/*


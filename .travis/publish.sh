if [ "$TRAVIS_BRANCH" = "master" ]; then
    python setup.py sdist bdist_wheel
    python -m twine upload dist/*
fi
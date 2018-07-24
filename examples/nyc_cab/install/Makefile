
build:
	docker build -t docker-apac:6081/stem-pres-nyc-cab:latest -f Dockerfile --no-cache .

push:
	docker push docker-apac:6081/stem-pres-nyc-cab:latest

env:
	conda create --name stem_nyc_cab python=3.6 || true
	pip install -r ./requirements.txt
	(source activate stem_nyc_cab ; python -m ipykernel install --user --name stem_nyc_cab --display-name "Python (stem_nyc_cab)")

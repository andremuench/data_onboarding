IMAGE := postgres:12
CNT := metadb

run:
	docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=admin123 --name $(CNT) $(IMAGE)

rm: 
	docker rm -f $(CNT)

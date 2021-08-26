docker-compose -f ..\\infrastructure\\docker-compose.yml down
docker-compose -f ..\\infrastructure\\docker-compose.yml up -d --build
docker logs -f infrastructure_postgres_1
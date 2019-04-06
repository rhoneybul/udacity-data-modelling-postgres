docker run -p 5432:5432 \
       -e POSTGRES_USER=student \
       -e POSTGRES_DB=studentdb \
       -e POSTGRES_PASSWORD=student \
       --name udacity-project-1-db \
       -d postgres:9.6
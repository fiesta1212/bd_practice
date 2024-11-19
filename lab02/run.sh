unzip ml-100k.zip

docker build -t lab02 .
docker run lab02 > out.txt
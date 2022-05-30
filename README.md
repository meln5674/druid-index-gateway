# Druid Ingest Gateway

This server acts as a gateway for ingesting files into Apache Druid. While Druid is great at ingesting huge data sets from production-grade datasources like S3 or SQL databases, its less good at doing ad-hoc insertions. This server solves this issue.

## Concept

Druid core comes with an HTTP data source, which allows ingesting files from a series of HTTP uris. This server offers an endpoint to accept a set of files in a multipart upload, and another to retrive those files. After submitting files to the multipart upload endpoint, which also includes a template index task specification, the server will mutate that index task to point to itself, and submit it to druid. The Druid middleManagers/indexers will then reach back out to this server and retrive the uploaded files. This server will return a the response from druid as a response to the upload endpoint. Because these files are only needed while the index task is running, they will be automatically cleaned up after a configurable time-out, as well as a endpoint to clean up manually, which can be called by the originall caller once the druid index task finishes.

## Building

```bash
go build -o gateway main.go
# or
docker build -t docker-index-gateway .
```

## Running

```bash
# see ./gateway --help for options
./gateway [flags...]
# or
docker run -p 8080:8080 docker-index-gateway [flags...]
```

## Run Tests

```bash
# Requires java 8
go test -v integration_test.go
```

## Submitting Tasks

```bash
curl <your gateway host>/tasks/task \
    -X POST \
    -F spec.json=@<path to your index spec> \
    -F <filename1>=@<path to first file to ingest> \
    -F <filename2>=@<path to second file to ingest> \
    ...
```

If a response is successfully submitted, the response will the same as the druid index endpoint, and you can track the task via the Druid API as usual

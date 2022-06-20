# Rust Object Store

A crate providing a generic interface to object stores, such as S3, Azure Blob Storage and Google Cloud Storage. Originally developed for [InfluxDB IOx](https://github.com/influxdata/influxdb_iox/) and later split out for external consumption.

See [docs.rs](https://docs.rs/object_store) for usage instructions

## Testing (AWS-SDK)

Tests are run with [MinIO](https://min.io/) which provides a containerized implementation of the Amazon S3 API.

Then start the MinIO container:

```bash
docker run \
--detach \
--rm \
--publish 9000:9000 \
--publish 9001:9001 \
--name minio \
--volume "$(pwd)/testdata:/data" \
--env "MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE" \
--env "MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
quay.io/minio/minio server /data \
--console-address ":9001"
```

Once started, run tests in normal fashion:

```bash
cargo test --features=awssdk
```


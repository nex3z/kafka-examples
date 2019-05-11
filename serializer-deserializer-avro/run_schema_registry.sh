#!/usr/bin/env bash
docker run \
    -p 8081:8081 \
    -e SCHEMA_REGISTRY_HOST_NAME=schema-registry \
    -e SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL=localhost:2181 \
    --name schema-registry \
    confluentinc/cp-schema-registry

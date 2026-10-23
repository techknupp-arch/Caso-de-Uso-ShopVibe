FROM python:3.12-alpine AS plugin
WORKDIR /build
COPY download_plugin.py .
RUN python download_plugin.py

# Match the Debezium image already verified in this project.
#FROM quay.io/debezium/connect@sha256:b811d23b084182a6a1c7a542495155a670f8353551bff220841be4feb6bda4e9
FROM quay.io/debezium/connect:2.7.4.Final@sha256:5a62d0fb204c3ff3ba66f9e402ec8d628e8b05d527b06185764c72e28dfb1d04
COPY --from=plugin /plugins/ /kafka/connect/

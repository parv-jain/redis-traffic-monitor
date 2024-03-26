# Use the official node image as base
FROM node:20-alpine3.18
RUN apk add python3
RUN apk add --no-cache --virtual .gyp \
        make \
        curl \
        g++ \
        libpcap-dev

# Set the working directory inside the container
WORKDIR /redis-query-analyzer

# Copy the source code into the container
COPY . /redis-query-analyzer/

# Download and install any dependencies
RUN npm ci

ENV REDIS_PORT=6379
ENV INFLUX_DB_URL=YOUR_INFLUX_URL
ENV INFLUX_DB_TOKEN=YOUR_INFLUX_DB_TOKEN
ENV INFLUX_DB_ORG=YOUR_INFLUX_DB_ORG
ENV INFLUX_DB_BUCKET=YOUR_INFLUX_DB_BUCKET
ENV NETWORK_INTERFACE=YOUR_NETWORK_INTERFACE

CMD ["node", "index.js"]
FROM ubuntu:14.04

RUN apt-get update && apt-get install -y \
  build-essential \
  autoconf \
  libcurl4-openssl-dev \
  libfuse-dev \
  libexpat1-dev

ADD . s3backer

WORKDIR "./s3backer"

RUN ["./autogen.sh"]
RUN ["./configure"]
RUN ["make"]
RUN ["make", "install"]

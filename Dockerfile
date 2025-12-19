FROM ubuntu:latest
LABEL authors="abhays"

ENTRYPOINT ["top", "-b"]
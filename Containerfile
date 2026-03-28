FROM scratch as build
COPY . /

FROM python
RUN apt-get update && apt-get install -y --no-install-recommends btrfs-progs
RUN --mount=type=bind,target=/btsnap,from=build pip install /btsnap
ENTRYPOINT ["btsnap"]

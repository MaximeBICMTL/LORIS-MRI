FROM python:3.11

RUN cat /etc/os-release

ENV BUCKET_URL=https://ace-minio-1.loris.ca:9000
ENV BUCKET_NAME=loris-rb-data
ENV BUCKET_ACCESS_KEY=lorisadmin-ro
ENV BUCKET_SECRET_KEY=Tn=qP3LupmXnMuc

RUN apt-get update && apt-get install -y s3fs fuse kmod
RUN modprobe fuse
RUN mkdir /data-imaging
RUN touch .passwd-s3fs
RUN chmod 600 .passwd-s3fs
RUN echo $BUCKET_ACCESS_KEY:$BUCKET_SECRET_KEY > .passwd-s3fs
RUN s3fs $env.BUCKET_NAME /data-imaging -o url=$BUCKET_URL -o passwd_file=.passwd-s3fs -o use_path_request_style -o allow_other

RUN ls /data-imaging

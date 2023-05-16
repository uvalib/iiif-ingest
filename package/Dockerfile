#
# build the target application
#
FROM public.ecr.aws/docker/library/golang:1.20-alpine3.18 as builder
RUN apk update && apk upgrade && apk add make

WORKDIR /build
COPY go.mod go.sum Makefile ./
COPY cmd ./cmd
RUN make linux

#
# build the target container
#
FROM public.ecr.aws/docker/library/debian:bullseye-slim

# update the packages
RUN apt-get -y update && apt-get -y upgrade && apt-get -y install procps htop ca-certificates imagemagick-6.q16

# Create the run user and group
RUN groupadd -g 18570 docker && useradd -u 1984 -g docker docker

# set the timezone appropriatly
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Specify home 
ENV APP_HOME /iiif-ingest
WORKDIR $APP_HOME

# Create necessary directories
RUN mkdir -p $APP_HOME $APP_HOME/bin $APP_HOME/scripts
RUN chown -R docker $APP_HOME && chgrp -R docker $APP_HOME

# run command
CMD scripts/entry.sh

# Move in necessary assets
COPY package/data/container_bash_profile /home/docker/.profile
COPY package/scripts/entry.sh $APP_HOME/scripts/entry.sh
COPY --from=builder /build/bin/iiif-ingest.linux $APP_HOME/bin/iiif-ingest

# Ensure permissions are correct
RUN chown docker:docker /home/docker/.profile $APP_HOME/scripts/entry.sh $APP_HOME/bin/iiif-ingest && chmod 755 /home/docker/.profile $APP_HOME/scripts/entry.sh $APP_HOME/bin/iiif-ingest

# Add the build tag
ARG BUILD_TAG
RUN test -n "$BUILD_TAG" && touch $APP_HOME/buildtag.build-$BUILD_TAG || touch $APP_HOME/buildtag.build-0

# Specify the user
USER docker

#
# end of file
#

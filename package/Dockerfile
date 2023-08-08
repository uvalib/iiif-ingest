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
FROM public.ecr.aws/docker/library/alpine:3.18

# update the packages
RUN apk update && apk upgrade && apk add bash tzdata ca-certificates curl exiftool libgomp libgcc openjpeg && rm -rf /var/cache/apk/*

# Create the run user and group
RUN addgroup --gid 18570 docker && adduser --uid 1984 docker -G docker -D

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

# image magick support
COPY distro/ImageMagick-7.x/convert /usr/local/bin
COPY distro/ImageMagick-7.x/libMagickCore-7.Q16HDRI.so.10 /usr/local/lib
COPY distro/ImageMagick-7.x/libMagickWand-7.Q16HDRI.so.10 /usr/local/lib

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

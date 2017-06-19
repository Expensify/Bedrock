FROM alpine:3.6

# Requirements
RUN apk update \
	&& apk add alpine-sdk

RUN apk add musl-dev pcre-dev zlib-dev libexecinfo-dev

RUN adduser -h /out -u 1000 -D build \
	&& chgrp build /lib \
	&& chmod 775 /lib

COPY makeme /usr/bin

USER build
WORKDIR /src

ENTRYPOINT ["makeme"]

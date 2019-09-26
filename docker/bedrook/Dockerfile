FROM alpine:3.6

RUN apk update \
	&& apk add libpcrecpp libstdc++ libgcc libexecinfo

RUN adduser -h /db -u 1000 -D bed \
	&& chgrp bed /lib \
	&& chmod 775 /lib

COPY bedrock start-bedrock /usr/local/bin/
COPY udf.so /usr/local/lib/

#USER bed
VOLUME /db
WORKDIR /db

ENTRYPOINT ["start-bedrock"]

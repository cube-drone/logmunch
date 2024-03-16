unpack:
	cp sample.log.gz sample.log.unpack.gz
	gzip -df sample.log.unpack.gz
	mv sample.log.unpack sample.log

build:
	docker build . -t log-gen

run:
	docker stop log-jennifer -t 1 || true
	docker rm log-jennifer || true
	docker run -d \
		--network=host \
		--name=log-jennifer \
		--log-driver=splunk \
		--log-opt splunk-token=SPLUNK-TOKEN-GOES-HERE \
		--log-opt splunk-url=http://host.docker.internal:9283 \
		--log-opt tag="SPLUNK-TAG" \
		--log-opt splunk-format=raw \
		--log-opt env=HAMS_AHOY \
		--log-opt env=HAMS_AHOY2 \
		--env "HAMS_AHOY=PORKSWINDLE" \
		--env "HAMS_AHOY2=SWINEFLESH" \
		--label location=fatlanta \
		log-gen

clean:
	docker stop log-jennifer -t 1
	docker rm log-jennifer
default:
	cat Makefile

unpack:
	cp sample.log.gz sample.log.unpack.gz
	gzip -df sample.log.unpack.gz
	mv sample.log.unpack sample.log

build:
	docker build . -t log-gen

start:
	docker stop log-jennifer -t 1 || true
	docker rm log-jennifer || true
	docker stop log-bennifer -t 1 || true
	docker rm log-bennifer || true
	docker stop log-tynnifer -t 1 || true
	docker rm log-tynnifer || true
	docker stop log-mannifer -t 1 || true
	docker rm log-mannifer || true
	docker stop log-dannifer -t 1 || true
	docker rm log-dannifer || true
	docker run -d \
		--network=host \
		--name=log-jennifer \
		--log-driver=splunk \
		--log-opt splunk-token=SPLUNK-TOKEN-GOES-HERE \
		--log-opt splunk-url=http://host.docker.internal:9283 \
		--log-opt tag="JENNIFER" \
		--log-opt splunk-format=raw \
		--log-opt env=HAMS_AHOY \
		--log-opt env=HAMS_AHOY2 \
		--log-opt env=LOG_FILE \
		--env "HAMS_AHOY=PORKSWINDLE" \
		--env "HAMS_AHOY2=SWINEFLESH" \
		--env "LOG_FILE=sample.log" \
		--env "DELAY_MS=1" \
		--label location=fatlanta \
		log-gen

	docker run -d \
		--network=host \
		--name=log-bennifer \
		--log-driver=splunk \
		--log-opt splunk-token=SPLUNK-TOKEN-GOES-HERE \
		--log-opt splunk-url=http://host.docker.internal:9283 \
		--log-opt tag="BENNIFER" \
		--log-opt splunk-format=raw \
		--log-opt env=HAMS_AHOY \
		--log-opt env=HAMS_AHOY2 \
		--log-opt env=LOG_FILE \
		--env "HAMS_AHOY=PORKSWINDLE" \
		--env "HAMS_AHOY2=SWINEFLESH" \
		--env "LOG_FILE=garbage.log" \
		--env "DELAY_MS=1" \
		--label location=fatlanta \
		log-gen

	docker run -d \
		--network=host \
		--name=log-tynnifer \
		--log-driver=splunk \
		--log-opt splunk-token=SPLUNK-TOKEN-GOES-HERE \
		--log-opt splunk-url=http://host.docker.internal:9283 \
		--log-opt tag="TYNNIFER" \
		--log-opt splunk-format=raw \
		--log-opt env=HAMS_AHOY \
		--log-opt env=HAMS_AHOY2 \
		--log-opt env=LOG_FILE \
		--env "HAMS_AHOY=PORKSWANDLE" \
		--env "HAMS_AHOY2=SWOONFLESH" \
		--env "LOG_FILE=sample.log" \
		--env "DELAY_MS=1" \
		--label location=fatlanta \
		log-gen

	docker run -d \
		--network=host \
		--name=log-mannifer \
		--log-driver=splunk \
		--log-opt splunk-token=SPLUNK-TOKEN-GOES-HERE \
		--log-opt splunk-url=http://host.docker.internal:9283 \
		--log-opt tag="MANNIFER" \
		--log-opt splunk-format=raw \
		--log-opt env=HAMS_AHOY \
		--log-opt env=HAMS_AHOY2 \
		--log-opt env=LOG_FILE \
		--env "HAMS_AHOY=PORKSWONDLE" \
		--env "HAMS_AHOY2=BUTTFLESH" \
		--env "LOG_FILE=sample.log" \
		--env "DELAY_MS=1" \
		--label location=fatlanta \
		log-gen

	docker run -d \
		--network=host \
		--name=log-dannifer \
		--log-driver=splunk \
		--log-opt splunk-token=SPLUNK-TOKEN-GOES-HERE \
		--log-opt splunk-url=http://host.docker.internal:9283 \
		--log-opt tag="DANNIFER" \
		--log-opt splunk-format=raw \
		--log-opt env=HAMS_AHOY \
		--log-opt env=HAMS_AHOY2 \
		--log-opt env=LOG_FILE \
		--env "HAMS_AHOY=PORKSWONDLE" \
		--env "HAMS_AHOY2=BUTTFLESH" \
		--env "LOG_FILE=sample.log" \
		--env "DELAY_MS=1" \
		--label location=fatlanta \
		log-gen

clean:
	docker stop log-jennifer -t 1
	docker rm log-jennifer
	docker stop log-bennifer -t 1 || true
	docker rm log-bennifer || true
	docker stop log-tynnifer -t 1 || true
	docker rm log-tynnifer || true
	docker stop log-mannifer -t 1 || true
	docker rm log-mannifer || true
	docker stop log-dannifer -t 1 || true
	docker rm log-dannifer || true
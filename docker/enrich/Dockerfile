FROM snowplow/snowplow-enrich-kinesis:6.10.0
USER root

# Bake the DB-IP City Lite database into the image. Snowplow no longer hosts
# a usable MaxMind GeoLite2 mirror (their S3 object is a 1 KB stub since
# MaxMind licensing changed). DB-IP Lite is free, MaxMind-compatible, and
# geolocates to country + city. Rebuild the image monthly to refresh.
ARG DBIP_MONTH=2026-04
RUN mkdir -p /snowplow/config/maxmind \
  && curl -fsSL -o /tmp/dbip.mmdb.gz \
       "https://download.db-ip.com/free/dbip-city-lite-${DBIP_MONTH}.mmdb.gz" \
  && gunzip -c /tmp/dbip.mmdb.gz > /snowplow/config/maxmind/dbip-city-lite.mmdb \
  && rm /tmp/dbip.mmdb.gz

COPY enrich.hocon /snowplow/config/enrich.hocon
COPY iglu-resolver.json /snowplow/config/iglu-resolver.json
COPY enrichments/ /snowplow/config/enrichments/

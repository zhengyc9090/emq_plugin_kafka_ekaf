PROJECT = emq_plugin_kafka_ekaf
PROJECT_DESCRIPTION = EMQ Plugin Template
PROJECT_VERSION = 2.3.5

BUILD_DEPS = emqttd cuttlefish ekaf
dep_emqttd = git https://github.com/emqtt/emqttd master
dep_cuttlefish = git https://github.com/emqtt/cuttlefish
dep_ekaf = git https://github.com/helpshift/ekaf.git master

ERLC_OPTS += +debug_info
ERLC_OPTS += +'{parse_transform, lager_transform}'

NO_AUTOPATCH = cuttlefish

COVER = true

include erlang.mk

app:: rebar.config

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/emq_plugin_kafka_ekaf.conf -i priv/emq_plugin_kafka_ekaf.schema -d data

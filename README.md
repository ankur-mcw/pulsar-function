Use below commands to run function in local pulsar environment

bin/pulsar-admin functions localrun \
--inputs persistent://public/default/my-topic \
--output persistent://public/default/exception-output \
--jar \<path-to-executable-jar\>/pulsar-function-1.0-SNAPSHOT-jar-with-dependencies.jar \
--classname com.narvar.pulsar.function.RedirectToExceptionFunction \
--user-config '{"api_gateway_url":"value-1", "event_codes":"500,700"}' \
--log-topic persistent://public/default/log-topic \
--name RedirectToExceptionFunction


bin/pulsar-admin functions localrun \
--log-topic persistent://public/default/log-topic \
--function-config-file \<path-to-yaml-file\>/redirect-to-exception.yaml

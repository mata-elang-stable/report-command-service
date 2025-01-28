# Go parameters
PROTOC = protoc

proto-compile: ## Compile proto file
	$(PROTOC) --go_out=./internal protos/sensor_events.proto
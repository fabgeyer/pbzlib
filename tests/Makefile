
all: messages_pb2.py messages.descr

%_pb2.py: %.proto
	protoc -I=$(dir $<) --python_out=$(dir $@) $<

%.descr: %.proto
	protoc -I=$(dir $<) --include_imports --descriptor_set_out=$@ $<

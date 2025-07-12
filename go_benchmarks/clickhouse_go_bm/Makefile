# Define the name of the output binary
BINARY_NAME = output/bm1

# Define all Go source files
GO_SRC = cmd/bm1/main.go

# Target to build the greeter binary
build:
	go build -o $(BINARY_NAME) $(GO_SRC)

# Target to run the greeter binary
run: build
	./$(BINARY_NAME)

# Target to clean up the built binary
clean:
	rm -f $(BINARY_NAME)

# Target to display help message
help:
	@echo "Available targets:"
	@echo "  build  - Build the bm1 binary"
	@echo "  run    - Run the bm1 binary"
	@echo "  clean  - Clean up the built binary"
	@echo "  help   - Display this help message"
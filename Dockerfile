FROM ubuntu:24.10

# Install dependencies
RUN apt-get update && apt-get install -y \
	curl \
	unzip \
	python3 \
	python3-pip

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
	unzip awscliv2.zip && \
	./aws/install

# Clean up
RUN rm -rf awscliv2.zip aws

# Copy the application code
COPY . /app
WORKDIR /app

# Set the entrypoint
ENTRYPOINT ["tail", "-f", "/dev/null"]


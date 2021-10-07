FROM msales/go-builder:1.17-base-1.0.0 as builder

# This image only serves testing purposes.

# Set token
ARG GITHUB_TOKEN
RUN git config --global url."https://${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"

WORKDIR /app

COPY ./ .

# It's a library. Build will only check if the code can be compiled successfully.
RUN go build -v ./...
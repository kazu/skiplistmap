# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.183.0/containers/go/.devcontainer/base.Dockerfile

# The base image provides the vscode user and optional Node tooling.
ARG VARIANT="1.17"
ARG GO_VERSION="1.27.1"
FROM golang:${GO_VERSION}-bookworm AS go-toolchain
FROM mcr.microsoft.com/vscode/devcontainers/go:0-${VARIANT}

RUN rm -rf /usr/local/go
COPY --from=go-toolchain /usr/local/go /usr/local/go

# [Option] Install Node.js
ARG INSTALL_NODE="true"
ARG NODE_VERSION="lts/*"
RUN if [ "${INSTALL_NODE}" = "true" ]; then su vscode -c "umask 0002 && . /usr/local/share/nvm/nvm.sh && nvm install ${NODE_VERSION} 2>&1"; fi

# [Optional] Uncomment this section to install additional OS packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends <your-package-list-here>

# [Optional] Uncomment the next line to use go get to install anything else you need
# RUN go get -x <your-dependency-or-tool>

# [Optional] Uncomment this line to install global node packages.
# RUN su vscode -c "source /usr/local/share/nvm/nvm.sh && npm install -g <your-package-here>" 2>&1
COPY command.sh /tmp/command.sh
RUN chmod +x /tmp/command.sh
ENTRYPOINT ["/tmp/command.sh"]

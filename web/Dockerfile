FROM node:17

# Change shell to bash
SHELL ["/bin/bash", "-c"]

ARG USE_PROXY
ARG PROXY_SERVER=http://www-authproxy.statoil.net:80
RUN echo $USE_PROXY && echo $PROXY_SERVER

# Set env vars if USE_PROXY is set, else empty
# (note: lowercase name needed for apt-get!)
ENV http_proxy=${USE_PROXY:+${PROXY_SERVER}}
ENV https_proxy=${USE_PROXY:+${PROXY_SERVER}}
ENV HTTP_PROXY=${http_proxy}
ENV HTTPS_PROXY=${https_proxy}

RUN echo "Checking proxy env vars (should be null if USE_PROXY is null):" \
&& echo "HTTP_PROXY:${http_proxy}" \
&& echo "HTTPS_PROXY:${https_proxy}"



WORKDIR /code
COPY package.json package-lock.json ./
COPY . .
RUN npm i

# USER 1000:1000
CMD ["npm", "start"]

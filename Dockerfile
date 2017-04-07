FROM ubuntu

RUN apt-get update && apt-get install -y \
    vim \
    wget \
    python \
    gcc \
    g++ \
    build-essential \
    default-jdk

RUN export JAVA_HOME=/usr/bin

RUN wget https://nodejs.org/dist/v7.8.0/node-v7.8.0-linux-x64.tar.xz
RUN tar -xvf node-v7.8.0-linux-x64.tar.xz
RUN ln -s /node-v7.8.0-linux-x64/bin/node /usr/bin/node
RUN ln -s /node-v7.8.0-linux-x64/bin/npm /usr/bin/npm
RUN rm -f node-v7.8.0-linux-x64.tar.xz

RUN mkdir /app
WORKDIR /app
RUN mkdir logs
COPY drivers/ drivers/
COPY src/ src/
COPY package.json .

RUN npm install

EXPOSE 8080 
CMD ["/usr/bin/node", "/app/src/index.js"]

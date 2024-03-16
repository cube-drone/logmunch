FROM node:20-alpine
COPY . /home/node/app
WORKDIR /home/node/app
RUN npm install
EXPOSE 9283
CMD ["node", "index.js"]
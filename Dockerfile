FROM node:alpine

WORKDIR /app    

COPY package.json /app

RUN npm i

COPY . /app

ENV PORT=8000

EXPOSE ${PORT}

CMD ["npm","start"]
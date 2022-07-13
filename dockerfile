FROM node:16-alpine

USER node

WORKDIR /home/node/app

CMD ["yarn", "run", "dev"]
FROM node:18 AS builder

WORKDIR /app

COPY package*.json ./

RUN npm install --production --silent

COPY . .

RUN mkdir -p /var/lib/app

EXPOSE 8000

# Start the app
CMD ["node", "index.js"]

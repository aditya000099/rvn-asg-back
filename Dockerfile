FROM node:18 AS builder

WORKDIR /app

# Copy package files first for caching
COPY package*.json ./

# Install dependencies
RUN npm install --production --silent

# Copy app code
COPY . .

# Ensure data dir exists
RUN mkdir -p /var/lib/app

EXPOSE 8000

# Run app
CMD ["node", "index.js"]

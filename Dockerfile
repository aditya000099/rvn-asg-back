FROM node:18 AS builder

WORKDIR /app

# Install build tools for native modules
RUN apt-get update && apt-get install -y \
    build-essential \
    python3 \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy package.json first (cache)
COPY package*.json ./

# Install dependencies (remove --silent for debugging)
RUN npm install --production

# Copy the rest of the code
COPY . .

# Ensure data folder exists
RUN mkdir -p /var/lib/app

EXPOSE 8000

CMD ["node", "index.js"]


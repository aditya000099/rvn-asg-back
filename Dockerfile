FROM oven/bun:1 AS base

WORKDIR /app

# Copy package files and install production dependencies with Bun
COPY package*.json ./
RUN bun install --production

# Copy the rest of the application
COPY . .

# Ensure data directory exists inside container
RUN mkdir -p /var/lib/app

EXPOSE 8000

# Use bun to run the application directly
CMD ["bun", "index.js"]

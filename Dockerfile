# Use Node.js LTS Debian-based image (better compatibility for native modules)
FROM node:20-alpine AS build

# Set working directory
WORKDIR /app

# Copy package files first (for caching)
COPY package*.json ./

# Install runtime dependencies. Use npm install here because
# this project doesn't include a package-lock.json and some
# native modules (duckdb) build more reliably on Debian images.
RUN npm install --production --silent

# Copy rest of the app
COPY . .

# Ensure data directory exists inside container
RUN mkdir -p /var/lib/app

# Expose the port your app uses
EXPOSE 8000

# Start the app
CMD ["node", "index.js"]

FROM oven/bun:1.2.21-slim

WORKDIR /app

COPY package.json ./
COPY yarn.lock ./

RUN bun install

COPY . .

EXPOSE 3000

# Run your app with Bun
CMD ["bun", "src"]

# docker build -t turbolfs:latest .

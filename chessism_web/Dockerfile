FROM node:22-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install

COPY . .

ENV VITE_API_BASE_URL=/api
EXPOSE 6789

CMD ["npm", "run", "dev"]

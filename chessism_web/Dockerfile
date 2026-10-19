FROM node:22-alpine

WORKDIR /app

COPY chessism_web/package.json chessism_web/package-lock.json* ./
RUN npm install

COPY chessism_web/ .
COPY hegel_.md /hegel_.md

ENV VITE_API_BASE_URL=/api
EXPOSE 6789

CMD ["npm", "run", "dev"]

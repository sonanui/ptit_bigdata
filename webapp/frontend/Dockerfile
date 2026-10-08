# Frontend React: build tĩnh bằng Node, phục vụ bằng Nginx; /api được chuyển tới container web-backend.
# Build: docker compose --profile web build web-frontend
FROM node:24-alpine AS build
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm test && npm run build

FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /src/dist /usr/share/nginx/html
EXPOSE 80

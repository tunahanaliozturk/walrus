FROM node:24-alpine AS build
WORKDIR /source

# The lockfile first, so the install layer is reused whenever only source has changed.
COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build && node scripts/bundle-budget.mjs

FROM nginx:1-alpine AS runtime

# The official image runs templates through envsubst at start, substituting only variables that exist in
# the environment, so nginx's own $host and $uri pass through untouched.
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY security-headers.conf /etc/nginx/security-headers.conf
COPY --from=build /source/dist /usr/share/nginx/html

EXPOSE 8080

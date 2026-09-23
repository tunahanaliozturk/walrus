# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /source

# Restore before the rest of the source is copied, so editing a .cs file does not invalidate the restore layer.
# The .editorconfig comes along because analyzer severities live in it and the build treats warnings as errors,
# so leaving it out makes the image build fail where a local build passes.
COPY global.json .editorconfig Directory.Build.props Directory.Packages.props ./
COPY src/Walrus.Domain/Walrus.Domain.csproj src/Walrus.Domain/
COPY src/Walrus.Application/Walrus.Application.csproj src/Walrus.Application/
COPY src/Walrus.Infrastructure/Walrus.Infrastructure.csproj src/Walrus.Infrastructure/
COPY src/Walrus.Api/Walrus.Api.csproj src/Walrus.Api/
RUN dotnet restore src/Walrus.Api/Walrus.Api.csproj

COPY src/ src/
RUN dotnet publish src/Walrus.Api/Walrus.Api.csproj --configuration Release --no-restore --output /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble AS runtime
WORKDIR /app

# The runtime image has neither curl nor wget, so the health check is a short bash script that speaks HTTP
# over /dev/tcp rather than a network client installed for one request.
COPY --chmod=755 docker/healthcheck.sh /usr/local/bin/healthcheck

# The image ships a non-root user. The only reason services still run as root is that nobody changed it.
USER $APP_UID
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080

COPY --from=build /app .
ENTRYPOINT ["dotnet", "Walrus.Api.dll"]

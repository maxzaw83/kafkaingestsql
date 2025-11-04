# ---------------------------------------------------------------------------------
# Stage 1: Build the application
# ---------------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# FIX: Corrected casing from 'IngestServer' to 'ingestserver' to match the file system.
COPY ["ingestserver/ingestserver.csproj", "ingestserver/"]
RUN dotnet restore "ingestserver/ingestserver.csproj"

# Copy all remaining source files
COPY . .

# Publish the application
WORKDIR "/src/ingestserver"
RUN dotnet publish -c Release -o /app/publish

# ---------------------------------------------------------------------------------
# Stage 2: Final minimal runtime image
# ---------------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
COPY --from=build /app/publish .

# FIX: Corrected casing of the DLL file to match the project name (ingestserver.dll)
ENTRYPOINT ["dotnet", "ingestserver.dll"]

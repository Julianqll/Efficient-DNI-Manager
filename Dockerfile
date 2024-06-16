# Usar una imagen base con Ubuntu
FROM ubuntu:latest

# Establecer el directorio de trabajo
WORKDIR /app

# Instalar dependencias necesarias
RUN apt-get update && apt-get install -y \
    g++ \
    make \
    cmake \
    libpistache-dev \
    libzstd-dev \
    libboost-iostreams-dev \
    libboost-system-dev

# Copiar los archivos del proyecto al contenedor
COPY . .

# Compilar la aplicación
RUN g++ -o main main.cpp -lpistache -lzstd -lboost_iostreams -lboost_system

# Exponer el puerto en el que la aplicación escucha (ajusta esto según tu API)
EXPOSE 5000

# Comando para ejecutar la aplicación
CMD ["./main"]

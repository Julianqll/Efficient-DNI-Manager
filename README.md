# Backend Proyecto de EDA Avanzada
API construída en C++ con Pistache
## Instrucciones de uso
### Requisitos
Tener [Docker](https://www.docker.com/get-started/) previamente instalado
### Pasos
1. Clona el repositorio
```bash
git clone https://github.com/Julianqll/edavProyectoBackend
```
2. Ingresa a la carpeta del repositorio clonado
```bash
cd edavProyectoBackend
```
3. Ejecuta el contenedor
```docker
docker run -d -p 5000:5000 -v ./dataFiles:/app/data edav-api
```



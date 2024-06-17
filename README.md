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
3. Construir la imagen
```docker
docker build -t edav-api .
```
4. Ejecutar en el contenedor
```docker
docker run -d -p 5000:5000 -v ./dataFiles:/app/data edav-api
```

## Endpoints
- #### /create 
    Lee el archivo .txt con los 33 millones de registros y crea un Btree en caché. Es el endpoint incial - sin este no funcionan los demás
- #### /save 
    Guarda el Btree creado en caché en un archivo **(btreebinary.bin)** binario,el cual se visualizará en la carpeta ```dataFiles``` 
- #### /open 
    Abre un archivo binario donde se haya guardado previamente el Btree y lo carga a caché
- #### /search?dni=< dni (ejm: 00000001)> 
    Una vez se haya creado un arbol(```/create```) o abierto un archivo(```/open```) para tener un Btree en caché, se puede usar este endpoint para verificar la existencia de un registro
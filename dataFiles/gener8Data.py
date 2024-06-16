import csv
import random
import zstd
import os

def generate_data(num_records, filename):
    first_names = ["John", "Jane", "Alex", "Emily", "Chris", "Katie", "Michael", "Sarah"]
    last_names = ["Doe", "Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
    departments = ["Finance", "HR", "Engineering", "Sales"]
    provinces = ["Province1", "Province2", "Province3"]
    districts = ["District1", "District2", "District3"]
    locations = ["Location1", "Location2", "Location3"]

    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['dni', 'nombres', 'apellidos', 'lugar_nacimiento', 'departamento', 'provincia', 'ciudad', 'distrito', 'ubicacion', 'correo']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(1, num_records + 1):
            dni = str(i).zfill(8)
            nombres = random.choice(first_names)
            apellidos = random.choice(last_names)
            lugar_nacimiento = random.choice(cities)
            departamento = random.choice(departments)
            provincia = random.choice(provinces)
            ciudad = random.choice(cities)
            distrito = random.choice(districts)
            ubicacion = random.choice(locations)
            correo = f"{nombres.lower()}.{apellidos.lower()}@example.com"
            writer.writerow({
                'dni': dni, 'nombres': nombres, 'apellidos': apellidos, 'lugar_nacimiento': lugar_nacimiento,
                'departamento': departamento, 'provincia': provincia, 'ciudad': ciudad,
                'distrito': distrito, 'ubicacion': ubicacion, 'correo': correo
            })

def compress_file(input_filename, output_filename):
    with open(input_filename, 'rb') as input_file:
        data = input_file.read()
        compressed_data = zstd.compress(data, 19)
        with open(output_filename, 'wb') as output_file:
            output_file.write(compressed_data)

if __name__ == "__main__":
    num_records = 33000000  # Número de registros a generar
    csv_filename = 'data.csv'
    compressed_filename = 'data.zst'

    generate_data(num_records, csv_filename)
    compress_file(csv_filename, compressed_filename)

    # Eliminar el archivo CSV original para ahorrar espacio
    os.remove(csv_filename)

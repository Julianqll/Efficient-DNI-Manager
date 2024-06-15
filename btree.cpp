#include <iostream>
#include <vector>
#include <memory>
#include <string>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <zstd.h>
#include <unordered_map>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/spirit/include/qi.hpp>

using namespace std;
using namespace std::chrono;
namespace qi = boost::spirit::qi;

#pragma pack(1)

struct Direccion {
    uint32_t departamento; // Índice del departamento en el pool
    uint32_t provincia; // Índice de la provincia en el pool
    uint32_t ciudad; // Índice de la ciudad en el pool
    uint32_t distrito; // Índice del distrito en el pool
    uint32_t ubicacion; // Índice de la ubicación en el pool
};

class Ciudadano {
private:
    char dni[8]; // Almacenar DNI como un array de caracteres
    uint32_t nombres; // Índice de los nombres en el pool
    uint32_t apellidos; // Índice de los apellidos en el pool
    uint32_t lugar_nacimiento; // Índice del lugar de nacimiento en el pool
    Direccion direccion;
    uint64_t telefono; // Almacenar teléfono como entero sin signo
    uint32_t correo; // Índice del correo electrónico en el pool
    char nacionalidad[2]; // Almacenar código de país (ISO 3166-1 alpha-2)
    unsigned sexo : 1; // Almacenar sexo como bit
    unsigned estado_civil : 3; // Almacenar estado civil como 3 bits (0: Soltero, 1: Casado, etc.)

public:
    Ciudadano(const char* dni, uint32_t nombres, uint32_t apellidos, uint32_t lugar_nacimiento, Direccion direccion, uint64_t telefono, uint32_t correo, const char* nacionalidad, unsigned sexo, unsigned estado_civil)
        : nombres(nombres), apellidos(apellidos), lugar_nacimiento(lugar_nacimiento), direccion(direccion), telefono(telefono), correo(correo), sexo(sexo), estado_civil(estado_civil)
    {
        strncpy(this->dni, dni, 8);
        strncpy(this->nacionalidad, nacionalidad, 2);
    }

    string getDni() const { return string(dni, 8); }
    uint32_t getNombres() const { return nombres; }
    uint32_t getApellidos() const { return apellidos; }
    uint32_t getLugarNacimiento() const { return lugar_nacimiento; }
    Direccion getDireccion() const { return direccion; }
    uint64_t getTelefono() const { return telefono; }
    uint32_t getCorreo() const { return correo; }
    string getNacionalidad() const { return string(nacionalidad, 2); }
    unsigned getSexo() const { return sexo; }
    unsigned getEstadoCivil() const { return estado_civil; }

    void serialize(ostream& os) const {
        os.write(dni, 8);
        os.write(reinterpret_cast<const char*>(&nombres), sizeof(nombres));
        os.write(reinterpret_cast<const char*>(&apellidos), sizeof(apellidos));
        os.write(reinterpret_cast<const char*>(&lugar_nacimiento), sizeof(lugar_nacimiento));
        os.write(reinterpret_cast<const char*>(&direccion), sizeof(direccion));
        os.write(reinterpret_cast<const char*>(&telefono), sizeof(telefono));
        os.write(reinterpret_cast<const char*>(&correo), sizeof(correo));
        os.write(nacionalidad, 2);
        os.put((sexo << 3) | estado_civil);
    }

    static Ciudadano deserialize(istream& is) {
        char dni[8];
        is.read(dni, 8);

        uint32_t nombres;
        is.read(reinterpret_cast<char*>(&nombres), sizeof(nombres));

        uint32_t apellidos;
        is.read(reinterpret_cast<char*>(&apellidos), sizeof(apellidos));

        uint32_t lugar_nacimiento;
        is.read(reinterpret_cast<char*>(&lugar_nacimiento), sizeof(lugar_nacimiento));

        Direccion direccion;
        is.read(reinterpret_cast<char*>(&direccion), sizeof(direccion));

        uint64_t telefono;
        is.read(reinterpret_cast<char*>(&telefono), sizeof(telefono));

        uint32_t correo;
        is.read(reinterpret_cast<char*>(&correo), sizeof(correo));

        char nacionalidad[2];
        is.read(nacionalidad, 2);

        char sex_and_status;
        is.get(sex_and_status);

        unsigned sexo = (sex_and_status >> 3) & 1;
        unsigned estado_civil = sex_and_status & 7;

        return Ciudadano(dni, nombres, apellidos, lugar_nacimiento, direccion, telefono, correo, nacionalidad, sexo, estado_civil);
    }
};

class BTreeNode {
private:
    int t;
    int n;
    bool leaf;
    vector<unique_ptr<Ciudadano>> keys;
    vector<unique_ptr<BTreeNode>> children;

public:
    BTreeNode(int _t, bool _leaf);

    void traverse();
    void splitChild(int i, BTreeNode* y);
    void insertNonFull(unique_ptr<Ciudadano>&& citizen);
    Ciudadano* search(const string& dni);

    void serialize(ostringstream& buffer);
    void deserialize(istringstream& buffer);
    friend class Btree;
};

class Btree {
private:
    unique_ptr<BTreeNode> root;
    int t;

public:
    Btree(int _t) : root(nullptr), t(_t) {}

    void traverse() {
        if (root)
            root->traverse();
    }

    void insert(unique_ptr<Ciudadano>&& citizen);
    Ciudadano* search(const string& dni);

    void serialize(const string& filename);
    void deserialize(const string& filename);
};

BTreeNode::BTreeNode(int _t, bool _leaf) : t(_t), n(0), leaf(_leaf) {
    keys.resize(2 * t - 1);
    children.resize(2 * t);
}

void BTreeNode::traverse() {
    int i;
    for (i = 0; i < n; i++) {
        if (!leaf)
            children[i]->traverse();
        cout << keys[i]->getDni() << endl; // Just printing for demonstration
    }
    if (!leaf)
        children[i]->traverse();
}

void BTreeNode::insertNonFull(unique_ptr<Ciudadano>&& citizen) {
    int i = n - 1;

    if (leaf) {
        while (i >= 0 && keys[i]->getDni() > citizen->getDni()) {
            keys[i + 1] = move(keys[i]);
            i--;
        }
        keys[i + 1] = move(citizen);
        n++;
    } else {
        while (i >= 0 && keys[i]->getDni() > citizen->getDni())
            i--;

        if (children[i + 1]->n == 2 * t - 1) {
            splitChild(i + 1, children[i + 1].get());

            if (keys[i + 1]->getDni() < citizen->getDni())
                i++;
        }
        children[i + 1]->insertNonFull(move(citizen));
    }
}

void BTreeNode::splitChild(int i, BTreeNode* y) {
    unique_ptr<BTreeNode> z = make_unique<BTreeNode>(y->t, y->leaf);
    z->n = y->t - 1;

    for (int j = 0; j < y->t - 1; j++)
        z->keys[j] = move(y->keys[j + y->t]);

    if (!y->leaf) {
        for (int j = 0; j < y->t; j++)
            z->children[j] = move(y->children[j + y->t]);
    }

    y->n = y->t - 1;

    for (int j = n; j >= i + 1; j--)
        children[j + 1] = move(children[j]);

    children[i + 1] = move(z);

    for (int j = n - 1; j >= i; j--)
        keys[j + 1] = move(keys[j]);

    keys[i] = move(y->keys[y->t - 1]);
    n++;
}

void BTreeNode::serialize(ostringstream& buffer) {
    buffer.write(reinterpret_cast<const char*>(&n), sizeof(int));
    buffer.write(reinterpret_cast<const char*>(&leaf), sizeof(bool));
    for (int i = 0; i < n; i++) {
        keys[i]->serialize(buffer);
        if (!leaf)
            children[i]->serialize(buffer);
    }
    if (!leaf)
        children[n]->serialize(buffer);
}

void BTreeNode::deserialize(istringstream& buffer) {
    buffer.read(reinterpret_cast<char*>(&n), sizeof(int));
    buffer.read(reinterpret_cast<char*>(&leaf), sizeof(bool));
    keys.resize(2 * t - 1);
    children.resize(2 * t);
    for (int i = 0; i < n; i++) {
        keys[i] = make_unique<Ciudadano>(Ciudadano::deserialize(buffer));
        if (!leaf) {
            children[i] = make_unique<BTreeNode>(t, leaf);
            children[i]->deserialize(buffer);
        }
    }
    if (!leaf) {
        children[n] = make_unique<BTreeNode>(t, leaf);
        children[n]->deserialize(buffer);
    }
}

void Btree::insert(unique_ptr<Ciudadano>&& citizen) {
    if (!root) {
        root = make_unique<BTreeNode>(t, true);
        root->keys[0] = move(citizen);
        root->n = 1;
    } else {
        if (root->n == 2 * t - 1) {
            unique_ptr<BTreeNode> s = make_unique<BTreeNode>(t, false);
            s->children[0] = move(root);
            s->splitChild(0, s->children[0].get());

            int i = 0;
            if (s->keys[0]->getDni() < citizen->getDni())
                i++;

            s->children[i]->insertNonFull(move(citizen));
            root = move(s);
        } else {
            root->insertNonFull(move(citizen));
        }
    }
}

Ciudadano* BTreeNode::search(const string& dni) {
    int i = 0;
    while (i < n && dni > keys[i]->getDni())
        i++;

    if (i < n && dni == keys[i]->getDni())
        return keys[i].get();

    if (leaf)
        return nullptr;

    return children[i]->search(dni);
}

Ciudadano* Btree::search(const string& dni) {
    if (!root) {
        cout << "Tree is empty" << endl;
        return nullptr;
    }
    return root->search(dni);
}

void Btree::serialize(const string& filename) {
    ostringstream buffer;
    if (root) {
        auto start = high_resolution_clock::now();
        root->serialize(buffer);
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);
        cout << "B-Tree serialized to buffer in " << duration.count() << " milliseconds." << endl;

        // Compresión
        string uncompressed_data = buffer.str();
        size_t compressed_size = ZSTD_compressBound(uncompressed_data.size());
        vector<char> compressed_data(compressed_size);
        size_t actual_compressed_size = ZSTD_compress(compressed_data.data(), compressed_size, uncompressed_data.data(), uncompressed_data.size(), 1);

        // Escritura en archivo
        ofstream file(filename, ios::binary | ios::out);
        if (file.is_open()) {
            file.write(compressed_data.data(), actual_compressed_size);
            file.close();
            cout << "B-Tree serialized and compressed to file successfully." << endl;
        } else {
            cerr << "Error opening file for serialization." << endl;
        }
    } else {
        cerr << "Tree is empty." << endl;
    }
}

void Btree::deserialize(const string& filename) {
    ifstream file(filename, ios::binary | ios::in);
    if (file.is_open()) {
        auto start = high_resolution_clock::now();

        file.seekg(0, ios::end);
        size_t file_size = file.tellg();
        file.seekg(0, ios::beg);

        vector<char> compressed_data(file_size);
        file.read(compressed_data.data(), file_size);

        size_t uncompressed_size = ZSTD_getFrameContentSize(compressed_data.data(), compressed_data.size());
        if (uncompressed_size == ZSTD_CONTENTSIZE_ERROR) {
            cerr << "Error determining uncompressed size." << endl;
            return;
        } else if (uncompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
            cerr << "Original size unknown." << endl;
            return;
        }

        vector<char> uncompressed_data(uncompressed_size);
        size_t actual_uncompressed_size = ZSTD_decompress(uncompressed_data.data(), uncompressed_size, compressed_data.data(), compressed_data.size());
        if (ZSTD_isError(actual_uncompressed_size)) {
            cerr << "Decompression error: " << ZSTD_getErrorName(actual_uncompressed_size) << endl;
            return;
        }

        istringstream buffer(string(uncompressed_data.data(), actual_uncompressed_size));
        root = make_unique<BTreeNode>(t, true);
        root->deserialize(buffer);

        auto end = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);
        cout << "B-Tree deserialized in " << duration.count() << " milliseconds." << endl;

        file.close();
    } else {
        cerr << "Error opening file for deserialization." << endl;
    }
}

int main(int argc, char** argv) {
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " <filename>" << endl;
        return 1;
    }

    Btree tree(30);

    // Crear un pool de cadenas para almacenar nombres, apellidos, direcciones, correos y lugares de nacimiento
    unordered_map<string, uint32_t> string_pool;
    vector<string> pool_strings; // Vector para mantener el orden de las cadenas
    uint32_t pool_index = 0;

    auto get_pool_index = [&](const string& str) {
        if (string_pool.find(str) == string_pool.end()) {
            string_pool[str] = pool_index++;
            pool_strings.push_back(str);
        }
        return string_pool[str];
    };

    // Leer y descomprimir el archivo
    string input_filename = argv[1];
    string decompressed_filename = "decompressed_data.csv";

    ifstream input_file(input_filename, ios::binary);
    vector<char> compressed_data((istreambuf_iterator<char>(input_file)), istreambuf_iterator<char>());
    input_file.close();

    size_t uncompressed_size = ZSTD_getFrameContentSize(compressed_data.data(), compressed_data.size());
    if (uncompressed_size == ZSTD_CONTENTSIZE_ERROR) {
        cerr << "Error determining uncompressed size." << endl;
        return 1;
    } else if (uncompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
        cerr << "Original size unknown." << endl;
        return 1;
    }

    vector<char> uncompressed_data(uncompressed_size);
    size_t actual_uncompressed_size = ZSTD_decompress(uncompressed_data.data(), uncompressed_size, compressed_data.data(), compressed_data.size());
    if (ZSTD_isError(actual_uncompressed_size)) {
        cerr << "Decompression error: " << ZSTD_getErrorName(actual_uncompressed_size) << endl;
        return 1;
    }

    ofstream decompressed_file(decompressed_filename, ios::binary);
    decompressed_file.write(uncompressed_data.data(), actual_uncompressed_size);
    decompressed_file.close();

    // Leer el archivo descomprimido
    boost::iostreams::mapped_file mmap(decompressed_filename, boost::iostreams::mapped_file::readonly);
    const char* file_data = mmap.const_data();
    const char* file_end = file_data + mmap.size();

    auto start_parse = high_resolution_clock::now();

    while (file_data < file_end) {
        const char* line_end = std::find(file_data, file_end, '\n');
        string line(file_data, line_end);

        vector<string> fields;
        istringstream ss(line);
        string field;
        while (getline(ss, field, ',')) {
            fields.push_back(field);
        }

        if (fields.size() == 10) {
            string dni = fields[0];
            string nombres = fields[1];
            string apellidos = fields[2];
            string lugar_nacimiento = fields[3];
            Direccion direccion = { get_pool_index(fields[4]), get_pool_index(fields[5]), get_pool_index(fields[6]), get_pool_index(fields[7]), get_pool_index(fields[8]) };
            string correo = fields[9];

            tree.insert(make_unique<Ciudadano>(dni.c_str(), get_pool_index(nombres), get_pool_index(apellidos), get_pool_index(lugar_nacimiento), direccion, 987654321, get_pool_index(correo), "PE", 0, 0));
        }

        file_data = line_end + 1;
    }

    auto stop_parse = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(stop_parse - start_parse);
    cout << "Time taken by parsing: " << duration.count() / 1000.0 << "s\n";

    // Serializar el árbol en disco
    tree.serialize("btreenew.bin");

    // Deserializar el árbol desde disco
    tree.deserialize("btreenew.bin");

    string dniToSearch = "30000000";
    Ciudadano* found = tree.search(dniToSearch);
    if (found) {
        auto get_string_from_pool = [&](uint32_t index) -> string {
            return pool_strings[index];
        };

        cout << "\nDNI: " << found->getDni() << endl;
        cout << "Nombres: " << get_string_from_pool(found->getNombres()) << endl;
        cout << "Apellidos: " << get_string_from_pool(found->getApellidos()) << endl;
        cout << "Lugar de Nacimiento: " << get_string_from_pool(found->getLugarNacimiento()) << endl;
        Direccion dir = found->getDireccion();
        cout << "Direccion: " << get_string_from_pool(dir.departamento) << ", "
             << get_string_from_pool(dir.provincia) << ", "
             << get_string_from_pool(dir.ciudad) << ", "
             << get_string_from_pool(dir.distrito) << ", "
             << get_string_from_pool(dir.ubicacion) << endl;
        cout << "Telefono: " << found->getTelefono() << endl;
        cout << "Correo: " << get_string_from_pool(found->getCorreo()) << endl;
        cout << "Nacionalidad: " << found->getNacionalidad() << endl;
        cout << "Sexo: " << (found->getSexo() == 0 ? "Masculino" : "Femenino") << endl;
        cout << "Estado Civil: " << (found->getEstadoCivil() == 0 ? "Soltero" : "Casado") << endl;
    } else {
        cout << "\nDNI " << dniToSearch << " no encontrado." << endl;
    }

    return 0;
}

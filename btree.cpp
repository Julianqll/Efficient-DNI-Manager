#include <iostream>
#include <vector>
#include <memory>
#include <string>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <zstd.h> // Para la compresión con zstd

using namespace std;
using namespace std::chrono;

struct Direccion {
    string departamento;
    string provincia;
    string ciudad;
    string distrito;
    string ubicacion;
};

class Ciudadano {
private:
    int dni;
    string nombres;
    string apellidos;
    int telefono;
    char sexo;

public:
    Ciudadano(int dni, string nombres, string apellidos, int telefono, char sexo)
        : dni(dni), nombres(nombres), apellidos(apellidos), telefono(telefono), sexo(sexo) {}

    int getDni() const { return dni; }
    string getNombres() const { return nombres; }
    string getApellidos() const { return apellidos; }
    int getTelefono() const { return telefono; }
    char getSexo() const { return sexo; }

    string serialize() const {
        stringstream ss;
        ss << dni << "|" << nombres << "|" << apellidos << "|" << telefono << "|" << sexo << "|";
        return ss.str();
    }

    static Ciudadano deserialize(const string& data) {
        stringstream ss(data);
        string token;
        getline(ss, token, '|');
        int dni = stoi(token);
        getline(ss, token, '|');
        string nombres = token;
        getline(ss, token, '|');
        string apellidos = token;
        getline(ss, token, '|');
        int telefono = stoi(token);
        getline(ss, token, '|');
        char sexo = token[0];
        return Ciudadano(dni, nombres, apellidos, telefono, sexo);
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
    Ciudadano* search(int dni);

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
    Ciudadano* search(int dni);

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
        cout << keys[i]->serialize() << endl; // Just printing for demonstration
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
        string data = keys[i]->serialize();
        int size = data.size();
        buffer.write(reinterpret_cast<const char*>(&size), sizeof(int));
        buffer.write(data.c_str(), size);
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
        int size;
        buffer.read(reinterpret_cast<char*>(&size), sizeof(int));
        char* data = new char[size];
        buffer.read(data, size);
        keys[i] = make_unique<Ciudadano>(Ciudadano::deserialize(string(data, size)));
        delete[] data;
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

Ciudadano* BTreeNode::search(int dni) {
    int i = 0;
    while (i < n && dni > keys[i]->getDni())
        i++;

    if (i < n && dni == keys[i]->getDni())
        return keys[i].get();

    if (leaf)
        return nullptr;

    return children[i]->search(dni);
}

Ciudadano* Btree::search(int dni) {
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

int main() {
    Btree tree(30);

    // Ejemplo de datos de ciudadanos
    for (int i = 1; i <= 33000000; ++i) {
        tree.insert(make_unique<Ciudadano>(i, "Nombre" + to_string(i), "Apellido" + to_string(i), 987654321, 'M'));
    }

    // Serializar el árbol en disco
    tree.serialize("btreenew.bin");

    // Deserializar el árbol desde disco
    tree.deserialize("btreenew.bin");

     int dniToSearch = 30000000;
     Ciudadano* found = tree.search(dniToSearch);
     if (found)
         cout << "\nDNI " << dniToSearch << " encontrado: " << found->getNombres() << " " << found->getApellidos() << endl;
     else
         cout << "\nDNI " << dniToSearch << " no encontrado." << endl;

    return 0;
}

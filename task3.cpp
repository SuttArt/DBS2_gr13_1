// -------------------------------
// Group: 13
// Members:
//      Charlotte Kauter
//      Robert Sitner
//      Artem Suttar
// HU-Account names:
//      ---
//      sitnerro
//      suttarar
// -------------------------------
#include <iostream>
#include <fstream>
#include <sys/resource.h>
#include <string>

void sort_external_file(const std::string &input_file_name, const std::string &output_file_name) {
    std::ifstream file(input_file_name);
if (file.is_open()) {
    std::string line;
    while (std::getline(file, line)) {
        // using printf() in all tests for consistency
        printf("%s", line.c_str());
    }
    file.close();
}

    // creating an ifstream object named file
    std::ifstream file_output;

    // defining the file path
    std::string pathOfFile = "../sorted_file.txt";

    // attempting to open the file
    file_output.open(pathOfFile);

    // checking if the file opening was successful, it will only be true ie. file would have been opened, only if the file exists, so indirectly we are checking if the file exists or not on the given path by opening it from there, if opened, then the file exists, else does not exists
    if (file_output)
    {
        // printing the success message
        std::cout << "File exists" << std::endl;
    }
    else
    {
        // printing the error message
        std::cout << "File does not exists" << std::endl;
    }
    return;
}

float get_memory_usage() {
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    return usage.ru_maxrss / 1000.0;
}

bool file_is_sorted(const std::string &file_name) {
    std::ifstream file(file_name, std::ios::in);

    if (!file.is_open()) {
        std::cerr << "Unable to open file " << file_name << std::endl;
        return false;
    }

    int last, current;

    if (!(file >> last)) {
        // Can't read the first integer
        std::cerr << "Failed to read an integer or file is empty." << std::endl;
        return false;
    }

    while (file >> current) {
        if (file.fail()) {
            // Failed to read an integer
            std::cerr << "Failed to read an integer." << std::endl;
            return false;
        }

        if (current < last) {
            // File is not sorted
            return false;
        }
        last = current;
    }

    if (file.eof()) {
        // Reached end of file, so it's sorted
        return true;
    } else {
        // Some other error
        std::cerr << "File reading did not reach EOF. May have encountered an error." << std::endl;
        return false;
    }
}

int main() {
    const std::string input_file_name = "../large_file_test.txt";
    const std::string output_file_name = "../sorted_file.txt";

    float memory_size = get_memory_usage();
    sort_external_file(input_file_name, output_file_name);

    memory_size = get_memory_usage() - memory_size;
    std::cout << "[i] Used memory: " << memory_size << " MB\n";

    if (file_is_sorted(output_file_name)) {
        std::cout << "[+] The output file is correctly sorted.\n";
    } else {
        std::cout << "[-] The output file is not sorted correctly.\n";
    }

    return 0;
}

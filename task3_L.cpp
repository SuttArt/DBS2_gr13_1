// -------------------------------
// Group: 13
// Members: Charlotte Kauter, Robert Sitner, Artem Suttar
// HU-Account names: kauterch, x, y
// -------------------------------
#include <iostream>
#include <fstream>
#include <sys/resource.h>
#include <string>
//added:
#include <vector>
#include <sstream>
#include <cstdlib>
#include <algorithm>
#include <queue>

void sort_external_file(const std::string &input_file_name, const std::string &output_file_name) { 
    //proof if the input file exists
    std::ifstream infile(input_file_name);
    if (infile.is_open()) {
        //check if outfile exists
        //if: erase
        //if not: create
        //"w": Creates an empty file for writing. 
        //If a file with the same name already exists, its content is erased and the file is considered as a new empty file.
        FILE *outfile = fopen(output_file_name.c_str(), "w");
        //------------------------------------------------------
        //define some variables:

        // Define a block size (in number of integers) to fit within 50 MB of memory.
        const long long BLOCK_SIZE = 5e7/ sizeof(int);  // (47 megabytes to keep it below 50 MB memory)
        //Block counter
        int block_i = 1;
        
        while (!infile.eof()) {
            std::string line;
            std::vector<int> buffer(BLOCK_SIZE);  //define the buffer with the max size
            int i = 0;                            //index for buffer


            //read the file line by line until the buffer limit is reached
            while (i < BLOCK_SIZE && std::getline(infile, line)) {
                buffer[i] = atoi(line.c_str());
                i++;    //update buffer location
            }

            //sort the current buffer
            std::sort(buffer.begin(), buffer.end());            

            //sorted buffer to temp file---------------------------------------------------------------------------
            std::vector<std::ifstream> input_files; //to store the temp files => better access
            std::string filename = "temp_buffer_" +std::to_string(block_i) + ".txt"; //create file name
            //open in write mode and erase content
            std::ofstream tempFile(filename, std::ios::out | std::ios::trunc);//create and open a temporary file

            if (!tempFile.is_open()) {
            std::cerr << "Failed to open the temporary file." << std::endl;
            }

            //i: max pos in buffer
            for (int j = 0; j < i; j++) { 
                tempFile << buffer[j] << '\n'; //one number per line
            }
            
            tempFile.close(); //close the temp file after it
            input_files.push_back(std::ifstream(filename)); //store temp file
            block_i++; //update Block counter
            
        }
        //close input file
        infile.close();

        //have to save all first elements

        //std::priority_queue<int, std::vector<int>, 'ComparisonType> pq; //MinHeap

        /*while (!pq.empty()){

            int minValue = ;
            outfile << minValue << "\n";
        } 
        */


        fclose(outfile);
        return;
    //if not: throw exception
    }else{
        std::cerr<< "The input file does not exist\n";
    }
    
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
    const std::string input_file_name = "../large_file.txt";
    //const std::string input_file_name = "../small_file.txt";
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

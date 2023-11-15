// -------------------------------
// Group: 13
// Members:
//      Charlotte Kauter
//      Robert Sitner
//      Artem Suttar
//HU-Account names:
//      kauterch
//      sitnerro
//      suttarar
// -------------------------------
#include <iostream>
#include <fstream>
#include <sys/resource.h>
#include <string>
//added:
#include <algorithm> //sort
#include <queue>  // priority queue, vector

//for more readability
typedef std::pair<int, int> pr;



void sort_external_file(const std::string &input_file_name, const std::string &output_file_name) {
    //-------------------------------------------------------------------------------------------------SORT

    //proof if the input file exists
    std::ifstream infile(input_file_name);
    if (infile.is_open()) {

        /*check if outfile exists
            if: erase | if not: create
            "w": Creates an empty file for writing.
            If a file with the same name already exists, its content is erased and the file is considered as a new empty file.
        */

        //trunc: create when missing, delete content if not
        std::ofstream outfile(output_file_name, std::ios::out | std::ios::binary | std::ios::trunc);
        //------------------------------------------------------VARIABLES:

        const int BLOCK_SIZE = 5e7/ sizeof(int);  // Define a block size (in number of integers) to fit within 50 MB of memory.
        int block_i = 1;                                //Block counter
        std::vector<std::ifstream> tempFiles;           //to store the temp files => better access
        std::vector<std::string> str_tempFiles;         //to be able to delete them
        std::string line;                               //to read line by line

        while (!infile.eof()) { //proof if end of file is reached

            std::vector<int> buffer(BLOCK_SIZE);        //define the buffer with the max size
            int i = 0;                                  //index for buffer
            int num;

            //read the file line by line until the buffer limit is reached
            while (i < BLOCK_SIZE && infile >> num) {    //std::getline(infile, line)
                //if(!line.empty()){
                    buffer[i] =  num;           // string to integer    stoi(line);
                    i++;                                //update buffer location
                //}
            }
            //sort the current buffer
            std::sort(buffer.begin(), buffer.begin() + i);    //sort only the pos that are read

            //------------------------------------------sorted buffer to temp file

            std::string filename = "temp_buffer_" +std::to_string(block_i) + ".txt"; //create file name
                                                                               //open in write mode and erase content
            std::ofstream tempFile(filename, std::ios::out | std::ios::binary | std::ios::trunc); //create and open a temporary file

            //throw exception if file is not okay
            if (!tempFile.is_open()) {
                std::cerr << "Failed to open the temporary file." << std::endl;
            }

            //write current buffer to a temp file
            for (int j = 0; j < i; j++) {               //i: max pos in buffer
                tempFile << buffer[j] << "\n";          //write each number as one line
            }

            tempFiles.push_back(std::ifstream(filename)); //store temp file
            str_tempFiles.push_back(filename);            //store temp filenames as strings to delete them later
            block_i++; //update Block counter

        }

        infile.close();                                   //close input file
        //-------------------------------------------------------------------------------------------------MERGE

        //pq is empty
        //we create a priority queue with pairs to store:
        //1st: number
        //2nd: index of temp file in vector

        /*
           In C++ if the element is in the form of pairs,
           then by default the priority of the elements is dependent upon the first element.
           By default priority queues are max heaps.
           Therefore, we just have to use the priority queue of pairs with greater<> function object.
        */

        std::priority_queue<pr, std::vector<pr>, std::greater<pr>> pq; //stored as MinHeap

        //1st filling: take the first element of each temp file
        //NOTE: block_i is already +1
        for (int j = 0; j < (BLOCK_SIZE/block_i)/2; j++){ //only the half because of the pairs
            for(int i = 0; i < tempFiles.size() ; i++){  //take elements of each tmp file until limit is reached
                std::ifstream& tempFile = tempFiles[i];
                int element;
                if(tempFile >> element){ //to prevent occurring errors with empty lines
                    pq.push({element ,i});
                }
            }
        }

        //solution with only one value of each tempfile
        /*for (int i = 0; i < tempFiles.size() ; i++){
            std::ifstream& tempFile = tempFiles[i];
            int element;
            if(tempFile >> element){ //to prevent occurring errors with empty lines
                pq.push({element ,i});
            }
        }*/

        //NOTES:
        //Have to: .push() elements of buffer to pq
        //top() : first element (greatest prio)
        //pop() : erase first element

        //define some variables to save minValues in a vector
        std::vector<int> sort_out(BLOCK_SIZE/block_i);
        int sort_counter = 0;

        //keep going if priority queue is not empty
        while (!pq.empty()){
            //collect minValues until limit of sort buffer is reached
            if(sort_counter < BLOCK_SIZE/block_i){
                int value;                                          //to store top element of pq
                int minValue = pq.top().first;                      //number
                int tempIndex = pq.top().second;                    //temp file index
                pq.pop();                                           //erase min entry

                sort_out[sort_counter] = minValue;
                sort_counter++;

                if(!tempFiles[tempIndex].eof()){                    //proof if tempfile has elements to continue
                if(tempFiles[tempIndex] >> value){                   //take the next number of the temp file with the current min
                    pq.push({value, tempIndex});                     //add new number to priority queue
                }
                }else{
                    tempFiles[tempIndex].close();                   //close the temp file if the end is reached
                }
            }
            else{ //if vector full, write to disk
                for(int i = 0; i < sort_counter; i++){
                    outfile << sort_out[i] << "\n" ;
                }
                sort_counter = 0;                           //reset values
                std::vector<int> sort_out(BLOCK_SIZE/block_i);
            }


        }

        //when buffer is not empty then write to disk
        if(sort_out[0] != 0){
            for(int i = 0; i < sort_counter; i++){
                outfile << sort_out[i] << "\n" ;
            }
        }



        for (int i = 0 ; i < str_tempFiles.size(); i++){       //delete all temp files after finishing the merge step
            std::remove(str_tempFiles[i].c_str());
        }

        outfile.close();
        return;

    }else{                                                     //throw exception if input file does not exist
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

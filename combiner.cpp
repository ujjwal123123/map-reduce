#include <cassert>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <pthread.h>
#include <queue>
#include <sstream>
#include <string>
#include <sys/wait.h>
#include <tuple>
#include <unistd.h>
#include <vector>

struct parsed_tuple {
    int user_id;
    std::string topic;
    int score;
};

struct MapperArgs {
    int slot_count;
    int user_count;
};

int const END_OF_STREAM = -2;

void handle_error(const char *msg, int line) {
    std::cerr << "Error: " << __FILE__ << ":" << line << " " << msg << std::endl
              << std::flush;
    exit(EXIT_FAILURE);
}

template <typename T> class sharedQueue {
  private:
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t not_empty = PTHREAD_COND_INITIALIZER;
    pthread_cond_t not_full = PTHREAD_COND_INITIALIZER;
    parsed_tuple *array;
    int start = 0;
    int end = 0;
    int queue_size = 0;
    int capacity;

    void assertSize() {
        assert((capacity + end - start) % capacity == queue_size);
    }

  public:
    sharedQueue(int capacity) {
        pthread_mutex_init(&mutex, NULL);
        this->array = new parsed_tuple[capacity];
        this->capacity = capacity;
    }

    ~sharedQueue() {
        pthread_mutex_destroy(&mutex);
        // delete[] array;
    }

    // producer
    void push(parsed_tuple t) {
        int s = pthread_mutex_lock(&mutex);
        if (s != 0) {
            handle_error("Error locking mutex", __LINE__);
        }

        while (queue_size == capacity) {
            s = pthread_cond_wait(&not_full, &mutex);
            if (s != 0) {
                handle_error("Error waiting on condition variable", __LINE__);
            }
        }

        array[end] = t;
        end = (end + 1) % capacity;
        queue_size++;
        assertSize();

        s = pthread_mutex_unlock(&mutex);
        if (s != 0) {
            handle_error("Error unlocking mutex", __LINE__);
        }
        s = pthread_cond_signal(&not_empty);
        if (s != 0) {
            handle_error("Error signaling condition variable", __LINE__);
        }
        std::cout << "Pushed to index " << this->end << ": " << t.user_id << " "
                  << t.score << " " << t.topic << std::endl;
        std::cout << std::endl;
    }

    parsed_tuple pop() {
        int s = pthread_mutex_lock(&mutex);
        if (s != 0) {
            handle_error("Error locking mutex", __LINE__);
        }

        while (queue_size == 0) {
            s = pthread_cond_wait(&not_empty, &mutex);
            if (s != 0) {
                handle_error("Error waiting on condition variable", __LINE__);
            }
        }

        parsed_tuple return_val = array[start];
        start = (start + 1) % capacity;
        queue_size--;
        assertSize();

        s = pthread_mutex_unlock(&mutex);
        if (s != 0) {
            handle_error("Error unlocking mutex", __LINE__);
        }
        s = pthread_cond_signal(&not_full);
        if (s != 0) {
            handle_error("Error signaling condition variable", __LINE__);
        }

        return return_val;
    }

    int size() { return queue_size; }
};

void parseLineMapper(std::string &line, int &user_id, char &action,
                     std::string &topic) {
    std::stringstream ss(line);

    std::string substr;

    std::getline(ss, substr, ',');
    user_id = std::stoi(substr);

    std::getline(ss, substr, ',');
    action = substr[0];

    std::getline(ss, substr, ',');
    topic = substr;
}

void printScores(std::map<std::string, int> &scores, int user_id) {
    // print the scores
    for (auto &topic : scores) {
        std::cout << "(" << std::setfill('0') << std::setw(4) << user_id << ","
                  << topic.first << "," << topic.second << ")" << std::endl;
    }
}

void *reducer(void *args) {
    sharedQueue<parsed_tuple> user_queue =
        *static_cast<sharedQueue<parsed_tuple> *>(args);

    std::map<std::string, int> score_table;
    int const NOT_ASSIGNED = -1;
    int user_id = NOT_ASSIGNED;

    while (user_queue.size() > 0) {
        parsed_tuple tuple = user_queue.pop();

        std::stringstream ss;
        ss << "Popped " << tuple.user_id << " " << tuple.score << " "
           << tuple.topic << std::endl;
        std::cout << ss.str() << std::flush;

        if (tuple.user_id == END_OF_STREAM) {
            printScores(score_table, user_id);
            pthread_exit(NULL);
        } else if (user_id == NOT_ASSIGNED) {
            user_id = tuple.user_id;
        }

        assert(user_id == tuple.user_id);
        assert(user_id != NOT_ASSIGNED);
        assert(user_id != END_OF_STREAM);

        if (score_table.find(tuple.topic) != score_table.end()) {
            score_table[tuple.topic] += tuple.score;
        } else {
            score_table[tuple.topic] = tuple.score;
        }
    }

    pthread_exit(NULL);
}

void *mapper(void *args) {
    std::vector<sharedQueue<parsed_tuple>> queues =
        *static_cast<std::vector<sharedQueue<parsed_tuple>> *>(args);
    std::map<char, int> scoring_rules = {
        {'P', 50}, {'L', 20}, {'D', -10}, {'C', 30}, {'S', 40}};

    std::map<int, int> user_to_queue_map;
    long unsigned int next_queue_index = 0;

    // read from stdin line by line
    std::string line;
    while (std::getline(std::cin, line)) {
        line = line.substr(1, line.length() - 2);

        std::tuple<int, char, std::string> tuple;
        int user_id;
        char action;
        std::string topic;

        parseLineMapper(line, user_id, action, topic);

        int score = scoring_rules[action];

        if (user_to_queue_map.find(user_id) == user_to_queue_map.end()) {
            assert(next_queue_index < queues.size());

            user_to_queue_map[user_id] = next_queue_index;
            next_queue_index = next_queue_index + 1;
        }

        int queueIndex = user_to_queue_map[user_id];
        sharedQueue<parsed_tuple> &queue = queues.at(queueIndex);
        std::stringstream ss;
        ss << "Pushing to queue " << queueIndex << std::endl;
        std::cout << ss.str() << std::flush;
        queue.push(parsed_tuple{user_id, topic, score});
    }

    for (auto &queue : queues) {
        // push a sentinel value to indicate the end of the stream
        queue.push(parsed_tuple{END_OF_STREAM, "", END_OF_STREAM});
    }

    pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0]
                  << " [number of slots] [number of users or threads]"
                  << std::endl;
        return 1;
    }

    int slot_count = std::stoi(argv[1]);
    int user_count = std::stoi(argv[2]);

    // create queues for each user
    std::vector<sharedQueue<parsed_tuple>> queues;
    for (int i = 0; i < user_count; i++) {
        sharedQueue<parsed_tuple> queue(slot_count);
        queues.push_back(queue);
    }

    // create mapper thread
    pthread_t mapper_thread;
    pthread_create(&mapper_thread, NULL, mapper, &queues);

    // create reducer threads for each user
    std::vector<pthread_t> reducer_threads(user_count);
    for (int i = 0; i < user_count; i++) {
        pthread_create(&reducer_threads[i], NULL, reducer, &queues.at(i));
    }

    pthread_join(mapper_thread, NULL);
    for (int i = 0; i < user_count; i++) {
        pthread_join(reducer_threads.at(i), NULL);
    }

    pthread_exit(NULL);
}
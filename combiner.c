#include <assert.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int const END_OF_STREAM = -2;

struct parsed_tuple {
    int user_id;
    char *topic;
    int score;
};

struct MapperArgs {
    struct shared_queue **queues;
    int queue_count;
    int capacity;
};

struct shared_queue {
    int size;
    int start;
    int end;
    int capacity;
    struct parsed_tuple **data;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
};

struct shared_queue *shared_queue_init(int capacity) {
    struct shared_queue *queue =
        (struct shared_queue *)malloc(sizeof(struct shared_queue));
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->not_empty, NULL);
    pthread_cond_init(&queue->not_full, NULL);
    queue->size = 0;
    queue->start = 0;
    queue->end = 0;
    queue->capacity = capacity;
    queue->data =
        (struct parsed_tuple **)malloc(sizeof(struct parsed_tuple) * capacity);
    return queue;
}

void shared_queue_destroy(struct shared_queue *queue) {
    free(queue->data);
    free(queue);
}

void shared_queue_push(struct shared_queue *queue, struct parsed_tuple *tuple) {
    pthread_mutex_lock(&queue->mutex);

    while (queue->size == queue->capacity) {
        pthread_cond_wait(&queue->not_full, &queue->mutex);
    }

    assert(queue->size < queue->capacity);
    assert(queue->end < queue->capacity);
    assert(queue->end < queue->capacity);
    queue->data[queue->end] = tuple;

    queue->end = (queue->end + 1) % queue->capacity;
    queue->size++;
    pthread_mutex_unlock(&queue->mutex);
    pthread_cond_signal(&queue->not_empty);
}

struct parsed_tuple *shared_queue_pop(struct shared_queue *queue) {
    pthread_mutex_lock(&queue->mutex);
    while (queue->size == 0) {
        pthread_cond_wait(&queue->not_empty, &queue->mutex);
    }

    struct parsed_tuple *tuple = queue->data[queue->start];
    queue->start = (queue->start + 1) % queue->capacity;
    queue->size--;
    pthread_mutex_unlock(&queue->mutex);
    pthread_cond_signal(&queue->not_full);
    return tuple;
}

void *reducer(void *args) {
    struct shared_queue *queue = (struct shared_queue *)args;

    int const NOT_ASSIGNED = -1;
    int user_id = NOT_ASSIGNED;

    struct topic_score {
        char *topic;
        int score;
    } topic_scores[100];

    for (int i = 0; i < 100; i++) {
        topic_scores[i].topic = (char *)malloc(256 * sizeof(char));
        topic_scores[i].score = -1;
    }

    while (true) {
        struct parsed_tuple *tuple = shared_queue_pop(queue);

        if (tuple->user_id == END_OF_STREAM) {
            break;
        }

        int scores_index = -1;

        for (int i = 0; i < 100; i++) {
            if (topic_scores[i].score == -1) {
                scores_index = i;
                topic_scores[i].score = 0;
                break;
            } else if (strcmp(topic_scores[i].topic, tuple->topic) == 0) {
                scores_index = i;
                break;
            }
        }

        strcpy(topic_scores[scores_index].topic, tuple->topic);
        topic_scores[scores_index].score += tuple->score;

        user_id = tuple->user_id;
    }

    for (int i = 0; i < 100; i++) {
        if (topic_scores[i].score == -1) {
            break;
        }

        printf("(%04d,%s,%d)\n", user_id, topic_scores[i].topic,
               topic_scores[i].score);
    }

    return NULL;
}

int get_score(char action) {
    switch (action) {
    case 'P':
        return 50;
    case 'L':
        return 20;
    case 'D':
        return -10;
    case 'C':
        return 30;
    case 'S':
        return 40;
    default:
        return 0;
    }
}

void *mapper(void *args) {
    struct MapperArgs *mapper_args = (struct MapperArgs *)args;

    int next_queue_index = 0;
    struct user_id_to_queue_index {
        int user_id;
        int queue_index;
    };

    struct user_id_to_queue_index
        user_id_to_queue_indexes[mapper_args->queue_count];

    for (int i = 0; i < mapper_args->queue_count; i++) {
        user_id_to_queue_indexes[i].user_id = -1;
        user_id_to_queue_indexes[i].queue_index = -1;
    }

    while (!feof(stdin)) {
        char line[256];
        if (scanf(" %s", line) != EOF) {
            char *string = line + 1;
            int user_id = atoi(strtok(string, ","));
            char action = strtok(NULL, ",")[0];
            char *topic = (char *)malloc(256 * sizeof(char));
            strcpy(topic, strtok(NULL, ")"));

            struct parsed_tuple *tuple =
                (struct parsed_tuple *)malloc(sizeof(struct parsed_tuple));
            tuple->user_id = user_id;
            tuple->topic = topic;
            tuple->score = get_score(action);

            int queue_index = -1;

            for (int i = 0; i < mapper_args->queue_count; i++) {
                if (user_id_to_queue_indexes[i].user_id == user_id) {
                    queue_index = user_id_to_queue_indexes[i].queue_index;
                    break;
                }
            }

            if (queue_index == -1) {
                queue_index = next_queue_index;
                next_queue_index += 1;
                user_id_to_queue_indexes[queue_index].user_id = user_id;
                user_id_to_queue_indexes[queue_index].queue_index = queue_index;
            }

            assert(queue_index < mapper_args->queue_count);
            assert(queue_index >= 0);
            shared_queue_push(mapper_args->queues[queue_index], tuple);
        }
    }

    for (int i = 0; i < mapper_args->queue_count; i++) {
        struct parsed_tuple tuple = {END_OF_STREAM, "", 0};
        shared_queue_push(mapper_args->queues[i], &tuple);
    }

    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        printf("Usage: %s [slots count] [number of users]\n", argv[0]);
        return 1;
    }

    int slots_count = atoi(argv[1]);
    int users_count = atoi(argv[2]);

    // create queue
    struct shared_queue *queues[users_count];

    for (int i = 0; i < users_count; i++) {
        queues[i] = shared_queue_init(slots_count);
    }

    struct MapperArgs *mapper_args =
        (struct MapperArgs *)malloc(sizeof(struct MapperArgs));

    mapper_args->queues = queues;
    mapper_args->queue_count = users_count;
    mapper_args->capacity = slots_count;

    pthread_t mapper_thread;
    pthread_create(&mapper_thread, NULL, mapper, mapper_args);

    pthread_t reducer_thread[users_count];
    for (int i = 0; i < users_count; i++) {
        pthread_create(&reducer_thread[i], NULL, reducer, queues[i]);
    }

    pthread_join(mapper_thread, NULL);
    for (int i = 0; i < users_count; i++) {
        pthread_join(reducer_thread[i], NULL);
    }
}

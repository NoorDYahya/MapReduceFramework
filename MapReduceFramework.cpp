//
// Created by noor dar yahya on 15/05/2023.
//

#include "MapReduceFramework.h"
#include "./Barrier/Barrier.h"
#include <map>
#include <algorithm>
#include <iostream>
#include <pthread.h>
#include <vector>
#include<atomic>

#define BIT31  (uint64_t)1 << 31
const uint64_t one64BIT = 1;
const uint64_t two64BIT = 2;
const uint64_t thirtyThree64BIT = 33;
const uint64_t thirtyOne64BIT = 31;
const uint64_t sixtyTwo64BIT = 62;


struct sortting
{
    bool operator()(const K2 *k, const K2 *k_) const
    {
        return *k < *k_;
    }

};




struct Mutex
{
    pthread_mutex_t map_mutex;
    pthread_mutex_t emit2_mutex;
    pthread_mutex_t shuffle_mutex;
    pthread_mutex_t reduce_mutex;
    pthread_mutex_t emit3_mutex;
    pthread_mutex_t cycle_mutex;
    pthread_mutex_t reduce_init_mutex;
    pthread_mutex_t wait_for_job_mutex;
    pthread_mutex_t log_print_mutex;
};
struct dataBases
{
    std::map<K2 *, IntermediateVec , sortting>& shufflePhase;
    std::map<pthread_t, IntermediateVec *> &threadIntermadiateVec; // maps
    // thread
    // ids to intermediate vectors
//    std::map<K2 *, IntermediateVec> &shuffleMap; // maps k2 To vector of v2s
    // (for
    // shuffling)
    std::vector<IntermediateVec> &queue;
    std::vector<IntermediateVec *> &remember_vec;
};
struct JobContext
{
    MapReduceClient &client;
  InputVec &inputVec;
    OutputVec &outputVec;
    int multiThreadLevel;
    JobState state;
    Mutex mutex;
    Barrier *barrier;

    ///counters
    std::atomic<uint64_t> count_size;
    std::atomic<uint64_t> count_prec;
    uint64_t iterLong;

    ///data
    dataBases *data;
//    std::map<K2 *, IntermediateVec *, sortting> mapPhase;
    pthread_t *threads;
    pthread_t first_th;
//    std::map<pthread_t,IntermediateVec*>& id_to_vec_map; // maps thread ids to intermediate vectors
//    std::map<K2*,IntermediateVec>& shuffleMap; // maps k2 To vector of v2s
//    // (for
//    // shuffling)
//    std::vector<IntermediateVec> &queue;
//    std::vector<IntermediateVec*>& remember_vec;

    //// find the fisrt iteration and first thread
    bool already_waited;
    bool first_iter;


};


void check(int i)
{
    if (i != 0)
    {
        std::cout << "system error : create thread failed" << std::endl;
        exit(1);
    }
}


void lock(pthread_mutex_t *J)
{
    if (pthread_mutex_lock(J) != 0)
    {
        fprintf(stderr, "[[Barrier]] error on pthread_mutex_lock");
        exit(1);
    }
}

void unlock(pthread_mutex_t *J)
{
    if (pthread_mutex_unlock(J) != 0)
    {
        fprintf(stderr, "[[Barrier]] error on pthread_mutex_unlock");
        exit(1);
    }

}

void updatePrecentage(JobContext *j, const int stage, const int jobNum)
{
    unsigned long long count = ((BIT31 * stage) + jobNum);
    count = count << thirtyOne64BIT;
    j->count_prec = count;
}


                /*                PHASES               */
void map(JobContext *job)
{
    uint64_t old_value = 0;
    uint64_t z = pow(2, 31) - 1;
    int input_size = job->inputVec.size();
    while ((old_value & z) < input_size - 1)
    {
        lock(&job->mutex.map_mutex);
        old_value = job->count_size.load();
        if ((old_value & z) < input_size)
        {
            job->count_size++;
            job->count_prec++;
            InputPair pair = job->inputVec[(int) (old_value & z)];
            job->client.map(pair.first, pair.second, job);
            unlock(&job->mutex.map_mutex);
            continue;
        }
        unlock(&job->mutex.map_mutex);
        break;
    }
}

void sort(JobContext *job)
{
      IntermediateVec* cur = job->data->threadIntermadiateVec[pthread_self()];
      std::sort(cur->begin(),cur->end());
}

void shuffle(JobContext *job)
{      uint64_t inputS = job->inputVec.size();
      if(pthread_self() == job->first_th){
          lock(&job->mutex.shuffle_mutex);
          updatePrecentage(job,2,job->data->threadIntermadiateVec.size());
          job->count_size -= inputS;
          for(auto &id : job->data->threadIntermadiateVec){
              IntermediateVec *cur_vec = id.second;
              while(!cur_vec->empty()){
                  IntermediatePair cur_pair = cur_vec->back();
                  cur_vec->pop_back();
                  bool found_item = false;
                  if (job->data->shufflePhase.empty()){
                      job->data->shufflePhase[cur_pair.first].push_back
                      (cur_pair);
                      (job->count_size)++;  // count number of vectors in queue
                      continue;
                  }
                  else{
                      for(auto &elem: job->data->shufflePhase){
                          if(!((*elem.first < *cur_pair.first)||(*cur_pair.first < *elem.first))){
                              // cur pair's key already in the map
                              job->data->shufflePhase[elem.first].push_back
                              (cur_pair);
                              found_item = true;
                          }
                      }
                      if(!found_item){
                          auto * new_vec = new IntermediateVec();
                          job->data->remember_vec.push_back(new_vec);
                          new_vec->push_back(cur_pair);
                          (job->count_size)++;  // count number of vectors in queue
                          job->data->shufflePhase[cur_pair.first] = *new_vec;
                      }
                  }
              }
              job->count_prec++;
          }
          updatePrecentage(job, 3, job->data->shufflePhase.size());
          unlock(&job->mutex.shuffle_mutex);

      }
}
void fillQuery(JobContext *job){
    for(auto& vec : job->data->shufflePhase){
        IntermediateVec add ;
        for(auto& p : vec.second){
            add.push_back(p);
        }
        job->data->queue.push_back(add);
    }
}
void reduce(JobContext *job)
{

    lock(&job->mutex.reduce_mutex);
    if (job->first_iter){
        job->first_iter = false;
        uint64_t size = ((job->count_size << two64BIT) >> thirtyThree64BIT) <<
                thirtyOne64BIT; // setting the middle section to zero
        job->count_size -= size;
        fillQuery(job);
    }
    unlock(&job->mutex.reduce_mutex);

    lock(&job->mutex.reduce_mutex);

    uint64_t z= pow(2, 31) - 1;
    while((job->count_size << two64BIT) >> thirtyThree64BIT < ((job->count_size)&z)){
        // mid
        // ac
        // smaller then right part (queue size)
        int q_size = job->data->queue.size();
        if(q_size == 0){ break;}
        IntermediateVec v;
        v = job->data->queue[q_size-1];
        job->iterLong = v.size();
        job->data->queue.pop_back();
        job->count_prec ++;
        job->count_size += one64BIT << thirtyThree64BIT; // +1 to middle section
        job->client.reduce(&v,job);
    }
    unlock(&job->mutex.reduce_mutex);
}

void* cycle(void* val)
{
    auto *job = (JobContext*) val;
    lock(&job->mutex.cycle_mutex);
    auto *cur_vec = new std::vector<IntermediatePair>();
    if(job->data->threadIntermadiateVec.empty()){
        job->first_th = pthread_self();
    }
    if(job->count_prec >> 62 == 0){ updatePrecentage(job, 1, job->inputVec.size
    ());}
    job->data->threadIntermadiateVec.insert({pthread_self(),cur_vec}); // adding zero thread
    unlock(&job->mutex.cycle_mutex);
    map(job);

    sort(job);

    job->barrier->barrier();

    shuffle(job);

    job->barrier->barrier();

    reduce(job);
    return val;
}


                 /*                functions               */
void *mapReduceFun(void *context)
{

//    auto *contextT = static_cast<Context *> (context);
//    JobContext *jonEnv = contextT->env;
//    jonEnv->state.stage = MAP_STAGE;
//    //// lock
//    while (!(jonEnv->inputVec.empty()))
//    { // threre are sstill missions to do
//        InputPair p = jonEnv->inputVec.back();
//        jonEnv->inputVec.pop_back();
//        unlock(jonEnv);
//        jonEnv->client.map(p.first, p.second, context);
//
//        lock(jonEnv);
//    }
//    /// sorting and shuffle
//    unlock(jonEnv);
//    jonEnv->barrier->barrier();
//    jonEnv->state.stage = SHUFFLE_STAGE;
//    if (contextT->id == 0)
//    {
//        shuffle(jonEnv);
//    }
//    ///// reduce
//    jonEnv->barrier->barrier();
//    jonEnv->state.stage = REDUCE_STAGE;
//    lock(jonEnv);
//    while (!jonEnv->mapPhase.empty())
//    {
//        IntermediateVec *v = jonEnv->mapPhase.begin()->second;
//        jonEnv->mapPhase.erase(jonEnv->mapPhase.begin());
//        unlock(jonEnv);
//        jonEnv->client.reduce(v, context);
//        lock(jonEnv);
//    }
//    unlock(jonEnv);
//    return context;


}
void destroyMutex(JobContext* job){

        pthread_mutex_destroy(&job->mutex.reduce_mutex);
    pthread_mutex_destroy(&job->mutex.map_mutex);
    pthread_mutex_destroy(&job->mutex.wait_for_job_mutex);
    pthread_mutex_destroy(&job->mutex.cycle_mutex);
    pthread_mutex_destroy(&job->mutex.shuffle_mutex);
    pthread_mutex_destroy(&job->mutex.log_print_mutex);
    pthread_mutex_destroy(&job->mutex.reduce_init_mutex);
    pthread_mutex_destroy(&job->mutex.emit2_mutex);
    pthread_mutex_destroy(&job->mutex.emit3_mutex);

}
JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{
    Mutex* mutexess = new Mutex();
//    pthread_mutex_t *mutexes = new pthread_mutex_t [9];
    pthread_t *threads = new pthread_t[multiThreadLevel];
    auto id_to_vec_map = new std::map<pthread_t,IntermediateVec*>;
    auto key_to_vec_map = new std::map<K2*,IntermediateVec, sortting>;
    auto remember_vec = new std::vector<IntermediateVec*>;
    auto queue = new std::vector<IntermediateVec>;
    mutexess->reduce_mutex = PTHREAD_MUTEX_INITIALIZER;
    mutexess->map_mutex = PTHREAD_MUTEX_INITIALIZER;
    mutexess->shuffle_mutex = PTHREAD_MUTEX_INITIALIZER;
    mutexess->cycle_mutex = PTHREAD_MUTEX_INITIALIZER;
    mutexess->log_print_mutex = PTHREAD_MUTEX_INITIALIZER;
    mutexess->wait_for_job_mutex = PTHREAD_MUTEX_INITIALIZER;
    mutexess->reduce_init_mutex = PTHREAD_MUTEX_INITIALIZER;
    mutexess->emit2_mutex = PTHREAD_MUTEX_INITIALIZER;
    mutexess->emit3_mutex = PTHREAD_MUTEX_INITIALIZER;

    JobContext *job ;
    job->threads = threads;
    job->data->threadIntermadiateVec = *id_to_vec_map;
    job->data->shufflePhase =  *key_to_vec_map;
    job->data->queue = *queue;
    job->data->remember_vec = *remember_vec;
    job->mutex = *mutexess;
    job->inputVec = inputVec;
    job->client = client;
    job->outputVec = outputVec;
    job->barrier = new Barrier(multiThreadLevel);
    job->multiThreadLevel = multiThreadLevel;
    job->already_waited = false;
    job->first_iter = true;
    job->count_size = 0;
    job->count_prec = 0;


    pthread_mutex_t init_mutex = PTHREAD_MUTEX_INITIALIZER;
    lock(&init_mutex);
    for (int i = 0; i < multiThreadLevel; ++i){
        int res = pthread_create(&job->threads[i], NULL, &cycle, job);
        check(res);}
    unlock(&init_mutex);
    destroyMutex(job);
    delete mutexess;
    mutexess = nullptr;
    auto jc = static_cast<JobHandle>(job);
    return jc;
//    JobState state;
//    state.stage = UNDEFINED_STAGE;
//    state.percentage = 0;
//
//
//    Barrier barrier(multiThreadLevel);
//    IntermediateVec intermediateVectors;
//    std::map<K2 *, IntermediateVec *, sortting> mapPhase;
//    Barrier b(multiThreadLevel);
//    JobContext *job{};
//
//    job->inputVec = inputVec;
//    job->state = state;
//    job->barrier = &b;
//    job->count_size = inputVec.size();
//    job->mutex = PTHREAD_MUTEX_INITIALIZER;
//    job->threads = new pthread_t *[multiThreadLevel];
//    job->contexts = new Context *[multiThreadLevel];
//    for (int i = 0; i < multiThreadLevel; ++i)
//    {
//        Context *c;
//        c->id = i;
//        c->env = job;
//        job->contexts[i] = c;
//        int res = pthread_create(job->threads[i], NULL,
//                                 &mapReduceFun, job->contexts[i]);
//        check(res);
//    }
//    for (int i = 0; i < multiThreadLevel; ++i)
//    {
//        pthread_join(*job->threads[i], NULL);
//    }
//
//    return job;

}
//
void waitForJob(JobHandle job)
{

    auto jc = (JobContext *) job;
    lock(&jc->mutex.wait_for_job_mutex);

    if (!jc->already_waited) {
        for (int i = 0; i < jc->multiThreadLevel; ++i) {
            pthread_t tid = jc->threads[i];
            int ret = pthread_join(tid, nullptr);
            check(ret);
        }
        jc->already_waited = true;
    }
    unlock(&jc->mutex.wait_for_job_mutex);

}
//
void getJobState(JobHandle job, JobState *state)
{
    auto jc = (JobContext *) job;
    lock(&jc->mutex.wait_for_job_mutex);

    if (!jc->already_waited) {
        for (int i = 0; i < jc->multiThreadLevel; ++i) {
            pthread_t tid = jc->threads[i];
            int ret = pthread_join(tid, nullptr);
            check(ret);
        }
        jc->already_waited = true;
    }
    unlock(&jc->mutex.wait_for_job_mutex);

//    auto *jobN = static_cast<JobContext *> (job);
//    lock(jobN);
//    unsigned long atomic_c = jobN->count_prec.load();
//    unsigned long processed = atomic_c % BIT31;
//    unsigned long size = (atomic_c << two64BIT) >> thirtyThree64BIT;
//    state->stage = (stage_t) (atomic_c >> sixtyTwo64BIT);
//    state->percentage = ((float) processed / (float) size) * 100;
//    unlock(jobN);
}
//
void emit2(K2 *key, V2 *value, void *context)
{
    JobContext* job = (JobContext*) context;
    pthread_t tid = pthread_self();
    lock(&job->mutex.emit2_mutex);
    if (job->data->threadIntermadiateVec.find(tid)==job->data->threadIntermadiateVec.end()){
        // no vector
        // corresponds to self thread id
        IntermediateVec *inter_vec;
        job->data->threadIntermadiateVec.insert({tid, inter_vec});
    }
    job->data->threadIntermadiateVec[tid]->push_back(IntermediatePair(key,value));  //
    // adding the pair to the needed vector
    unlock(&job->mutex.emit2_mutex);
    uint64_t inc = one64BIT << thirtyOne64BIT;
    job->count_size += inc;
//    auto *con = static_cast<Context *> (context);
//
//    con->interVector.push_back(IntermediatePair(key, value));
}
//
void emit3(K3 *key, V3 *value, void *context)
{
    auto jc = (JobContext*) context;
    lock(&jc->mutex.emit3_mutex);
    jc->outputVec.push_back(OutputPair(key,value));
//    uint64_t cur_shifter_len = jc->cur_inter_len ;
//    jc->length_ac += cur_shifter_len;
    lock(&jc->mutex.emit3_mutex);
//    Context *con = static_cast<Context *> (context);
//    con->env->outputVec.push_back(OutputPair(key, value));
//    (con->atomic_counter)++;
}
//
void closeJobHandle(JobHandle job)
{
    waitForJob(job);
    auto j = (JobContext*) job;
    delete j->barrier;
    for (auto &elem: j->data->threadIntermadiateVec){
        delete elem.second;
    }
    for (auto elem: j->data->remember_vec){
        delete elem;
    }
    free(j->threads); j->threads = nullptr;

    delete &j->data->remember_vec;
    delete &j->data->threadIntermadiateVec;
    delete &j->data->shufflePhase;
    delete &j->data->queue;
    delete j;
//    waitForJob(job);
//    auto j = (JobContext *) job;
//    delete j->threads;
//    delete j->contexts;
//    delete j->barrier;
//    delete &j->mapPhase;
//    delete j;
}

/// 1-percentage
/// 2- context
/// 3- context struct
/// 4- by all orded
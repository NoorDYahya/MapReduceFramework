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
struct JobContext;

struct sortting
{
    bool operator()(const K2 *k, const K2 *k_) const
    {
        return *k < *k_;
    }

};
std::map<pthread_t,IntermediateVec*>& id_to_vec_map; // maps thread ids to intermediate vectors
std::map<K2*,IntermediateVec>& key_to_vec_map; // maps k2 To vector of v2s (for shuffling)
std::vector<IntermediateVec> &queue;
std::vector<IntermediateVec*>& remember_vec;
const MapReduceClient &client;
const InputVec& input_vec;
OutputVec& out_vec;
Barrier *barrier;
pthread_t zero_thread;
int num_threads;
bool already_waited;
bool first_iter;
pthread_t *threads;
uint64_t cur_inter_len;
//    std::atomic<uint64_t> length_ac;
std::atomic<uint64_t> ac;
std::atomic<uint64_t> percentage_ac;
mutex_struct mutexes;
struct JobContext
{
    const MapReduceClient &client;
    int multiThreadLevel;
    JobState state;
    pthread_mutex_t mutex;
    Barrier* barrier;
    std::atomic<int> count_size;
    std::map<K2 *, IntermediateVec *, sortting> mapPhase;
    pthread_t** threads;
    InputVec &inputVec;
    OutputVec &outputVec;

};


void check(int i)
{
    if (i != 0)
    {
        std::cout << "system error : create thread failed" << std::endl;
        exit(1);
    }
}

void shuffle(JobContext *job)
{
    for (int i = 0; i < job->multiThreadLevel; i++)
    {
        IntermediateVec v = job->contexts[i]->interVector;
        for (auto p : v)
        {
            if (job->mapPhase.find(p.first) == job->mapPhase.end())
            {
                job->mapPhase[p.first] = new IntermediateVec();
            } else
            {
                job->mapPhase[p.first]->push_back(p);
            }


        }
    }
    job->count_size = job->mapPhase.size();

}

void lock(JobContext* J)
{
    if (pthread_mutex_lock(&(J->mutex)) != 0)
    {
        fprintf(stderr, "[[Barrier]] error on pthread_mutex_lock");
        exit(1);
    }
}

void unlock(JobContext *J)
{
    if (pthread_mutex_unlock(&(J->mutex)) != 0)
    {
        fprintf(stderr, "[[Barrier]] error on pthread_mutex_unlock");
        exit(1);
    }

}

void *mapReduceFun(void *context)
{
    auto *contextT = static_cast<Context *> (context);
    JobContext* jonEnv = contextT->env;
    jonEnv->state.stage = MAP_STAGE;
    //// lock
    while (!(jonEnv->inputVec.empty()))
    { // threre are sstill missions to do
        InputPair p = jonEnv->inputVec.back();
        jonEnv->inputVec.pop_back();
        unlock(jonEnv);
        jonEnv->client.map(p.first, p.second, context);

        lock(jonEnv);
    }
    /// sorting and shuffle
    unlock(jonEnv);
    jonEnv->barrier->barrier();
    jonEnv->state.stage = SHUFFLE_STAGE;
    if (contextT->id == 0)
    {
        shuffle(jonEnv);
    }
    ///// reduce
    jonEnv->barrier->barrier();
    jonEnv->state.stage = REDUCE_STAGE;
    lock(jonEnv);
    while ( !jonEnv->mapPhase.empty() )
    {
        IntermediateVec* v = jonEnv->mapPhase.begin()->second;
        jonEnv->mapPhase.erase(jonEnv->mapPhase.begin());
        unlock(jonEnv);
        jonEnv->client.reduce(v, context);
        lock(jonEnv);
    }
    unlock(jonEnv);
    return context;

}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{
    JobState state;
    state.stage = UNDEFINED_STAGE;
    state.percentage = 0;


    Barrier barrier(multiThreadLevel);
    IntermediateVec intermediateVectors;
    std::map<K2 *, IntermediateVec *, sortting> mapPhase;
    Barrier b(multiThreadLevel);
    JobContext* job{};

    job->inputVec = inputVec;
    job->state = state;
    job->barrier = &b;
    job->count_size = inputVec.size();
    job->mutex= PTHREAD_MUTEX_INITIALIZER;
    job->threads = new pthread_t*[multiThreadLevel];
    job->contexts = new Context*[multiThreadLevel];
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        Context* c ;
        c->id = i;
        c->env = job;
        job->contexts[i] = c;
        int res = pthread_create(job->threads[i], NULL,
                                 &mapReduceFun, job->contexts[i]);
        check(res);
    }
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_join(*job->threads[i], NULL);
    }

    return job;

}

void waitForJob(JobHandle job)
{

    auto *jobN = static_cast<JobContext *> (job);
    for (int i = 0; i < jobN->multiThreadLevel; i++)
    {

       int r =  pthread_join(*jobN->threads[i], NULL);
        check(r);

    }

}

void getJobState(JobHandle job, JobState *state)
{

    auto *jobN = static_cast<JobContext *> (job);
    lock(jobN);
    float  per = 0;
    if(jobN->state.stage == UNDEFINED_STAGE){
        per = 0;

    }
    if(jobN->state.stage == MAP_STAGE){
        per = jobN->inputVec.size() /(jobN->count_size);
    }
    if(jobN->state.stage == REDUCE_STAGE){
        per = jobN->mapPhase.size() /(jobN->count_size);
    }
    if(jobN->state.stage == SHUFFLE_STAGE){
        per = 0.5;
    }
    state->percentage = 100 * (1 -per );
    state->stage = jobN->state.stage;
//    jobN->state.stage= state->stage;
//    jobN->state.percentage = state->percentage;
    unlock(jobN);
}

void emit2(K2 *key, V2 *value, void *context)
{
    auto *con = static_cast<Context *> (context);

    con->interVector.push_back(IntermediatePair (key, value));
}

void emit3(K3 *key, V3 *value, void *context)
{
    Context *con = static_cast<Context *> (context);
    con->env->outputVec.push_back(OutputPair(key, value));
    (con->atomic_counter)++;
}
void closeJobHandle(JobHandle job){
    waitForJob(job);
    auto j = (JobContext*) job ;
    delete j->threads;
    delete j->contexts;
    delete j->barrier;
    delete &j->mapPhase;
    delete j;
}

/// 1-percentage
/// 2- context
/// 3- context struct
/// 4- by all orded
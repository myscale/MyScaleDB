#include <iostream>
#include <random>
#include "../VectorSegmentExecutor.h"


using namespace VectorIndex;
int main()
{
    int dimension = 64;
    int nt = 1000;
    int k = 20;
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;
    float * train = new float[nt * dimension];
    float * train2 = new float [nt * dimension];
    std::cout << "I haven't started yet\n";
    for (size_t i = 0; i < nt * dimension*2; i++)
    {
        if(i<nt*dimension){train[i] = distrib(rng);}
        else{train2[i-nt*dimension] = random();}
    }
    std::cout << "train dataset generated\n";

    VectorSegmentExecutorPtr a = std::make_shared<VectorSegmentExecutor>(dimension, IndexType(IndexType::IVFPQ), IndexMode(CPU), "test");
    VectorSegmentExecutorPtr additional = std::make_shared<VectorSegmentExecutor>(dimension, IndexType(IndexType::IVFPQ), IndexMode(CPU), "test2");
    std::cout << "a generated\n";
    DatasetPtr data_set = std::make_shared<Dataset>(nt, dimension, train);
    DatasetPtr data_set2 = std::make_shared<Dataset>(nt, dimension, train2);

    a->buildIndex(data_set);
    additional->buildIndex(data_set2);
    std::cout << "built\n";
    a->addVectors(data_set);
    additional->addVectors(data_set2);
    std::cout << "added vectors\n";

    std::vector<float> pre_distance(nt * k);
    std::vector<int64_t> pre_ids(nt * k);

    std::vector<float> pre_distance2(nt * k);
    std::vector<int64_t> pre_ids2(nt * k);

    GeneralBitMapPtr bits = std::make_shared<GeneralBitMap>(nt);
    memset(bits->bitmap, 1, (nt / 8) + 1);

    a->search(data_set, k, pre_distance.data(), pre_ids.data(), bits);
    additional->search(data_set2, k, pre_distance2.data(), pre_ids2.data(), bits);

    for (int i = 0; i < k; i++)
    {
        std::cout << pre_ids[i] << ",";
        std::cout<<pre_distance[i]<<";";
    }
    std::cout<<"\n";
    for (int i = 0; i < k; i++)
    {
        std::cout << pre_ids2[i] << ",";
        std::cout<<pre_distance2[i]<<";";
    }
    std::cout<<"\n";

    a->cache();
    additional->cache();
    std::cout<<"cached\n";
    a->serialize();
    additional->serialize();
    std::cout << "serilized\n";
    VectorSegmentExecutorPtr b = std::make_shared<VectorSegmentExecutor>(dimension, IndexType(IndexType::IVFPQ), IndexMode(CPU), "test");
    VectorSegmentExecutorPtr bdditional = std::make_shared<VectorSegmentExecutor>(dimension, IndexType(IndexType::IVFPQ), IndexMode(CPU), "test2");

    std::cout << "b generated\n";
    b->load();
    bdditional->load();
    std::cout << "b loaded\n";

    std::vector<float> distance3(nt * k);
    std::vector<int64_t> ids3(nt * k);

    std::vector<float> distance4(nt * k);
    std::vector<int64_t> ids4(nt * k);


    GeneralBitMapPtr bit = std::make_shared<GeneralBitMap>(nt);
    memset(bit->bitmap, 1, (nt / 8) + 1);
    std::cout << "bitmap generated\n";
    b->search(data_set, k, distance3.data(), ids3.data(), bit);
    bdditional->search(data_set2, k, distance4.data(), ids4.data(), bit);
    std::cout << "searched\n";

    for (int i = 0; i < k; i++)
    {
        std::cout << ids3[i] << ",";
        std::cout<<distance3[i]<<";";
    }
    std::cout<<"\n";

    for (int i = 0; i < k; i++)
    {
        std::cout << ids4[i] << ",";
        std::cout<<distance4[i]<<";";
    }
    std::cout<<"\n";

    delete[] train;
    delete[] train2;
}

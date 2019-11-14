#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20

#define BUCKETS_PER_BLOCK 127

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {      \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

int hashFunctions(int indexDesc, int id) {

  int buckets;
  char *data;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  CALL_BF(BF_GetBlock(indexDesc, 0, mBlock));
  data = BF_Block_GetData(mBlock);
  buckets = *(int*)(data + 2 + sizeof(int));

  CALL_BF(BF_UnpinBlock(mBlock));
  BF_Block_Destroy(&mBlock);

  return id % buckets;
}

HT_ErrorCode HT_Init() {
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int buckets) {
  int indexDesc, block_num;;
  char *data, *tmpData;
  BF_Block *mBlock, *tmpBlock;
  BF_Block_Init(&mBlock);
  BF_Block_Init(&tmpBlock);

  CALL_BF(BF_CreateFile(filename));
  CALL_BF(BF_OpenFile(filename, &indexDesc));

  CALL_BF(BF_AllocateBlock(indexDesc, mBlock));             // 1st Block == General information Block
  data = BF_Block_GetData(mBlock);
  memset(data, 0, BF_BLOCK_SIZE); // optional
  memcpy(data, "HT", 2);
  memset(data + 2, 0, sizeof(int));                        // storing the number of records
  memcpy(data + 2 + sizeof(int), &buckets, sizeof(int));  // storing the number of buckets
  BF_Block_SetDirty(mBlock);
  CALL_BF(BF_UnpinBlock(mBlock));

  for(int i=0; i<(buckets/BUCKETS_PER_BLOCK); i++) {
    CALL_BF(BF_AllocateBlock(indexDesc, mBlock));
    data = BF_Block_GetData(mBlock);
    memset(data, 0, BF_BLOCK_SIZE);

    for(int j=0; j<BUCKETS_PER_BLOCK; j++) {
      CALL_BF(BF_AllocateBlock(indexDesc, tmpBlock));
      CALL_BF(BF_GetBlockCounter(indexDesc, &block_num));
      block_num--;
      memcpy(data + (j+1)*sizeof(int), &block_num, sizeof(int));
      CALL_BF(BF_UnpinBlock(tmpBlock));
    }

    if(i != (buckets/BUCKETS_PER_BLOCK - 1)) {
      CALL_BF(BF_GetBlockCounter(indexDesc, &block_num));
      memcpy(data, &block_num, sizeof(int));
    }
    else {
      if(buckets % BUCKETS_PER_BLOCK != 0) {
        CALL_BF(BF_GetBlockCounter(indexDesc, &block_num));
        memcpy(data, &block_num, sizeof(int));
      }
    }

    BF_Block_SetDirty(mBlock);
    CALL_BF(BF_UnpinBlock(mBlock));
  }

  if(buckets % BUCKETS_PER_BLOCK != 0) {
    CALL_BF(BF_AllocateBlock(indexDesc, mBlock));
    data = BF_Block_GetData(mBlock);
    memset(data, 0, BF_BLOCK_SIZE);

    for(int i=0; i<buckets%BUCKETS_PER_BLOCK; i++) {
      CALL_BF(BF_AllocateBlock(indexDesc, tmpBlock));
      CALL_BF(BF_GetBlockCounter(indexDesc, &block_num));
      block_num--;
      memcpy(data + (i+1)*sizeof(int), &block_num, sizeof(int));
      CALL_BF(BF_UnpinBlock(tmpBlock));
    }

    BF_Block_SetDirty(mBlock);
    CALL_BF(BF_UnpinBlock(mBlock));
  }

  // PRINTT ALL BLOCKS
  // CALL_BF(BF_GetBlock(indexDesc, 1, mBlock));
  // data = BF_Block_GetData(mBlock);

  // for(int i=0; i<(buckets/BUCKETS_PER_BLOCK); i++) {
  //   printf("\n--------------------\n");
  //   printf("Next Block: %d\n", *(int*)data);
  //   for(int j=0; j<BUCKETS_PER_BLOCK; j++) {
  //     printf("Block Number = %d\n", *(int*)(data + (j+1)*sizeof(int)));
  //   }
  //   printf("--------------------\n");

  //   CALL_BF(BF_UnpinBlock(mBlock));
  //   if(*(int*)data != 0) {
  //     CALL_BF(BF_GetBlock(indexDesc, *(int*)data, mBlock));
  //     data = BF_Block_GetData(mBlock);
  //   }
  // }

  // printf("\n--------------------\n");
  // for(int i=0; i<buckets%BUCKETS_PER_BLOCK; i++) {
  //     printf("Block Number = %d\n", *(int*)(data + (i+1)*sizeof(int)));
  // }
  // printf("--------------------\n");
  // CALL_BF(BF_UnpinBlock(mBlock));

  BF_Block_Destroy(&mBlock);
  BF_Block_Destroy(&tmpBlock);
  CALL_BF(BF_CloseFile(indexDesc));

  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){

  char *data, code[2];
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  CALL_BF(BF_OpenFile(fileName, indexDesc))
  CALL_BF(BF_GetBlock(*indexDesc, 0, mBlock));
  data = BF_Block_GetData(mBlock);
  memcpy(code, data, 2);
  if(strcmp(code, "HT")) {
    return HT_ERROR;
  }

  CALL_BF(BF_UnpinBlock(mBlock));
  BF_Block_Destroy(&mBlock);
  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
  CALL_BF(BF_CloseFile(indexDesc));
  return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {

  int block_num;
  char *data;
  BF_Block *mBlock, *tmpBlock;
  BF_Block_Init(&mBlock);
  BF_Block_Init(&tmpBlock);

  CALL_BF(BF_GetBlock(indexDesc, 0, mBlock));              // Checking if a reHash is needed
  data = BF_Block_GetData(mBlock);
  // if(*(int*)(data + 2) / *(int*)(data + 2 + sizeof(int))) {
    //REHASH()
  // }
  // else {
    int records_num = *(int*)(data + 2) + 1;
    memcpy(data + 2, &records_num, sizeof(int));           // records counter++
    BF_Block_SetDirty(mBlock);
    CALL_BF(BF_UnpinBlock(mBlock));
  // }
  //Getting the currect Block for the records after hashing the id
  CALL_BF(BF_GetBlock(indexDesc, 1, mBlock));
  data = BF_Block_GetData(mBlock);                                                   //|-> Needs to be changed after
  block_num = *(int*)(data + (1 + hashFunctions(indexDesc, record.id))*sizeof(int)); //|  -> implementing reHash()
  CALL_BF(BF_UnpinBlock(mBlock));

  CALL_BF(BF_GetBlock(indexDesc, block_num, mBlock));      // Getting the last Block in the BlockChain
  data = BF_Block_GetData(mBlock);
  block_num = *(int*)(data + 1);
  while(block_num != 0) {
    CALL_BF(BF_UnpinBlock(mBlock));
    CALL_BF(BF_GetBlock(indexDesc, block_num, mBlock));
    data = BF_Block_GetData(mBlock);
    block_num = *(int*)(data + 1);
  }
  int free_space = BF_BLOCK_SIZE - 1 - sizeof(int) - data[0]*sizeof(Record);
  if(free_space>=sizeof(Record)){
  // if(data[0] != 8) {
    memcpy(data + 1 + sizeof(int) + data[0]*sizeof(Record), &record, sizeof(Record));
    memset(data, data[0] + 1, 1);                   // records_counter++
  }
  else {                                            // Block is full, adding a new Block in the BlockChain
    CALL_BF(BF_AllocateBlock(indexDesc, tmpBlock));
    CALL_BF(BF_GetBlockCounter(indexDesc, &block_num));
    block_num--;  //because of index block
    memcpy(data + 1, &block_num, sizeof(int));
    data = BF_Block_GetData(tmpBlock);
    memset(data, 0, BF_BLOCK_SIZE);
    memset(data, 1, 1);
    memcpy(data + 1 + sizeof(int), &record, sizeof(Record));
    BF_Block_SetDirty(tmpBlock);
    CALL_BF(BF_UnpinBlock(tmpBlock));
  }

  BF_Block_SetDirty(mBlock);
  CALL_BF(BF_UnpinBlock(mBlock));
  BF_Block_Destroy(&mBlock);
  BF_Block_Destroy(&tmpBlock);
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {

  char *data;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);
  
  CALL_BF(BF_GetBlock(indexDesc, 0, mBlock));
  data = BF_Block_GetData(mBlock);
  // for(int i=2; i<128; i++) {   //Works only before the implementation of reHash
  for(int i=2; i<(*(int*)(data+2+sizeof(int))+1); i++) {  //for every bucket
    HT_PrintBlockChain(indexDesc, i, id);
  }

  CALL_BF(BF_UnpinBlock(mBlock));
  BF_Block_Destroy(&mBlock);
  return HT_OK;
}

void HT_PrintRecord(char *data, int i, int* id) {

  Record record;

  memcpy(&record, data + 1 + sizeof(int) + i*sizeof(Record), sizeof(Record));
  if(id == NULL || record.id == *id) {
    printf("%d, \"%s\", \"%s\", \"%s\"\n",record.id, record.name, record.surname, record.city);
  }
}

HT_ErrorCode HT_PrintBlockChain(int indexDesc, int block_num, int* id) {

  char *data;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  CALL_BF(BF_GetBlock(indexDesc, block_num, mBlock))
  data = BF_Block_GetData(mBlock);

  if(*(int*)(data + 1) != 0) {
    HT_PrintBlockChain(indexDesc, *(int*)(data + 1), id);
  }

  for(int i=0; i<data[0]; i++) {
    HT_PrintRecord(data, i, id);
  }

  CALL_BF(BF_UnpinBlock(mBlock));
  BF_Block_Destroy(&mBlock);
}

HT_ErrorCode HT_DeleteEntry(int indexDesc, int id) {

  char* data;
  BF_Block* mBlock;
  Record record, lastRecord;
  BF_Block_Init(&mBlock);

  CALL_BF(BF_GetBlock(indexDesc, hashFunctions(indexDesc, id) + 2, mBlock));
  data = BF_Block_GetData(mBlock);
  while(*(int*)(data + 1) != 0) {             //finds the last block of the blockchain
    CALL_BF(BF_UnpinBlock(mBlock));
    CALL_BF(BF_GetBlock(indexDesc, *(int*)(data + 1), mBlock))
    data = BF_Block_GetData(mBlock);
  }

  memcpy(&lastRecord, data + 1 + sizeof(int) + data[0]*sizeof(Record), sizeof(Record)); // saves last record
  memset(data, data[0] - 1, 1);     // records_countert--
  memset(data + 1 + sizeof(int) + data[0]*sizeof(Record), 0, sizeof(Record)); // "deleting" lastRecord by putting 0 everywhere

  if(lastRecord.id != id) {
    CALL_BF(BF_UnpinBlock(mBlock));
    CALL_BF(BF_GetBlock(indexDesc, hashFunctions(indexDesc, id) + 2, mBlock));
    data = BF_Block_GetData(mBlock);

    while(*(int*)(data + 1) != 0) {           // iterating the blockChain to find the record tha needs to be deleted
      for(int i=0; i<data[0]; i++) {
        memcpy(&record, (data + 1 + sizeof(int) + i*sizeof(Record)), sizeof(Record));
        if(record.id == id) {
          memcpy(data + 1 + sizeof(int) + i*sizeof(Record), &lastRecord, sizeof(Record));
        }
      }
      CALL_BF(BF_UnpinBlock(mBlock));
      CALL_BF(BF_GetBlock(indexDesc, *(int*)(data + 1), mBlock))
      data = BF_Block_GetData(mBlock);
    }
    for(int i=0; i<data[0]; i++) {            // iterating the last Records in the BlockChain
        memcpy(&record, (data + 1 + sizeof(int) + i*sizeof(Record)), sizeof(Record));
        if(record.id == id) {
          memcpy(data + 1 + sizeof(int) + i*sizeof(Record), &lastRecord, sizeof(Record));
        }
      }
  }

  CALL_BF(BF_UnpinBlock(mBlock));
  BF_Block_Destroy(&mBlock);
  return HT_OK;
}

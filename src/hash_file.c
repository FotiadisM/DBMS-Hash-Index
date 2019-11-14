#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20

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
  int indexDesc;
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
  // // // // // // // // // // // // // // // // // // // //
  // // // // // // // // // // // // // // // // // // // // 
  // in case buckets need more than one block to store
  // each bucket needs sizeof(int) space, find out how many blocks are needed
  // NT is the int at the beginning of index block that shows to the next block
  int num_blocks, remain, bytes_withoutNT;
  // int temp=0;
  // for(buckets=1;buckets<1600;buckets++){
    if( sizeof(int)*buckets <= BF_BLOCK_SIZE-sizeof(int) ){// max 127
      num_blocks=1;
    }
    else{// need more than one index blocks
      // how many blocks i need without the NT
      bytes_withoutNT=sizeof(int)*(buckets);
      num_blocks=bytes_withoutNT/(BF_BLOCK_SIZE-sizeof(int));
      remain=bytes_withoutNT%(BF_BLOCK_SIZE-sizeof(int));
      if(remain!=0){num_blocks++;}
    }
  //   ++temp;
  //   if(temp==127 || temp==1){printf("%d For %d buckets need %d blocks.\n",temp,buckets,num_blocks);}
  //   if(temp==127){printf("\n\n");}
  //   if(temp==127){temp=0;}
  // }
  printf("For %d buckets i need %d blocks.\n", buckets, num_blocks);
  // // // // // // // // // // // // // // // // // // // //
  // // // // // // // // // // // // // // // // // // // //
  int temp_buckets=buckets;   //used to see how many buckets have stayed
  int k=1;
  int up_bound;

  for(int i=0; i<num_blocks; i++){
    CALL_BF(BF_AllocateBlock(indexDesc, mBlock));                 // 2nd Block == 1st Index Block
    data = BF_Block_GetData(mBlock);
    memset(data, 0, BF_BLOCK_SIZE);

    //check if the remaining buckets need a whole block
    if( sizeof(int)*temp_buckets>=BF_BLOCK_SIZE-sizeof(int) ){
      up_bound=BF_BLOCK_SIZE/sizeof(int)-1;//127
    }
    else{
      up_bound=temp_buckets;
    }
    printf("for block %d the upper bound is %d\n",i+1, up_bound);

    // for(int i=0; i<buckets; i++) {
    // for every bucket of this block
    for(int j=0; j<up_bound; j++) {
  //   int k = i+2;
      k++;

  //   memcpy(data + sizeof(int) + i*sizeof(int), &k, sizeof(int));// storing the block_num of the record-Block
      memcpy(data + sizeof(int) + j*sizeof(int), &k, sizeof(int));
      CALL_BF(BF_AllocateBlock(indexDesc, tmpBlock));             // |
      tmpData = BF_Block_GetData(tmpBlock);                       // |->creating the record-Blocks for the 1st Index Block
      memset(tmpData, 0, BF_BLOCK_SIZE);                          // |
      BF_Block_SetDirty(tmpBlock);
      CALL_BF(BF_UnpinBlock(tmpBlock));
    }
    temp_buckets-=up_bound;
    if(temp_buckets>0){ //an exoun meinei buckets na mpoun se neo block 
      k++;
      memcpy(data, &k, sizeof(int));
    }

    BF_Block_SetDirty(mBlock);
    CALL_BF(BF_UnpinBlock(mBlock));
    // printf("to k vgike %d\n",k);
  }
  // // // // // // // // // // // // // // // // // // // //
  // // // // // // // // // // // // // // // // // // // //
  // print index blocks
  int records_num;// = *(int*)(data + 2) + 1;
  int block_to_get=1;
  for(int i=0; i<num_blocks; i++){
    CALL_BF(BF_GetBlock(indexDesc, block_to_get, mBlock));
    block_to_get+=128;
    data = BF_Block_GetData(mBlock);

    printf("For index block %d\n",i+1);
    for(int j=0; j<BF_BLOCK_SIZE/sizeof(int); j++){
      records_num=*(int*)(data+j*sizeof(int));
      printf("\t %d\n", records_num);
    }

    CALL_BF(BF_UnpinBlock(mBlock));
  }
  // // // // // // // // // // // // // // // // // // // //
  // // // // // // // // // // // // // // // // // // // //

  BF_Block_Destroy(&mBlock);
  BF_Block_Destroy(&tmpBlock);
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"

#define RECORDS_NUM 200 // you can change it if you want
#define BUCKETS_NUM 200 // you can change it if you want
#define MAX_OPEN_FILES 20

const char* names[] = {
  "Yannis",
  "Christofos",
  "Sofia",
  "Marianna",
  "Vagelis",
  "Maria",
  "Iosif",
  "Dionisis",
  "Konstantina",
  "Theofilos",
  "Giorgos",
  "Dimitris"
};

const char* surnames[] = {
  "Ioannidis",
  "Svingos",
  "Karvounari",
  "Rezkalla",
  "Nikolopoulos",
  "Berreta",
  "Koronis",
  "Gaitanis",
  "Oikonomou",
  "Mailis",
  "Michas",
  "Halatsis"
};

const char* cities[] = {
  "Athens",
  "San Francisco",
  "Los Angeles",
  "Amsterdam",
  "London",
  "New York",
  "Tokyo",
  "Hong Kong",
  "Munich",
  "Miami"
};


typedef struct file
{
  int indexDesc;
  char name[9];
  BF_Block* fBlock;
} fileInfo;

typedef fileInfo* fileInfoPtr;

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {      \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

#define CALL_OR_DIE(call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

int main(int argc, char *argv[]) {

  int indexDesc, r;
  char fileName[] = "data#.db";
  fileInfoPtr* array;

  if(argc != 2) {
    printf("Please enter the number of files to open in the command line\n");
    return 0;
  }
  if(atoi(argv[1]) > MAX_OPEN_FILES){
    printf("Number of files should be at most %d.\n",MAX_OPEN_FILES);
    return 0;
  }
  array = malloc(atoi(argv[1])*sizeof(fileInfo));

  BF_Init(LRU);
  
  CALL_OR_DIE(HT_Init());

  for(int i=0; i < atoi(argv[1]); i++) {
    array[i] = malloc(sizeof(fileInfo));
  
    fileName[4] = i + '0';
    CALL_OR_DIE(HT_CreateIndex(fileName, BUCKETS_NUM));
    CALL_OR_DIE(HT_OpenIndex(fileName, &indexDesc));

    array[i]->indexDesc = indexDesc;
    strcpy(array[i]->name, fileName);
  }

  Record record;
  srand(12569874);

  printf("\nInsert Entries\n");
  for (int id = 0; id < RECORDS_NUM; ++id) {
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

    r = rand() % atoi(argv[1]);
    CALL_OR_DIE(HT_InsertEntry(array[r]->indexDesc, record));
  }

  printf("RUN PrintAllEntries\n");
  int id = rand() % RECORDS_NUM;
  for(int i=0; i < atoi(argv[1]); i++) {
    printf("IN FILE NUMBER : %d\n", i);
    CALL_OR_DIE(HT_PrintAllEntries(array[i]->indexDesc, NULL));
    CALL_OR_DIE(HT_PrintAllEntries(array[i]->indexDesc, &id));
  }

  printf("\nDelete Entry with id = %d\n" ,id);
  for(int i=0; i < atoi(argv[1]); i++) {
    CALL_OR_DIE(HT_DeleteEntry(array[i]->indexDesc, id));
  }

  printf("\nPrint Entry with id = %d\n", id); 
  for(int i=0; i < atoi(argv[1]); i++) {
    printf("IN FILE NUMBER : %d\n", i);
    CALL_OR_DIE(HT_PrintAllEntries(array[i]->indexDesc, &id));
  }

  for(int i=0; i < atoi(argv[1]); i++) {
    CALL_OR_DIE(HT_CloseFile(array[i]->indexDesc));

    free(array[i]);
  }
  free(array);

  BF_Close();

  return 0;
}
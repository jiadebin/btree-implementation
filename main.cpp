/**
 * main.cpp
 * Contains tests for our BTreeIndex implementation.
 *
 * @author Aly Valliani
 * @author Oscar Chen
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <vector>
#include <time.h>
#include <stdlib.h>
#include "btree.h"
#include "include/page.h"
#include "include/fileScanner.h"
#include "include/page_iterator.h"
#include "include/file_iterator.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/insufficient_space_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/end_of_file_exception.h"

#include "randRels.cpp"

#define checkPassFail(a, b)                                         \
{                                                                   \
  if(a == b)                                                        \
    std::cout << "\nTest passed at line no:" << __LINE__ << "\n";   \
  else                                                              \
  {                                                                 \
    std::cout << "\nTest FAILS at line no:" << __LINE__;            \
    std::cout << "\nExpected no of records:" << b << std::endl;                  \
    std::cout << "\nActual no of records found:" << a << std::endl;              \
    std::cout << std::endl;                                         \
    exit(1);                                                        \
  }                                                                 \
}

#define PRINT_ERROR(str) \
{ \
  std::cerr << "On Line No:" << __LINE__ << "\n"; \
  std::cerr << str << "\n"; \
  exit(1); \
}

using namespace wiscdb;

// -----------------------------------------------------------------------------
// Globals
// ----------------------------------------------------------------------------

const std::string relationName = "relA";

//If you change the relation size, note that you may want to 
//comment out some of the chechPassFail calls below since they will fail
//on higher values
const int  relationSize = 5000;

std::string indexName;

// This is the structure for tuples in the base relation
typedef struct tuple {
  int i;
  double d;
  char s[64];
} RECORD;

PageFile* file1;
RecordId rid;
RECORD record1;
std::string dbRecord1;

BufferManager * bufMgr = new BufferManager(5000);

// -----------------------------------------------------------------------------
// Forward declarations
// -----------------------------------------------------------------------------

void createRelationForward();
void createRelationBackward();
void createRelationRandom();
void deleteRelation();

void testForward();
void testBackward();
void testRandom();

void indexTests();

void testFileload();
void showInitInsert();
void showInsertLeafBrim();
void showInsertForward();
void showInsertBackward();

void stringTests();
int stringScan(BTreeIndex *index, int lowVal, Operator lowOp, int highVal, Operator highOp);
void scanExceptionTests();

int main(int argc, char **argv)
{
  std::cout << "leaf size:" << LEAF_NUM_KEYS 
            << " non-leaf size:" << NON_LEAF_NUM_KEYS << std::endl;

  // Clean up from any previous runs that crashed.
  try {
    File::remove(relationName);
  } catch(FileNotFoundException) {}

  {
    // Create a new database file.
    PageFile new_file = PageFile::create(relationName);
    // Allocate some pages and put data on them.
    for (int i = 0; i < 20; ++i) {
      PageId new_page_number;
      Page new_page = new_file.allocatePage(new_page_number);

      sprintf(record1.s, "%05d string record", i);
      record1.i = i;
      record1.d = (double)i;
      std::string new_data((char*)(&record1), sizeof(record1));

      new_page.insertRecord(new_data);
      new_file.writePage(new_page_number, new_page);
    }
  }
  // new_file goes out of scope here, so file is automatically closed.

  {
    FileScanner fscan(relationName, bufMgr);
    try {
      RecordId scanRid;
      while(1) {
        fscan.scanNext(scanRid);
        //Assuming RECORD.i is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record. 
        std::string recordStr = fscan.getRecord();
        const char *record = recordStr.c_str();
        int key = *((int *)(record + offsetof (RECORD, i)));
        std::cout << "Extracted : " << key << std::endl;
      }
    } catch(EndOfFileException e) {
      std::cout << "Read all records" << std::endl;
}
  }
  // filescan goes out of scope here, so relation file gets closed.

  File::remove(relationName);

  // Tests a forward relation (deterministic, 1x) , then a backwards relation (deterministic, 1x) , then a random relation (stochastic, 20x)
  testForward();
  delete bufMgr; // something with internal code of bufMgr was causing errors before (flush file not working properly), so now deleting bufMgr each time
  bufMgr = new BufferManager(5000);
  testBackward();
  delete bufMgr;
  bufMgr = new BufferManager(5000);
  //testRandom(); //for single iteration testing
  for (int i = 0; i < 20; i++) {
    printf("---[Iteration %d]---\n", i);
    testRandom();
    delete bufMgr; 
    bufMgr = new BufferManager(5000);
  }

  delete bufMgr;
  return 1;
}

/* 
 * testForward - creates a relation with tuples valued 0 to relationSize in 
 * forward order and perform index tests
 */
void testForward() {
  printf("---------------------\n");
  printf("TEST 1: Forward\n");
  printf("---------------------\n");
  createRelationForward();
  indexTests();
  deleteRelation();
}

/** 
 * testBackward - creates a relation with tuples valued 0 to relationSize in 
 * backward order and perform index tests
 */
void testBackward() {
  printf("---------------------\n");
  printf("TEST 2: Backward\n");
  printf("---------------------\n");
  createRelationBackward();
  indexTests();
  deleteRelation();
}

/** 
 * testRandom - creates a relation with tuples valued 0 to relationSize in 
 * random order and perform index tests
 * @param i - 
 */
void testRandom() {
  printf("---------------------\n");
  printf("TEST 3: Random\n");
  printf("---------------------\n");
  createRelationRandom();
  indexTests();
  deleteRelation();
}

// -----------------------------------------------------------------------------

/**
 * createRelationForward - creates a bunch of relations where values
 * are generated in a loop
 */
void createRelationForward() {
  // destroy any old copies of relation file
  try {
    File::remove(relationName);
  } catch(FileNotFoundException e) {}

  file1 = new PageFile(relationName, true);
  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
  PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // Insert a bunch of tuples into the relation.
  for(int i = 0; i < relationSize; i++ ) {
    sprintf(record1.s, "%05d string record", i);
    record1.i = i;
    record1.d = (double)i;
    std::string new_data((char*)(&record1), sizeof(record1));
    while(1) {
      try {
        new_page.insertRecord(new_data);
        break;
      } catch(InsufficientSpaceException e) {
        file1->writePage(new_page_number, new_page);
        new_page = file1->allocatePage(new_page_number);
      }
    }
  }
  file1->writePage(new_page_number, new_page);
}

/**
 * createRelationBackward - creates a bunch of relations where values
 * are generated in a loop in backwards order
 */
void createRelationBackward() {
  // destroy any old copies of relation file
  try {
    File::remove(relationName);
  } catch(FileNotFoundException e) {}
  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
  PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // Insert a bunch of tuples into the relation.
  for(int i = relationSize-1; i >= 0; i--) {
    sprintf(record1.s, "%05d string record", i);
    record1.i = i;
    record1.d = (double)i;
    std::string new_data((char*)(&record1), sizeof(record1));
    while(1) {
      try {
        new_page.insertRecord(new_data);
        break;
      } catch(InsufficientSpaceException e) {
        file1->writePage(new_page_number, new_page);
        new_page = file1->allocatePage(new_page_number);
      }
    }
  }
  file1->writePage(new_page_number, new_page);
}

/**
 * createRelationRandom - creates a bunch of relations where values
 * are generated in a loop, then shuffled randomly
 */
void createRelationRandom() {
  // destroy any old copies of relation file, create new file
  try {
    File::remove(relationName);
  } catch(FileNotFoundException e) {}
  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
  PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);
  int arr[relationSize];

  //Populate the array
  for(int i = 0; i < relationSize; i++) { arr[i] = i; }
  //Random shuffle the array
  srand(time(NULL));
  int j, temp;
  for(int i = 0; i < relationSize; i++) { //shuffles items within the array
    j = i+rand() / (RAND_MAX / (relationSize-i)+1);
    temp = arr[j];
    arr[j] = arr[i];
    arr[i] = temp;
  }

  // Insert a bunch of tuples into the relation.
  for(int i = 0; i < relationSize; i++ ) {
    sprintf(record1.s, "%05d string record", arr[i]);
    // These were tests that failed previously and now work when we change the relation size
    //sprintf(record1.s, "%s", relA_test1[i]); // 20
    //sprintf(record1.s, "%s", relA_test2[i]); // 20
    //sprintf(record1.s, "%s", relA_test3[i]); // 100
    //sprintf(record1.s, "%s", relA_test4[i]); // 100
    //sprintf(record1.s, "%s", relA_test5[i]); // 200
    //sprintf(record1.s, "%s", relA_test6[i]); // 200
    //sprintf(record1.s, "%s", relA_test7[i]); // 20
    //sprintf(record1.s, "%s", relA_test8[i]); // 1000
    //sprintf(record1.s, "%s", relA_HashAlreadyPresent[i]); // 100
    record1.i = i;
    record1.d = (double)i;
    std::string new_data((char*)(&record1), sizeof(record1));
    while(1) {
      try {
        new_page.insertRecord(new_data);
        break;
      } catch(InsufficientSpaceException e) {
        file1->writePage(new_page_number, new_page);
        new_page = file1->allocatePage(new_page_number);
      }
    }
  }
  file1->writePage(new_page_number, new_page);
}

/**
 * deleteRelation - deletes the relation file currently being used for tests
 */
void deleteRelation()
{
  if(file1) {
    bufMgr->flushFile(file1);
    delete file1;
    file1 = NULL;
  }
  try { File::remove(relationName); }
  catch(FileNotFoundException e) {}
  return;
}

// -----------------------------------------------------------------------------
// indexTests - use to call multiple tests on the same relation
// -----------------------------------------------------------------------------

void indexTests() {
  try{ File::remove(indexName); }
  catch(FileNotFoundException e){}
  testFileload();
  /**Visual Diagnostics**/
  //showInitInsert();
  //showInsertLeafBrim();
  showInsertForward();
  showInsertBackward();
  stringTests();
  scanExceptionTests();
  try{
    File::remove(indexName);
  }
  catch(FileNotFoundException e){}
}

// -----------------------------------------------------------------------------
//  Tests
// -----------------------------------------------------------------------------

/**
 * stringTests - Given the generated index, runs a few scans and checks the
 * number of returned items is correct
 */
void stringTests() {
  std::cout << "Create a B+ Tree index on the string field" << std::endl;
  BTreeIndex index(relationName, indexName, bufMgr, offsetof(tuple,s));
  checkPassFail(stringScan(&index,5,GT,15,LT),9); 
  checkPassFail(stringScan(&index, 8, GTE, 16, LT), 8);
  checkPassFail(stringScan(&index,25,GT,40,LT), 14);
  checkPassFail(stringScan(&index,20,GTE,35,LTE), 16);
  checkPassFail(stringScan(&index,-3,GT,3,LT), 3);
  checkPassFail(stringScan(&index,996,GT,1001,LT), 4);
  checkPassFail(stringScan(&index,0,GT,1,LT), 0); // no such key, 0 keys found
  checkPassFail(stringScan(&index,100,GT,150,LT), 49);
  checkPassFail(stringScan(&index,300,GT,400,LT), 99);
  checkPassFail(stringScan(&index,3000,GTE,4000,LT), 1000);
  checkPassFail(stringScan(&index,10,GTE,10,LTE), 1); // added equality check
  checkPassFail(stringScan(&index,0,GTE,relationSize,LT), relationSize); // added full scan check 
  printf("===Passed stringTests===\n");
}

/**
 * stringScan - Runs a full index scan for a given range of integers
 * @param index - pointer to BTreeIndex to run scan on
 * @param lowVal - Low value of range, integer
 * @param lowOp - Low operator (GT/GTE)
 * @param highVal - high value of range, integer
 * @param highOp - high operator (LT/LTE)
 * @return returns number of matching keys (results) found
 */
int stringScan(BTreeIndex * index, int lowVal, Operator lowOp, int highVal, Operator highOp) {
  char lowValStr[100];
  sprintf(lowValStr,"%05d string record",lowVal);
  char highValStr[100];
  sprintf(highValStr,"%05d string record",highVal);
  int numResults = 0;
  Page* curPage;
  std::cout << "Scan for ";
  if( lowOp == GT ) { std::cout << "("; } else { std::cout << "["; }
  std::cout << lowVal << "," << highVal;
  if( highOp == LT ) { std::cout << ")"; } else { std::cout << "]"; }
  std::cout << std::endl;

  try {
    index->startScan(lowValStr, lowOp, highValStr, highOp);
  } catch(NoSuchKeyFoundException e) { // if no matches found, return 0
    std::cout << "No Key Found satisfying the scan criteria." << std::endl;
    return 0;
  }
  try {
    while(true) {
      RecordId rid;
      //Get Record from index and then retrieve from heapfile
      index->scanNext(rid);
      bufMgr->readPage(file1, rid.page_number, curPage);

      //read record into struct for easy handling below
      RECORD myRec = *(RECORD*)(curPage->getRecord(rid).c_str());
      bufMgr->unPinPage(file1, rid.page_number, false);
      
      //Only print out first few values
      if(numResults < 5){
        std::cout << "rid:" << rid.page_number << "," << rid.slot_number;
        std::cout << " -->:" << myRec.i << ":" << myRec.d << ":" << myRec.s << ":" << std::endl;
      }
      numResults++;
    }
  } catch(IndexScanCompletedException e){}
  return numResults;
}

/**
 * scanExceptionTests - Tests for a range of scan exceptions, including
 *      ScanNotInitializedException, BadScanrangeException, and BadOpcodesException
 */
void scanExceptionTests() {
  printf("relationName: %s, indexName: %s, offsetof(tuple, s): %d\n", relationName.c_str(), indexName.c_str(), offsetof(tuple,s));
  BTreeIndex index(relationName, indexName, bufMgr, offsetof(tuple,s));
  try { //end uninitialized scan
    index.endScan();
    PRINT_ERROR("end scan on uninitialized scan didn't call ScanNotInitializedException");
  } catch (ScanNotInitializedException e) {}
  try { //scan next with uninitialized scan
    RecordId dummyRid;
    index.scanNext(dummyRid);
    PRINT_ERROR("scan next on uninitialized scan didn't call ScanNotInitializedException");
  } catch (ScanNotInitializedException e) {}
  // Test bad scan exceptions
  try { stringScan(&index,10,GT,5,LT); PRINT_ERROR("end scan on uninitialized scan didn't call ScanNotInitializedException"); } 
    catch (BadScanrangeException e) {}
  try { stringScan(&index,11,GT,10,LT); PRINT_ERROR("end scan on uninitialized scan didn't call ScanNotInitializedException"); } 
    catch (BadScanrangeException e) {}
  // Test bad op code exceptions
  try { stringScan(&index,5,LT,15,LT); PRINT_ERROR("scan on (5,LT,15,LT) didn't call BadScanRangeException"); } 
    catch (BadOpcodesException e) {}
  try { stringScan(&index,5,LTE,15,LT); PRINT_ERROR("scan on (5,LTE,15,LT) didn't call BadScanRangeException"); } 
    catch (BadOpcodesException e) {}
  try { stringScan(&index,5,GT,15,GT); PRINT_ERROR("scan on (5,GT,15,GT) didn't call BadScanRangeException"); } 
    catch (BadOpcodesException e) {}
  try { stringScan(&index,5,GT,15,GTE); PRINT_ERROR("scan on (5,GT,15,GTE) didn't call BadScanRangeException"); } 
    catch (BadOpcodesException e) {}

  printf("===Scan exceptions tests passed===\n");
  return;
}

/**
 * testFileload - Tests if index can be deleted and re-opened correctly 
 * (including all header metadata), then tests BadIndexException cases
 */
void testFileload(){
    BTreeIndex* index = new BTreeIndex(relationName, indexName, bufMgr, offsetof(tuple,s));
    std::cout << "Successfully created the initial index\n";
    checkPassFail(stringScan(index, 0, GTE, relationSize, LT), relationSize);
    std::cout << "Successfully conducted an initial scan\n";
    delete index;
    index = new BTreeIndex(relationName, indexName, bufMgr, offsetof(tuple,s));
    std::cout << "Successfully reloaded index\n";
    checkPassFail(stringScan(index, 0, GTE, relationSize, LT), relationSize);
    std::cout << "Successfully conducted scan on reloaded index\n";
    delete index;
    File::remove(indexName);
}

/*
 * showInitInsert - Prints tree after initial construction of relationName's 
 * index for visual inspection
 */
void showInitInsert() {
  BTreeIndex* index = new BTreeIndex(relationName, indexName, bufMgr, offsetof(tuple,s));
  FileScanner* fscan = new FileScanner(relationName, bufMgr);
  RecordId scanRid;
  fscan->scanNext(scanRid);
  std::string recordStr = fscan->getRecord();
  const char* record = recordStr.c_str();
  const char* key = record + offsetof(tuple,s);
  index->insertEntry(key, scanRid);
  index->printTree();
  delete fscan;
  delete index;
}

/*
 * showInsertLeafBrim - Prints tree after initial construction and inserting
 * until a specific leaf is completely filled up (to only be used in debug)
 */
void showInsertLeafBrim() {
  BTreeIndex* index = new BTreeIndex(relationName, indexName, bufMgr, offsetof(tuple,s));
  FileScanner* fscan = new FileScanner(relationName, bufMgr);
  RecordId scanRid;
  for (int i = 0; i < 8; i++) {
    fscan->scanNext(scanRid);
    std::string recordStr = fscan->getRecord();
    const char* record = recordStr.c_str();
    const char* key = record + offsetof(tuple,s);
    index->insertEntry(key, scanRid);
  }
  index->printTree();
  delete fscan;
  delete index;
  return;
}

/*
 * showInsertForward - Prints tree after initial construction and inserting
 * 197 items in forward order from 0 to 196
 */
void showInsertForward() {
  BTreeIndex* index = new BTreeIndex(relationName, indexName, bufMgr, offsetof(tuple,s));
  FileScanner* fscan = new FileScanner(relationName, bufMgr);
  RecordId scanRid;
  for (int i = 0; i < 197; i++) {
    printf("<%d>\n", i);
    fscan->scanNext(scanRid);
    std::string recordStr = fscan->getRecord();
    const char* record = recordStr.c_str();
    const char* key = record + offsetof(tuple,s);
    index->insertEntry(key, scanRid);
  }
  index->printTree();
  delete fscan;
  delete index;
  return;
}

/*
 * showInsertBackward - Prints tree after initial construction and inserting
 * 198 items in backwards order from 197 to 0
 */
void showInsertBackward() {
  BTreeIndex* index = new BTreeIndex(relationName, indexName, bufMgr, offsetof(tuple,s));
  FileScanner* fscan = new FileScanner(relationName, bufMgr);
  RecordId scanRid;
  for (int i = 0; i < 198; i++) {
    printf("<%d>\n", i);
    fscan->scanNext(scanRid);
    std::string recordStr = fscan->getRecord();
    const char* record = recordStr.c_str();
    const char* key = record + offsetof(tuple,s);
    index->insertEntry(key, scanRid);
  }
  index->printTree();
  delete fscan;
  delete index;
  return;
}
